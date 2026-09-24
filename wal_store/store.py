"""Write-ahead log key value store.

Every mutation is first appended to a framed log. A commit marker is the
only thing that advances the durable sequence number. When a store is opened,
any tail that was never committed (including a record torn by a hard kill) is
discarded, so recovery always yields exactly the committed state.

Only the very last log record may be incomplete: a process killed while
appending leaves at most one torn record, which is dropped without being
applied. Any other framing failure -- a truncated record in the middle, a
length field out of bounds, unparseable content, or a duplicate/regressing
commit sequence -- is corruption, and recovery raises ``CorruptLogError``
without touching the store.

Recovery itself may be killed at any point, any number of times. Two small
sidecar files in the store directory, both maintained below the write path,
make the convergence protocol kill-safe and its report stable:

``wal.ckp``
    A byte-for-byte copy of the log prefix ending at the most recent commit
    marker, replaced atomically (temp file, fsync, rename, directory fsync)
    only after that commit frame is durable. A log torn at *any* offset --
    even offset 0, or inside the committed prefix -- is rebuilt from it, so
    the durable sequence number never regresses. It is also the published
    snapshot read-only processes pin (see below).

``wal.rec``
    A recovery marker written only after a log needing repair has passed
    full validation and only *before* the atomic rebuild starts. It records
    the clean prefix length and whether a torn record was discarded. Its
    presence makes a rebuild interrupted mid-copy converge on reopen (the
    rebuild is simply repeated) and keeps the reported ``discarded`` count
    stable across reopens and repeated recoveries. It is removed by the next
    successful commit.

Compaction (``Store.compact()``) rewrites the committed history into one
compact log holding exactly the live key/value records plus a single commit
marker carrying the *same* durable sequence number, and releases the old
log's space. It uses the same atomic-publish machinery and two more
sidecars, both kept out of the reader path:

``wal.cmp``
    The fully written, fsynced compacted image: one put frame per live key
    in first-write order followed by one *base* commit marker (``"b":1``)
    carrying the preserved sequence. It is staged as a temp file and
    atomically renamed, so it is either absent or a complete compacted
    snapshot -- never a half image.

``wal.cpr``
    The compaction plan marker, written only after ``wal.cmp`` is durable
    and only *before* the publish. It records the compacted image length,
    its sequence and the old log/sidecar byte lengths the plan was computed
    from. Its presence makes a compaction killed at any byte converge on
    reopen: the plan is only honoured while it still matches the image and
    the files it was built from, so a kill followed by new writer activity
    can never install a stale image; a stale plan is discarded and the
    compacted image rebuilt. It is removed when the publish completes.

The publish itself never edits a file readers can see in place: the
checkpoint and the log are each replaced atomically (temp, fsync, rename),
and the new log is installed only after the new checkpoint already covers
it. A read-only process scans its chosen image once into memory and closes
the files at open, so a reader that pinned the old snapshot keeps serving
every byte of it from that index even after the old space is released, and
every fresh read-only open sees either the old or the new complete
committed snapshot, at the same sequence number, never a mix, a half state
or released bytes.


Read-only processes (``Store(path, read_only=True)``) take no lock and never
create or modify a file. At open each one picks the highest committed prefix
available at that instant -- the validated ``wal.log`` prefix or ``wal.ckp``
-- scans it once into memory and serves every ``get`` from that index.
Because the chosen image is complete and is only ever replaced atomically
(never edited in place), a refresh caught mid-rename, a half-written commit,
a torn record, or a log being rebuilt or shrunk can never expose partial
bytes: each read lands on one full committed snapshot that was current at or
after the open. A directory written by an older version is opened the same
way from its log and checkpoint. Reads never replay the log afterwards, so
their cost is independent of history and they never block the writer; a
reader that is killed or simply hangs changes nothing on disk.

The durable commit boundary is the highest of the validated log boundary,
the checkpoint boundary and the marker boundary, so it can never move
backwards. Every repair step is either idempotent or an atomic rename; a
kill between any two steps leaves a state the next open converges from to
the identical result.

Log frame layout (all integers big-endian)::

    +---------+----------------+------------+----------------------+---------+
    | b"WAL2" | payload_length | header_crc | payload              | crc32   |
    | 4 bytes | 8 bytes        | 4 bytes    | metadata\\n<value>    | 4 bytes |
    +---------+----------------+------------+----------------------+---------+

``header_crc`` is the crc32 of the magic and length bytes. It makes a
partial header that borrows bytes from a following frame fail validation,
so an incomplete record in the middle of the log reads as corruption while
a genuinely short final header reads as a torn tail.

Metadata is a compact JSON object: ``{"t":"p","k":key}`` for puts,
``{"t":"d","k":key}`` for deletes and ``{"t":"c","s":seq}`` for commits.
The raw value bytes follow the first newline, so values need no encoding.
A compacted log's single commit marker additionally carries ``"b":1``: it
is a *base* commit that snapshots the whole live state at its sequence
number, so its sequence need not be one (ordinary, non-base commits still
form the strictly increasing 1, 2, 3, ... chain).
"""

from __future__ import annotations

import json
import os
import sys
import zlib

__all__ = ["Store", "CorruptLogError", "inject_tear"]


class CorruptLogError(ValueError):
    """The write-ahead log is corrupt, not merely missing a torn tail.

    A single incomplete final record is a normal crash remnant and is
    discarded during recovery. Anything else that fails framing, checksum,
    metadata or commit-sequence validation is corruption; recovery stops
    without applying or removing anything.
    """


LOG_NAME = "wal.log"
_CHECKPOINT_NAME = "wal.ckp"
_MARKER_NAME = "wal.rec"
_COMPACT_NAME = "wal.cmp"
_COMPACT_PLAN_NAME = "wal.cpr"

_MAGIC = b"WAL2"
_PREFIX = 12  # 4 bytes magic + 8 bytes payload length
_HEADER = 16   # prefix + 4 bytes crc32 of the prefix
_TRAILER = 4   # crc32 of the payload
_MAX_PAYLOAD = 1 << 40

_OP_PUT = "p"
_OP_DELETE = "d"
_OP_COMMIT = "c"


def _encode_frame(op: str, value: bytes = b"", key: str | None = None,
                  seq: int | None = None, base: bool = False) -> bytes:
    meta: dict[str, object] = {"t": op}
    if key is not None:
        meta["k"] = key
    if seq is not None:
        meta["s"] = seq
    if base:
        meta["b"] = 1
    head = json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload = head + b"\n" + value
    prefix = _MAGIC + len(payload).to_bytes(8, "big")
    header = prefix + zlib.crc32(prefix).to_bytes(4, "big")
    return header + payload + zlib.crc32(payload).to_bytes(4, "big")


def _iter_frames(f):
    """Scan an open binary log file from position 0.

    For every intact frame yields
    ``("frame", meta, value, start, end)``. Scanning stops with either no
    terminal event (clean EOF), ``("torn", start)`` for a single incomplete
    final frame, or ``("invalid", start)`` for a frame that fails framing,
    header/payload checksum or metadata parsing. With the header checksum a
    partial header that borrows bytes from a following record cannot survive,
    so a short read landing exactly at EOF is reliably the one unfinished
    record a single writer was appending when it died; bytes left beyond such
    a read instead make it corruption.
    """
    f.seek(0, os.SEEK_END)
    eof = f.tell()
    f.seek(0)
    while True:
        start = f.tell()
        header = f.read(_HEADER)
        if not header:
            return
        if len(header) < _HEADER:
            # Trailing bytes of a frame header are a torn tail only when
            # nothing follows them; a gap anywhere else is corruption.
            yield (("torn", start) if f.tell() == eof
                   else ("invalid", start))
            return
        if header[:4] != _MAGIC:
            yield "invalid", start
            return
        if int.from_bytes(header[_PREFIX:], "big") != zlib.crc32(
                header[:_PREFIX]):
            yield "invalid", start
            return
        length = int.from_bytes(header[4:_PREFIX], "big")
        if length > _MAX_PAYLOAD:
            yield "invalid", start
            return
        payload = f.read(length)
        checksum = f.read(_TRAILER)
        if len(payload) < length or len(checksum) < _TRAILER:
            # Only the single final record may be incomplete.
            yield (("torn", start) if f.tell() == eof
                   else ("invalid", start))
            return
        if int.from_bytes(checksum, "big") != zlib.crc32(payload):
            yield "invalid", start
            return
        newline = payload.find(b"\n")
        if newline < 0:
            yield "invalid", start
            return
        try:
            meta = json.loads(payload[:newline])
        except (ValueError, UnicodeDecodeError):
            yield "invalid", start
            return
        if not isinstance(meta, dict) or not isinstance(meta.get("t"), str):
            yield "invalid", start
            return
        yield "frame", meta, payload[newline + 1:], start, f.tell()


def _fsync_dir(path: str) -> None:
    """Fsync a directory so a nearby rename/unlink is durable.

    Windows cannot open directories for fsync and raises ``PermissionError``
    (access denied); directory entries there are made durable by the file's
    own flush-on-rename, so skipping the directory fsync keeps store creation
    working everywhere.
    """
    if sys.platform == "win32":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            os.fsync(fd)
        except PermissionError:
            # Some non-Windows mounts reject directory fsync the same way.
            pass
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def inject_tear(source, destination, offset):
    """Copy a log cut at exactly ``offset`` bytes, for bytewise verification.

    Produces ``destination`` containing the first ``offset`` bytes of
    ``source`` -- a replica of a process killed precisely when the write had
    reached that byte offset. The source file is never modified. Either path
    argument may be a store directory, in which case its ``wal.log`` is used.

    ``offset`` must satisfy ``0 <= offset <= len(source)``; a negative or
    past-the-end offset raises ``IndexError``. The full-length copy
    (``offset == len(source)``) is the clean control case.

    Verification aid only; the normal write path never calls this.
    """
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise TypeError("offset must be an integer byte offset")

    src = os.path.join(source, LOG_NAME) if os.path.isdir(source) else source
    dst = (os.path.join(destination, LOG_NAME)
           if os.path.isdir(destination) else destination)

    size = os.path.getsize(src)
    if offset < 0 or offset > size:
        raise IndexError(
            f"tear offset {offset} out of range for log of {size} bytes")
    if os.path.abspath(src) == os.path.abspath(dst):
        raise ValueError("tear destination must differ from the source log")

    remaining = offset
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        while remaining:
            chunk = fsrc.read(min(remaining, 1 << 20))
            if not chunk:  # pragma: no cover - size came from the same file
                raise IndexError("source log shrank while copying")
            fdst.write(chunk)
            remaining -= len(chunk)
        fdst.truncate(offset)
        os.fsync(fdst.fileno())
    return dst


class Store:
    """A single-writer key value store backed by an append-only log.

    ``path`` must be an existing store directory; the log file inside it is
    created on first use. Opening a missing directory raises
    ``FileNotFoundError`` and opening a path that is not a directory raises
    ``OSError``.

    With ``read_only=True`` the store is opened purely for reading. No file
    is ever created or modified, and no lock is taken: any number of
    read-only processes may coexist with the one writer. The reader pins the
    committed snapshot current at open time -- the highest committed prefix
    of ``wal.log`` or of the atomically replaced ``wal.ckp`` sidecar --
    scans it once, and serves every ``get`` from that in-memory index. The
    snapshot is independent of the log's later history, so a read never
    replays the log and cannot block the writer, and uncommitted bytes, a
    half-written commit, a torn record or a half-finished rebuild are never
    visible. ``put``, ``delete``, ``commit`` and ``recover`` are rejected on
    a read-only store; ``stats`` reports the pinned snapshot.
    """

    def __init__(self, path: str, read_only: bool = False):
        self._dir = path
        self._path = os.path.join(path, LOG_NAME)
        self._ckp_path = os.path.join(path, _CHECKPOINT_NAME)
        self._marker_path = os.path.join(path, _MARKER_NAME)
        self._compact_path = os.path.join(path, _COMPACT_NAME)
        self._compact_plan_path = os.path.join(path, _COMPACT_PLAN_NAME)
        self._read_only = read_only

        # Recovery needs a directory to replay; a missing target is an error
        # at open time, not an implicit create.
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"store directory does not exist: {path!r}")
        if not os.path.isdir(path):
            raise OSError(f"store path is not a directory: {path!r}")

        self._data: dict[str, bytes] = {}
        self._seq = 0
        self._entries = 0
        self._pending = 0
        self._closed = False
        self._corrupt = False
        # Torn records dropped in this recovery epoch; wal.rec carries it
        # across processes until the next commit.
        self._discarded = 0

        if read_only:
            # A read-only open holds no writable fd and never takes a lock;
            # leave the writer fd slot unset for clarity.
            self._fd = -1
            self._open_read_only()
            return

        log_created = not os.path.exists(self._path)
        self._fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                           0o644)
        if log_created:
            os.fsync(self._fd)
            _fsync_dir(path)

        self._converge_on_open()

    # -- log scanning and validation --------------------------------------

    def _read_log(self, path=None):
        frames = []
        terminal = None
        with open(path or self._path, "rb") as f:
            for event in _iter_frames(f):
                if event[0] == "frame":
                    _, meta, value, start, end = event
                    frames.append((meta, value, start, end))
                else:
                    terminal = event
        return frames, terminal

    @staticmethod
    def _validate(frames, terminal):
        """Validate every frame and the strictly increasing commit sequence.

        Returns ``(committed_end, last_seq)`` -- the byte offset just past
        the last commit marker and its sequence number. An ``"invalid"``
        terminal is corruption and raises ``CorruptLogError``; a ``"torn"``
        terminal is left for the caller, which decides from the durable
        checkpoint whether it is a tail to discard or a gash to restore.

        The first commit marker of a compacted log carries ``"b":1``: it is
        a *base* commit snapshotting the whole live state, so its sequence
        may be any positive value (the sequence preserved across
        compaction); later commits then continue at ``seq + 1``. All frames
        before such a base commit must be puts with distinct keys -- the
        compacted snapshot -- and the base must be the first commit. An
        ordinary first commit still has to be sequence 1 and every later
        non-base commit must advance by exactly one.
        """
        expected_seq = 1
        last_seq = 0
        committed_end = 0
        commits_seen = 0
        for index, (meta, _value, _start, end) in enumerate(frames):
            op = meta.get("t")
            if op in (_OP_PUT, _OP_DELETE):
                key = meta.get("k")
                if not isinstance(key, str) or key == "":
                    raise CorruptLogError(
                        f"invalid frame metadata: {meta!r}")
                if "b" in meta:
                    raise CorruptLogError(
                        f"base flag on a non-commit frame: {meta!r}")
            elif op == _OP_COMMIT:
                seq = meta.get("s")
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise CorruptLogError(
                        f"invalid commit marker: {meta!r}")
                flag = meta.get("b")
                if flag is not None and not (
                        isinstance(flag, int) and not isinstance(flag, bool)
                        and flag == 1):
                    raise CorruptLogError(
                        f"invalid base flag on commit marker: {meta!r}")
                base = flag == 1
                if base:
                    if commits_seen != 0:
                        raise CorruptLogError(
                            "base commit must be the first commit marker")
                    if seq < 1:
                        raise CorruptLogError(
                            f"base commit sequence out of range: {seq}")
                    snapshot_keys: set[str] = set()
                    for pre in frames[:index]:
                        pre_meta = pre[0]
                        if pre_meta.get("t") != _OP_PUT:
                            raise CorruptLogError(
                                "compacted base snapshot contains a delete")
                        pre_key = pre_meta["k"]
                        if pre_key in snapshot_keys:
                            raise CorruptLogError(
                                "compacted base snapshot repeats a key")
                        snapshot_keys.add(pre_key)
                    expected_seq = seq + 1
                elif seq != expected_seq:
                    raise CorruptLogError(
                        f"commit sequence {seq} out of order, "
                        f"expected {expected_seq}")
                else:
                    expected_seq += 1
                commits_seen += 1
                last_seq = seq
                committed_end = end
            else:
                raise CorruptLogError(f"unknown frame type: {op!r}")
        if terminal is not None and terminal[0] == "invalid":
            raise CorruptLogError("corrupt log frame")
        return committed_end, last_seq

    @staticmethod
    def _replay(frames) -> tuple[dict[str, bytes], int]:
        state: dict[str, bytes] = {}
        applied = 0
        for meta, value, _start, _end in frames:
            op = meta["t"]
            if op == _OP_COMMIT:
                continue
            if op == _OP_PUT:
                state[meta["k"]] = value
            else:  # _OP_DELETE
                state.pop(meta["k"], None)
            applied += 1
        return state, applied

    # -- crash-convergence sidecars ---------------------------------------

    @staticmethod
    def _copy_prefix(fsrc, fdst, length: int) -> None:
        remaining = length
        while remaining:
            chunk = fsrc.read(min(remaining, 1 << 20))
            if not chunk:
                raise CorruptLogError(
                    "source ended before the committed prefix")
            fdst.write(chunk)
            remaining -= len(chunk)
        fdst.truncate(length)

    def _atomic_file(self, target: str, write) -> None:
        """Replace ``target`` atomically: temp file, fsync, rename, dir fsync."""
        tmp = target + ".tmp"
        with open(tmp, "wb") as f:
            write(f)
            os.fsync(f.fileno())
        os.replace(tmp, target)
        _fsync_dir(self._dir)

    def _write_checkpoint(self, end: int) -> None:
        """Persist the log prefix ``[0, end)`` as the clean checkpoint."""
        if end <= 0:
            return

        def write(fdst):
            with open(self._path, "rb") as fsrc:
                self._copy_prefix(fsrc, fdst, end)

        self._atomic_file(self._ckp_path, write)

    def _checkpoint_boundary(self) -> int:
        """Commit boundary the durable checkpoint covers; 0 if absent.

        A checkpoint that is torn, unparseable or not exactly one committed
        prefix is corruption of the recovery machinery rather than a discard.
        """
        try:
            frames, terminal = self._read_log(self._ckp_path)
        except FileNotFoundError:
            return 0
        if not frames:
            # An empty checkpoint covers the empty prefix; anything non-empty
            # that parses to no frames is corruption.
            if os.path.getsize(self._ckp_path) == 0 and terminal is None:
                return 0
            raise CorruptLogError("checkpoint is not a clean committed prefix")
        if terminal is not None:
            raise CorruptLogError("checkpoint is not a clean committed prefix")
        end, _seq = self._validate(frames, None)
        if end != os.path.getsize(self._ckp_path):
            raise CorruptLogError("checkpoint has bytes past its commit")
        return end

    def _write_marker(self, clean_end: int, discarded: int) -> None:
        payload = json.dumps(
            {"end": clean_end, "discarded": discarded},
            separators=(",", ":")).encode("utf-8")

        def write(f):
            f.write(payload)

        self._atomic_file(self._marker_path, write)

    def _read_marker(self):
        try:
            with open(self._marker_path, "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            return None
        try:
            marker = json.loads(raw)
            end = marker["end"]
            discarded = marker["discarded"]
        except (ValueError, TypeError, KeyError):
            raise CorruptLogError("unparseable recovery marker")
        if (not isinstance(end, int) or isinstance(end, bool) or end < 0
                or discarded not in (0, 1)):
            raise CorruptLogError("invalid recovery marker")
        return end, discarded

    def _remove_marker(self) -> None:
        try:
            os.unlink(self._marker_path)
        except FileNotFoundError:
            return
        _fsync_dir(self._dir)

    # -- compaction sidecars ----------------------------------------------

    def _write_compact_plan(self, seq: int, length: int,
                            log_size: int, ckp_size: int) -> None:
        payload = json.dumps(
            {"s": seq, "n": length, "log": log_size, "ckp": ckp_size},
            separators=(",", ":")).encode("utf-8")

        def write(f):
            f.write(payload)

        self._atomic_file(self._compact_plan_path, write)

    def _read_compact_plan(self):
        """Return ``(seq, length, log_size, ckp_size)`` or ``None``.

        A torn or malformed plan is corruption of the convergence machinery,
        not a discardable tail: the plan itself is always atomically
        replaced, so it cannot be half-written by a kill.
        """
        try:
            with open(self._compact_plan_path, "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            return None
        try:
            plan = json.loads(raw)
            seq = plan["s"]
            length = plan["n"]
            log_size = plan["log"]
            ckp_size = plan["ckp"]
        except (ValueError, TypeError, KeyError):
            raise CorruptLogError("unparseable compaction plan")

        def ok(v, allow_minus_one=False) -> bool:
            return (isinstance(v, int) and not isinstance(v, bool)
                    and v >= (-1 if allow_minus_one else 0))

        if (not ok(seq) or not ok(length) or not ok(log_size)
                or not ok(ckp_size, allow_minus_one=True)):
            raise CorruptLogError("invalid compaction plan")
        return seq, length, log_size, ckp_size

    def _read_compact_image(self, seq: int, length: int):
        """Validate ``wal.cmp`` as the exact compacted image of ``seq``.

        Returns its frames. The image must parse as one exact committed
        prefix ending in a single *base* commit carrying ``seq`` (an empty
        zero-length image is the compacted form of the never-committed
        store, seq 0); anything else is corruption of the machinery.
        """
        try:
            image = self._read_clean_prefix(self._compact_path)
        except FileNotFoundError:
            image = None
        if image is None:
            raise CorruptLogError(
                "compaction plan without its compacted image")
        frames, end, image_seq = image
        if end != length or image_seq != seq:
            raise CorruptLogError("compaction image does not match its plan")
        if length == 0:
            if seq != 0 or frames:
                raise CorruptLogError("invalid empty compaction image")
            return frames
        base = next((meta for meta, _v, _s, _e in frames
                     if meta.get("t") == _OP_COMMIT), None)
        if base is None or base.get("b") != 1:
            raise CorruptLogError("compaction image is missing its base commit")
        if any(meta.get("t") == _OP_COMMIT for meta, _v, _s, _e in
               frames[:-1]) or frames[-1][0].get("t") != _OP_COMMIT:
            raise CorruptLogError("compaction image has extra commit markers")
        return frames

    def _remove_compaction_sidecars(self) -> None:
        for name in (self._compact_plan_path, self._compact_path):
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass
        _fsync_dir(self._dir)

    def _install_log_from(self, src_path: str, length: int) -> None:
        """Atomically replace the log with ``[0, length)`` of ``src_path``.

        Mirrors the checkpoint rebuild: the replacement is fully written
        and fsynced as a temp file, so a kill leaves either the old log or
        the complete new one -- never a half-written file -- and rerunning
        converges. The append fd is closed for the rename (Windows cannot
        rename over a path it holds) and reopened afterwards.
        """
        tmp = self._path + ".tmp"
        with open(src_path, "rb") as fsrc, open(tmp, "wb") as fdst:
            self._copy_prefix(fsrc, fdst, length)
            os.fsync(fdst.fileno())
        os.close(self._fd)
        try:
            os.replace(tmp, self._path)
            _fsync_dir(self._dir)
        finally:
            self._fd = os.open(
                self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if os.fstat(self._fd).st_size != length:
            raise CorruptLogError("compaction did not converge")

    def _prepare_compaction(self, plan):
        """Make the staged image match an interrupted plan, or declare stale.

        Returns the compacted image's frames when the plan can still be
        finished, and ``None`` when a strictly newer durable commit makes it
        obsolete (the caller discards the plan/image and recovers
        normally).

        Convergence at *any* byte position is the point: the compacted
        image is a deterministic function of the committed state, so a torn
        or missing ``wal.cmp`` is simply regenerated from whichever intact
        committed source still carries the plan's sequence -- the log's
        committed prefix or the checkpoint -- and a torn checkpoint/log is
        overwritten by the publish anyway. Genuine ambiguity -- no intact
        source at the plan's sequence, or a regenerated image whose length
        disagrees with the plan -- is ``CorruptLogError``. A newer commit
        cannot be hidden: every commit durably advances either the log
        boundary or the atomically replaced checkpoint, and the single
        writer always finishes a pending plan at open before appending.
        """
        seq, length, _log_size, _ckp_size = plan

        frames, terminal = self._read_log()
        log_end, log_seq = self._validate(frames, terminal)

        ckp = None
        if os.path.exists(self._ckp_path):
            try:
                ckp = self._read_clean_prefix(self._ckp_path)
            except CorruptLogError:
                # A torn or damaged checkpoint is repairable only while the
                # log itself pins the plan's sequence.
                ckp = "damaged"
        ckp_seq = ckp[2] if isinstance(ckp, tuple) else 0

        durable_seq = max(log_seq, ckp_seq)
        if durable_seq > seq:
            return None
        if durable_seq < seq:
            raise CorruptLogError(
                "compaction plan has no committed source at its sequence")

        if log_seq == seq:
            # The committed prefix is intact even when a torn or merely
            # uncommitted tail follows it (an "invalid" terminal already
            # raised in _validate); replay exactly the committed frames.
            source = [fr for fr in frames if fr[3] <= log_end]
        elif isinstance(ckp, tuple) and ckp_seq == seq:
            source = ckp[0]
        else:
            raise CorruptLogError(
                "no intact committed source to finish compaction")

        state, _applied = self._replay(source)
        parts = [_encode_frame(_OP_PUT, value=value, key=key)
                 for key, value in state.items()]
        if seq:
            parts.append(_encode_frame(_OP_COMMIT, seq=seq, base=True))
        image = b"".join(parts)
        if len(image) != length:
            raise CorruptLogError(
                "compaction plan does not match the committed state")

        # Keep an already-published, valid image; otherwise republish the
        # regenerated one atomically before touching any reader-visible
        # file.
        image_ok = False
        try:
            existing = self._read_clean_prefix(self._compact_path)
        except CorruptLogError:
            existing = None
        if existing is None:
            image_ok = False
        else:
            ex_frames, ex_end, ex_seq = existing
            image_ok = (
                ex_end == length and ex_seq == seq and
                (length == 0 or (
                    ex_frames[-1][0].get("t") == _OP_COMMIT
                    and ex_frames[-1][0].get("b") == 1
                    and all(m.get("t") == _OP_PUT
                            for m, _v, _s, _e in ex_frames[:-1]))))
        if not image_ok:
            def write_image(f):
                f.write(image)

            self._atomic_file(self._compact_path, write_image)

        return self._read_compact_image(seq, length)

    def _finish_compaction(self, plan):
        """Publish the staged compacted image; idempotent and kill-safe.

        The checkpoint is replaced first, so a complete restorable image is
        published before the old log is unlinked; the log is installed from
        the same bytes immediately afterwards. Each step is an atomic
        rename of a fully fsynced temp file, so a kill at any point leaves
        whole files only and rerunning repeats the same publish.
        """
        seq, length, _log_size, _ckp_size = plan
        if length:
            def write_checkpoint(fdst):
                with open(self._compact_path, "rb") as fsrc:
                    self._copy_prefix(fsrc, fdst, length)

            self._atomic_file(self._ckp_path, write_checkpoint)
        else:
            try:
                os.unlink(self._ckp_path)
                _fsync_dir(self._dir)
            except FileNotFoundError:
                pass

        self._install_log_from(self._compact_path, length)
        # The compacted snapshot closes any open recovery epoch: the torn
        # tail it recorded is history that no longer exists.
        self._remove_marker()
        # Drop the plan before its image, so a kill in between can never
        # leave a plan that points at a missing image.
        try:
            os.unlink(self._compact_plan_path)
        except FileNotFoundError:
            pass
        _fsync_dir(self._dir)
        try:
            os.unlink(self._compact_path)
        except FileNotFoundError:
            pass
        _fsync_dir(self._dir)

        frames, terminal = self._read_log()
        end, published_seq = self._validate(frames, terminal)
        if end != length or published_seq != seq or terminal is not None:
            raise CorruptLogError("compaction did not converge")
        return frames, 0

    def _remove_stale_temps(self) -> None:
        for name in (self._path + ".tmp", self._ckp_path + ".tmp",
                     self._marker_path + ".tmp",
                     self._compact_path + ".tmp",
                     self._compact_plan_path + ".tmp"):
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass

    def _rebuild_log(self, clean_end: int) -> None:
        """Make the log exactly its clean ``[0, clean_end)`` prefix.

        Kill-safe and idempotent: when a checkpoint covers the prefix the new
        log is fully written and fsynced as a temp file and atomically renamed
        over the old one, so a kill leaves either the old log or the complete
        restored log -- never a half-written file -- and rerunning converges.
        Without a checkpoint but with the committed bytes still at stable
        offsets, a synced truncate drops the tail instead. The empty prefix is
        a truncate to zero.
        """
        size = os.fstat(self._fd).st_size
        if size == clean_end:
            return

        if clean_end == 0:
            os.ftruncate(self._fd, 0)
            os.fsync(self._fd)
            return

        try:
            ckp_size = os.path.getsize(self._ckp_path)
        except FileNotFoundError:
            ckp_size = -1

        if ckp_size < clean_end and size > clean_end:
            # No checkpoint, but the committed bytes survive at stable
            # offsets: drop the dirty tail. Idempotent if killed mid-call.
            try:
                os.fsync(self._fd)
            except OSError:
                pass
            os.ftruncate(self._fd, clean_end)
            os.fsync(self._fd)
            return
        if ckp_size < clean_end:
            raise CorruptLogError(
                "committed prefix is torn and no checkpoint covers it")

        tmp = self._path + ".tmp"
        with open(self._ckp_path, "rb") as fsrc, open(tmp, "wb") as fdst:
            self._copy_prefix(fsrc, fdst, clean_end)
            os.fsync(fdst.fileno())
        # Windows cannot rename over a path held open by the append fd.
        os.close(self._fd)
        try:
            os.replace(tmp, self._path)
            _fsync_dir(self._dir)
        finally:
            self._fd = os.open(
                self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if os.fstat(self._fd).st_size != clean_end:
            raise CorruptLogError("restore did not converge")

    def _converge(self):
        """Validate and converge the on-disk log to its clean state.

        Returns ``(frames, discarded)`` for the now-clean log. Raises
        ``CorruptLogError`` without changing anything if the log is corrupt.
        Safe to run repeatedly and safe to kill at any point.

        The durable commit boundary is the highest of the validated log
        boundary, the checkpoint boundary and the marker boundary, so it can
        never regress. A log torn inside its committed prefix is rebuilt from
        the checkpoint and ``discarded`` stays 0 (those bytes were committed,
        not an unfinished write); a torn or merely uncommitted tail past the
        boundary is discarded, and the marker freezes that report until the
        next commit even when the kill lands mid-repair.
        """
        self._remove_stale_temps()

        # A compaction interrupted at any point converges before any other
        # repair: its plan is only honoured while it still matches the exact
        # files it was published between, so a kill followed by newer
        # activity simply discards the stale plan and image and continues
        # with normal recovery from the newer commit.
        plan = self._read_compact_plan()
        if plan is not None:
            if self._prepare_compaction(plan) is not None:
                return self._finish_compaction(plan)
            self._remove_compaction_sidecars()
        elif os.path.exists(self._compact_path):
            # No plan means the publish already completed (the plan is only
            # removed once the new checkpoint and log are both installed);
            # a kill before the image unlink left this orphan.
            try:
                os.unlink(self._compact_path)
                _fsync_dir(self._dir)
            except FileNotFoundError:
                pass

        marker = self._read_marker()
        marker_end = -1
        marker_discarded = 0
        if marker is not None:
            marker_end, marker_discarded = marker

        frames, terminal = self._read_log()
        log_end, log_seq = self._validate(frames, terminal)
        ckp_end = self._checkpoint_boundary()
        size = os.fstat(self._fd).st_size

        target = max(log_end, ckp_end, marker_end)
        # The target bytes must come from somewhere still on disk: the
        # validated log itself or the checkpoint.
        if target > log_end and target > ckp_end:
            raise CorruptLogError("recovery target has no durable source")

        log_clean = terminal is None and size == log_end

        if log_clean and log_end == target:
            # Nothing to repair. An old marker whose epoch a newer commit
            # closed durably can finally go; otherwise its discarded count
            # stays frozen until that commit.
            if marker is not None:
                if target > marker_end:
                    self._remove_marker()
                    discarded = 0
                else:
                    discarded = marker_discarded
            else:
                discarded = 0
            if ckp_end < target:
                self._write_checkpoint(target)
            return frames, discarded

        if target > log_end:
            # Committed bytes were torn away (offset 0 included): the
            # checkpoint is the source of truth. The discarded count is only
            # meaningful when the marker pins this exact boundary.
            discarded = marker_discarded if target == marker_end else 0
            self._rebuild_log(target)
        else:
            # A dirty suffix sits past the boundary. Decide discarded from
            # the suffix itself: a torn final record counts, complete
            # uncommitted frames do not. This also covers a repair killed
            # before the rebuild (the original suffix is still here, so the
            # same decision is reached) and fresh writes appended after a
            # previous repair (a new crash is a new decision rather than the
            # old marker's). A marker is only frozen once the log itself is
            # rebuilt clean; see the clean branch above.
            discarded = 1 if terminal is not None else 0
            if ckp_end < target:
                # Make the prefix independently restorable before we publish
                # the plan or remove any bytes.
                self._write_checkpoint(target)
            self._write_marker(target, discarded)
            self._rebuild_log(target)

        frames, terminal = self._read_log()
        end, _seq = self._validate(frames, terminal)
        if end != target or terminal is not None:
            raise CorruptLogError("restore did not converge")
        return frames, discarded

    def _publish(self, frames, discarded) -> None:
        state, _applied = self._replay(frames)
        self._data = state
        self._seq = max(
            (meta["s"] for meta, _v, _s, _e in frames
             if meta.get("t") == _OP_COMMIT),
            default=0)
        self._entries = len(frames)
        self._pending = 0
        self._discarded = discarded
        self._corrupt = False

    def _converge_on_open(self) -> None:
        """Crash cleanup at open; leave a corrupt store inert for recover()."""
        try:
            frames, discarded = self._converge()
        except CorruptLogError:
            self._corrupt = True
            return
        self._publish(frames, discarded)

    # -- read-only opens --------------------------------------------------

    def _open_read_only(self) -> None:
        """Pin a committed snapshot without creating or modifying anything.

        No lock is taken and nothing on disk is written. The reader chooses
        the highest committed prefix available at the instant it opens --
        the validated prefix of ``wal.log`` or the existing ``wal.ckp``
        sidecar, which the writer replaces atomically as part of every
        commit -- scans that prefix once into memory, and serves every
        ``get`` from that index. Later reads are independent of log history,
        never replay the log and never block the writer.

        The checkpoint is a complete, self-contained image that is never
        modified in place (only atomically replaced), so reading it cannot
        expose uncommitted bytes, a half-written commit, a torn record or a
        half-finished rebuild/shrink. It is also what makes the pin safe once
        the reader closes the file: the image survives any later rename.
        """
        frames, end, seq = self._choose_readonly_prefix()
        state, _applied = self._replay(frames)
        self._ro_index: dict[str, bytes] = state
        self._ro_seq = seq
        self._ro_entries = len(frames)
        self._ro_bytes = end

    def _choose_readonly_prefix(self):
        """Return ``(frames, end, seq)`` for the highest committed snapshot.

        Takes the higher of the validated log boundary and the checkpoint
        boundary, mirroring the writer's durable-boundary rule. A torn or
        merely uncommitted tail past the last commit is excluded; a framing
        or sequence failure in committed bytes is corruption and raises
        ``CorruptLogError``. The checkpoint is the source when a crash or a
        concurrent rebuild has torn/shrunk the committed log bytes.
        """
        log_frames = None
        log_end = -1
        log_seq = 0
        if os.path.exists(self._path):
            frames, terminal = self._read_log(self._path)
            log_end, log_seq = self._validate(frames, terminal)
            # Keep only frames at or before the last commit; the torn final
            # record already arrives as the terminal, not as a frame.
            log_frames = [fr for fr in frames if fr[3] <= log_end]

        ckp = self._read_clean_prefix(self._ckp_path)
        if ckp is None:
            if log_frames is None:
                return [], 0, 0
            return log_frames, log_end, log_seq

        ckp_frames, ckp_end, ckp_seq = ckp
        if log_end >= ckp_end:
            return log_frames, log_end, log_seq
        return ckp_frames, ckp_end, ckp_seq

    def _read_clean_prefix(self, path: str):
        """Validate ``path`` as an exact committed prefix.

        Returns ``(frames, end, seq)`` or ``None`` if the file is absent.
        Raises ``CorruptLogError`` when it exists but is not exactly a clean
        committed prefix: such a sidecar is replaced atomically, so a torn
        or unparseable image cannot be a publish caught mid-flight. The size
        used for the "bytes past its commit" check is taken from the same
        open inode that was scanned, so a concurrent atomic replace can
        never mix the bytes of one version with the size of another.
        """
        try:
            f = open(path, "rb")
        except FileNotFoundError:
            return None
        with f:
            frames = []
            terminal = None
            for event in _iter_frames(f):
                if event[0] == "frame":
                    _, meta, value, start, end = event
                    frames.append((meta, value, start, end))
                else:
                    terminal = event
            size = os.fstat(f.fileno()).st_size
        if terminal is not None:
            raise CorruptLogError("sidecar is not a clean committed prefix")
        if not frames:
            if size == 0:
                return frames, 0, 0
            raise CorruptLogError("sidecar is not a clean committed prefix")
        end, seq = self._validate(frames, None)
        if end != size:
            raise CorruptLogError("sidecar has bytes past its commit")
        return frames, end, seq

    def _read_key(self, key: str) -> bytes | None:
        value = self._ro_index.get(key)
        return None if value is None else bytes(value)

    # -- argument validation ----------------------------------------------

    @staticmethod
    def _check_key(key) -> None:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must not be empty")

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("store is closed")

    def _ensure_writable(self) -> None:
        self._ensure_open()
        if self._read_only:
            raise ValueError("store is opened read-only")
        if self._corrupt:
            raise CorruptLogError(
                "log is corrupt; call recover() to diagnose before writing")

    # -- public API --------------------------------------------------------

    def put(self, key: str, value: bytes) -> None:
        self._ensure_writable()
        self._check_key(key)
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes")
        _write_all(self._fd, _encode_frame(_OP_PUT, value=value, key=key))
        self._data[key] = value
        self._pending += 1
        self._entries += 1

    def get(self, key: str) -> bytes | None:
        self._ensure_open()
        self._check_key(key)
        if self._read_only:
            return self._read_key(key)
        return self._data.get(key)

    def delete(self, key: str) -> None:
        self._ensure_writable()
        self._check_key(key)
        _write_all(self._fd, _encode_frame(_OP_DELETE, key=key))
        self._data.pop(key, None)
        self._pending += 1
        self._entries += 1

    def commit(self) -> int:
        self._ensure_writable()
        if self._pending == 0:
            return self._seq
        _write_all(self._fd, _encode_frame(_OP_COMMIT, seq=self._seq + 1))
        os.fsync(self._fd)

        # The commit frame is durable first, so even a kill here leaves a
        # clean log the next open refreshes the checkpoint from. Publish the
        # new restorable prefix, then close the recovery epoch.
        clean_end = os.fstat(self._fd).st_size
        self._write_checkpoint(clean_end)
        had_marker = os.path.exists(self._marker_path)
        self._seq += 1
        self._pending = 0
        self._entries += 1
        self._discarded = 0
        if had_marker:
            self._remove_marker()
        return self._seq

    def recover(self) -> dict:
        """Replay the log and return ``{"applied", "discarded", "seq"}``.

        ``applied`` counts the committed put/delete records replayed,
        ``discarded`` is 1 when a single torn final record was dropped (0
        otherwise) and ``seq`` is the durable sequence number after recovery.
        Raises ``CorruptLogError`` (a ``ValueError``) if the log is corrupt;
        nothing is applied or truncated in that case.

        Recovery is idempotent and kill-safe: interrupting it any number of
        times and rerunning converges to the same state and the same report
        as one clean recovery, and the next commit always uses ``seq + 1``.
        """
        self._ensure_open()
        if self._read_only:
            raise ValueError("cannot recover a store opened read-only")
        if self._pending:
            raise ValueError(
                "cannot recover with uncommitted changes in this session")

        # Re-validate and re-converge; a genuinely corrupt log raises here
        # before anything is applied or removed.
        frames, discarded = self._converge()

        state, applied = self._replay(frames)
        seq = max(
            (meta["s"] for meta, _v, _s, _e in frames
             if meta.get("t") == _OP_COMMIT),
            default=0)
        self._data = state
        self._seq = seq
        self._entries = len(frames)
        self._pending = 0
        self._discarded = discarded
        self._corrupt = False
        return {"applied": applied, "discarded": discarded, "seq": seq}

    def compact(self) -> dict:
        """Rewrite committed history into one compact log and free the old.

        After compaction the log holds exactly one committed record per key
        alive in the current committed snapshot, followed by a single base
        commit marker carrying the *same* durable sequence number, so the
        sequence never moves and the next commit is still ``seq + 1``. The
        old log's space is released; a store that never committed compacts
        to an empty log, and keys only ever present in deleted history do
        not come back.

        Compaction is kill-safe at every byte and idempotent: the compacted
        image is fully staged and fsynced as ``wal.cmp`` and a plan marker
        ``wal.cpr`` is atomically published before any reader-visible file
        is replaced, whereupon the checkpoint and log are each replaced
        atomically. A kill at any point leaves only whole files, and
        reopening (or calling ``compact`` again) converges to exactly the
        bytes one clean compaction produces. Read-only processes never open
        the staging files, so a pinned reader keeps its old snapshot while a
        new reader sees either complete snapshot, never a mixture.

        Requires a clean session: uncommitted pending mutations are
        rejected, exactly as for ``recover``. Returns the post-compaction
        stats: ``{"seq", "entries", "bytes"}``.
        """
        self._ensure_writable()
        if self._pending:
            raise ValueError(
                "cannot compact with uncommitted changes in this session")

        # Finish (or discard and redo) any compaction a kill interrupted and
        # converge ordinary torn-tail repairs first, so the image is built
        # from one fully committed state.
        frames, _discarded = self._converge()
        state, _applied = self._replay(frames)
        seq = max(
            (meta["s"] for meta, _v, _s, _e in frames
             if meta.get("t") == _OP_COMMIT),
            default=0)

        parts = [_encode_frame(_OP_PUT, value=value, key=key)
                 for key, value in state.items()]
        if seq:
            parts.append(_encode_frame(_OP_COMMIT, seq=seq, base=True))
        image = b"".join(parts)
        length = len(image)

        log_size = os.fstat(self._fd).st_size
        try:
            ckp_size = os.path.getsize(self._ckp_path)
        except FileNotFoundError:
            ckp_size = -1

        def write_image(f):
            f.write(image)

        # Image first, plan second: a kill between them leaves a harmless
        # orphan the next open removes; a plan never points at a missing or
        # partial image.
        self._atomic_file(self._compact_path, write_image)
        self._write_compact_plan(seq, length, log_size, ckp_size)
        frames, _discarded = self._finish_compaction((
            seq, length, log_size, ckp_size))
        self._publish(frames, 0)
        return self.stats()

    def stats(self) -> dict:
        self._ensure_open()
        if self._read_only:
            # The pinned snapshot's sequence, frame count and byte size; it
            # does not move as the writer appends later history.
            return {
                "seq": self._ro_seq,
                "entries": self._ro_entries,
                "bytes": self._ro_bytes,
            }
        return {
            "seq": self._seq,
            "entries": self._entries,
            "bytes": os.fstat(self._fd).st_size,
        }

    def close(self) -> None:
        if self._closed:
            return
        if not self._read_only:
            os.close(self._fd)
        self._closed = True

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
