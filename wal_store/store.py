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

Read-only processes (``Store(path, read_only=True)``) take no lock and never
create or modify a file. At open each one picks the highest committed prefix
available at that instant -- the validated ``wal.log`` prefix or ``wal.ckp``
-- scans it once into memory and serves every ``get`` from that index.
Because the chosen image is complete and is only ever replaced atomically
(never edited in place), a refresh caught mid-rename, a half-written commit,
a torn record, or a log being rebuilt, shrunk or compacted can never expose
partial bytes: each read lands on one full committed snapshot that was
current at or after the open. A directory written by an older version is
opened the same way from its log and checkpoint. Reads never replay the log
afterwards, so their cost is independent of history and they never block
the writer; a reader that is killed or simply hangs changes nothing on disk.

Compaction (``Store.compact()``) rewrites the committed history into one
tight image without losing a byte of committed state. The compacted log
holds exactly the live puts -- one per currently committed key, in sorted
key order -- followed by a *base commit* marker carrying the pre-compaction
sequence number. The durable sequence keeps that value, so the next commit
is ``seq + 1`` and every later commit continues the strict sequence; keys
that were deleted never come back. The image is assembled in memory and
staged as the atomically replaced ``wal.cmp`` sidecar; finishing installs
it as ``wal.ckp`` first and as ``wal.log`` second. Every intermediate state
is therefore either the pair of old files or the pair of complete new
images, which is precisely the two-image world the reader already chooses
between: a kill anywhere leaves a state reopening converges by simply
repeating the finish, and interrupting compaction any number of times
produces the same bytes as one clean run. Live readers keep serving their
pinned old snapshots (the inode survives on Unix; on Windows readers hold
no writer-side fd) and new readers land on the new snapshot, never a mix,
a half-written file or a byte that was already reclaimed.

The durable commit boundary is the highest of the validated log boundary,
the checkpoint boundary and the marker boundary, so it can never move
backwards. Every repair step is either idempotent or an atomic rename; a
kill between any two steps leaves a state the next open converges from to
the identical result.

Range scans (``Store.scan(start, end)``) hand out an ordered cursor over
the committed snapshot pinned at the moment the cursor opens. The cursor
materialises the matching live keys once, sorted by their raw bytes, and
then serves them independently of the store: commits, compaction or crash
recovery that happen while it is being read never change what it yields,
and it keeps reading after the log space its snapshot came from has been
reclaimed. Keys that only ever appeared in delete history never show up,
an overwritten key yields only its last committed value, and empty values
are returned like any other. The range is half-open -- the start key is
included, the end key excluded, and a ``None`` endpoint leaves that side
unbounded. Scan cost tracks the number of live keys, never the length of
the log history, and a scan never takes a lock or writes a file.

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
``{"t":"d","k":key}`` for deletes, ``{"t":"c","s":seq}`` for ordinary
commits and ``{"t":"b","s":seq}`` for the base commit that closes a
compacted log. The two carry the sequence identically; a base marker is
additionally required to be the first commit marker in its file.
The raw value bytes follow the first newline, so values need no encoding.
"""

from __future__ import annotations

import io
import json
import os
import sys
import zlib

__all__ = ["Store", "ScanCursor", "CorruptLogError", "inject_tear"]


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

_MAGIC = b"WAL2"
_PREFIX = 12  # 4 bytes magic + 8 bytes payload length
_HEADER = 16   # prefix + 4 bytes crc32 of the prefix
_TRAILER = 4   # crc32 of the payload
_MAX_PAYLOAD = 1 << 40

_OP_PUT = "p"
_OP_DELETE = "d"
_OP_COMMIT = "c"
_OP_BASE = "b"
_COMMIT_OPS = (_OP_COMMIT, _OP_BASE)

# os.open() defaults to text mode on Windows, which would inflate every
# b"\n" payload byte to CRLF on write; the log must be strictly binary so
# the same history lands as the same bytes on every platform.
_O_BINARY = getattr(os, "O_BINARY", 0)


def _encode_frame(op: str, value: bytes = b"", key: str | None = None,
                  seq: int | None = None) -> bytes:
    meta: dict[str, object] = {"t": op}
    if key is not None:
        meta["k"] = key
    if seq is not None:
        meta["s"] = seq
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
    working everywhere. Other platforms occasionally refuse the directory
    open or the fsync the same way (restrictive mounts, virtualised FSes);
    that is likewise best-effort -- the file's own fsync is what carries the
    data -- so the system refusing either step never aborts store creation
    or a commit/compaction.
    """
    if sys.platform == "win32":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError:
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


class ScanCursor:
    """Ordered cursor over one pinned committed snapshot.

    Yields ``(key, value)`` pairs in raw key-byte order. The snapshot is
    fully materialised when the cursor is created, so later commits,
    compaction, crash recovery or reclamation of the underlying log space
    never change what it yields and can never make it fail. The cursor is
    independent of the store it came from; closing the store does not
    close it.

    Reading a closed cursor raises ``ValueError``. ``close()`` is
    idempotent, and the cursor is a context manager.
    """

    def __init__(self, items):
        self._items = items
        self._pos = 0
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise ValueError("scan cursor is closed")
        if self._pos >= len(self._items):
            raise StopIteration
        item = self._items[self._pos]
        self._pos += 1
        return item

    def close(self) -> None:
        self._closed = True
        self._items = []

    def __enter__(self) -> "ScanCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


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
    visible. ``put``, ``delete``, ``commit``, ``recover`` and ``compact``
    are rejected on a read-only store; ``stats`` reports the pinned
    snapshot.

    ``scan(start, end)`` -- on either open form -- returns an ordered
    cursor over the committed snapshot pinned at that moment, yielding
    ``(key, value)`` pairs in raw key-byte order over the half-open range.
    The cursor is materialised at open and is then independent of the
    store and of everything that later happens to the log.
    """

    def __init__(self, path: str, read_only: bool = False):
        self._dir = path
        self._path = os.path.join(path, LOG_NAME)
        self._ckp_path = os.path.join(path, _CHECKPOINT_NAME)
        self._marker_path = os.path.join(path, _MARKER_NAME)
        self._cmp_path = os.path.join(path, _COMPACT_NAME)
        self._read_only = read_only

        # Recovery needs a directory to replay; a missing target is an error
        # at open time, not an implicit create.
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"store directory does not exist: {path!r}")
        if not os.path.isdir(path):
            raise OSError(f"store path is not a directory: {path!r}")

        self._data: dict[str, bytes] = {}
        # Keys touched since the last commit, mapped to the value they held
        # in the committed state (None when the key was absent). It lets a
        # scan pin the committed snapshot without replaying the log, while
        # costing memory only for as-yet uncommitted keys.
        self._uncommitted: dict[str, bytes | None] = {}
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
        self._fd = os.open(self._path,
                           os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY,
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

        A compacted image starts with a base commit marker (``"b"``) that
        carries the pre-compaction sequence: it must be the first commit
        marker in the file, it establishes the expected sequence at
        ``seq + 1`` and no second base marker may ever follow. An ordinary
        first commit must still be sequence 1.
        """
        expected_seq = 1
        last_seq = 0
        committed_end = 0
        for meta, _value, _start, end in frames:
            op = meta.get("t")
            if op in (_OP_PUT, _OP_DELETE):
                key = meta.get("k")
                if not isinstance(key, str) or key == "":
                    raise CorruptLogError(
                        f"invalid frame metadata: {meta!r}")
            elif op == _OP_BASE:
                seq = meta.get("s")
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise CorruptLogError(
                        f"invalid base commit marker: {meta!r}")
                if last_seq != 0:
                    raise CorruptLogError(
                        "base commit must be the first commit marker")
                if seq < 1:
                    raise CorruptLogError(
                        f"base commit sequence {seq} out of order")
                last_seq = seq
                expected_seq = seq + 1
                committed_end = end
            elif op == _OP_COMMIT:
                seq = meta.get("s")
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise CorruptLogError(
                        f"invalid commit marker: {meta!r}")
                if seq != expected_seq:
                    raise CorruptLogError(
                        f"commit sequence {seq} out of order, "
                        f"expected {expected_seq}")
                last_seq = seq
                expected_seq += 1
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
            if op in _COMMIT_OPS:
                continue
            if op == _OP_PUT:
                state[meta["k"]] = value
            else:  # _OP_DELETE
                state.pop(meta["k"], None)
            applied += 1
        return state, applied

    @staticmethod
    def _last_seq(frames) -> int:
        return max(
            (meta["s"] for meta, _v, _s, _e in frames
             if meta.get("t") in _COMMIT_OPS),
            default=0)

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

    def _remove_stale_temps(self) -> None:
        for name in (self._path + ".tmp", self._ckp_path + ".tmp",
                     self._marker_path + ".tmp", self._cmp_path + ".tmp"):
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
                self._path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY, 0o644)
        if os.fstat(self._fd).st_size != clean_end:
            raise CorruptLogError("restore did not converge")

    # -- compaction --------------------------------------------------------

    def _build_compact_image(self, seq: int) -> tuple[bytes, int]:
        """Serialise the committed state into one compact log image.

        One put frame per live key, keys in sorted order, followed by a base
        commit marker carrying the pre-compaction sequence. The empty state
        at seq 0 is an empty image rather than a zero-sequence marker.
        """
        if seq == 0:
            return b"", 0
        parts = [_encode_frame(_OP_PUT, value, key=key)
                 for key, value in sorted(self._data.items())]
        parts.append(_encode_frame(_OP_BASE, seq=seq))
        return b"".join(parts), len(self._data)

    def _read_compact_candidate(self):
        """Validate ``wal.cmp`` as one compacted image.

        Returns ``(image, end, seq, live)`` where ``image`` is the exact
        byte content, or ``None`` when the sidecar is absent. The image must
        be a clean file ending exactly on a base commit marker, preceded only
        by put frames with strictly increasing keys; anything else is
        corruption of the crash-convergence machinery, never a discardable
        tail (the sidecar is always atomically replaced, never written in
        place).
        """
        try:
            f = open(self._cmp_path, "rb")
        except FileNotFoundError:
            return None
        with f:
            image = f.read()
        if not image:
            # An empty candidate encodes the empty state at seq 0.
            return b"", 0, 0, 0

        frames, terminal = self._scan_bytes(image)
        if terminal is not None:
            raise CorruptLogError(
                "compaction candidate is not a clean committed prefix")
        end, seq = self._validate(frames, None)
        if end != len(image) or seq <= 0:
            raise CorruptLogError(
                "compaction candidate has bytes past its base commit")
        live = 0
        last_key: str | None = None
        saw_base = False
        for meta, _value, _start, _end in frames:
            op = meta.get("t")
            if op == _OP_PUT:
                if saw_base:
                    raise CorruptLogError(
                        "compaction candidate has puts past its base commit")
                key = meta["k"]
                if last_key is not None and key <= last_key:
                    raise CorruptLogError(
                        "compaction candidate keys are not sorted uniquely")
                last_key = key
                live += 1
            elif op == _OP_BASE:
                if saw_base:
                    raise CorruptLogError(
                        "compaction candidate has multiple base commits")
                saw_base = True
            else:
                raise CorruptLogError(
                    "compaction candidate contains a non-compacted frame")
        if not saw_base:
            raise CorruptLogError(
                "compaction candidate ends without a base commit")
        return image, end, seq, live

    def _scan_bytes(self, data: bytes):
        """Frame-scan raw bytes as if they were a log file."""
        frames = []
        terminal = None
        with io.BytesIO(data) as f:
            for event in _iter_frames(f):
                if event[0] == "frame":
                    _, meta, value, start, end = event
                    frames.append((meta, value, start, end))
                else:
                    terminal = event
        return frames, terminal

    def _install_log_bytes(self, image: bytes) -> None:
        """Atomically replace the log with ``image`` and reopen the fd.

        The complete image is written and fsynced as a temp file first, so a
        kill leaves either the old log or the complete new one -- never a
        half-written log. The append fd is closed for the rename because
        Windows cannot replace a path held open by it, then reopened.
        """
        tmp = self._path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(image)
            f.truncate(len(image))
            os.fsync(f.fileno())
        os.close(self._fd)
        try:
            os.replace(tmp, self._path)
            _fsync_dir(self._dir)
        finally:
            self._fd = os.open(
                self._path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY, 0o644)
        if os.fstat(self._fd).st_size != len(image):
            raise CorruptLogError("compaction did not converge")

    def _install_compaction(self, image: bytes, seq: int) -> None:
        """Publish a staged compacted image; idempotent and kill-safe.

        The torn-tail marker is dropped first: its byte offset belongs to
        the old (longer) log and must not outlive the shrink. Checkpoint is
        replaced next and the log last, both via complete-file atomic
        renames, then the candidate is removed. Any ordering of a kill
        between those steps leaves the durable files equal to either the old
        pair or the complete new pair, and rerunning (via the surviving
        candidate) converges to the same compacted bytes.
        """
        # Compaction closes any prior recovery epoch the way a commit does;
        # do it while the old log still exists so a later kill replays this
        # whole publish from the durable candidate.
        self._remove_marker()

        if seq == 0:
            # Empty committed state: the log is zero length and there is no
            # checkpoint to publish; reclaim every old byte and drop the plan.
            os.ftruncate(self._fd, 0)
            os.fsync(self._fd)
            try:
                os.unlink(self._ckp_path)
                _fsync_dir(self._dir)
            except FileNotFoundError:
                pass
            self._remove_candidate()
            return

        def write(fdst):
            fdst.write(image)

        self._atomic_file(self._ckp_path, write)
        self._install_log_bytes(image)
        self._remove_candidate()

    def _remove_candidate(self) -> None:
        try:
            os.unlink(self._cmp_path)
        except FileNotFoundError:
            return
        _fsync_dir(self._dir)

    def _finish_compaction(self) -> None:
        """Complete a compaction interrupted after its candidate went durable.

        With a single writer a present candidate means its compaction never
        finished (no later commit was possible), so the log and checkpoint
        cannot be ahead of it; the publish is simply repeated. A candidate
        whose boundary a newer durable epoch has overtaken is stale and is
        discarded instead.
        """
        candidate = self._read_compact_candidate()
        if candidate is None:
            return
        image, _end, cand_seq, _live = candidate

        log_seq = 0
        if os.path.exists(self._path):
            frames, terminal = self._read_log(self._path)
            _log_end, log_seq = self._validate(frames, terminal)
        ckp_seq = 0
        ckp = self._read_clean_prefix(self._ckp_path)
        if ckp is not None:
            _frames, _end, ckp_seq = ckp
        if max(log_seq, ckp_seq) > cand_seq:
            # A newer committed epoch exists; the staged image is obsolete.
            self._remove_candidate()
            return
        self._install_compaction(image, cand_seq)

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
        # A durable compaction candidate left by a killed compact() is
        # published before ordinary convergence; it is itself a complete
        # committed prefix, so this never invents or loses a committed byte.
        self._finish_compaction()
        marker = self._read_marker()
        marker_end = -1
        marker_discarded = 0
        if marker is not None:
            marker_end, marker_discarded = marker

        frames, terminal = self._read_log()
        log_end, _log_seq = self._validate(frames, terminal)
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
        self._uncommitted.clear()
        self._seq = self._last_seq(frames)
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

    def _committed_view(self) -> dict[str, bytes]:
        """The writer's committed state, with uncommitted edits folded out.

        Costs nothing when there are no pending changes; otherwise copies
        the live state and restores each pending key's committed value.
        """
        if not self._uncommitted:
            return self._data
        view = dict(self._data)
        for key, old in self._uncommitted.items():
            if old is None:
                view.pop(key, None)
            else:
                view[key] = old
        return view

    # -- argument validation ----------------------------------------------

    @staticmethod
    def _check_key(key) -> None:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must not be empty")

    @staticmethod
    def _scan_endpoint(value, name: str) -> bytes | None:
        """Validate a scan endpoint and return its raw ordering bytes."""
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"scan {name} endpoint must be a string or None")
        return value.encode("utf-8")

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
        if key not in self._uncommitted:
            self._uncommitted[key] = self._data.get(key)
        self._data[key] = value
        self._pending += 1
        self._entries += 1

    def get(self, key: str) -> bytes | None:
        self._ensure_open()
        self._check_key(key)
        if self._read_only:
            return self._read_key(key)
        return self._data.get(key)

    def scan(self, start: str | None = None,
             end: str | None = None) -> ScanCursor:
        """Iterate the pinned committed snapshot in raw key-byte order.

        Returns a :class:`ScanCursor` yielding ``(key, value)`` pairs for
        every committed key in the half-open range ``[start, end)``: the
        start key is included, the end key excluded, and a ``None``
        endpoint leaves that side unbounded. Keys are ordered by their raw
        bytes; keys and values come back exactly as stored, with no
        encoding conversion. Keys that only ever appeared in delete
        history never show up, a key overwritten any number of times
        yields only its last committed value, and empty values are
        returned like any other.

        The cursor materialises the matching live keys once, at open, and
        is then independent of the store: commits, compaction or crash
        recovery that happen while it is being read never change what it
        yields, and it keeps reading after the log space its snapshot came
        from has been reclaimed. Its cost tracks the number of live keys,
        never the length of the log history, and it never takes a lock or
        writes a file -- a read-only store scans its pinned snapshot
        without touching anything on disk.

        A ``start`` that sorts after ``end`` raises ``ValueError``, as
        does reading from a closed cursor. Non-string endpoints raise
        ``TypeError``.
        """
        self._ensure_open()
        start_b = self._scan_endpoint(start, "start")
        end_b = self._scan_endpoint(end, "end")
        if start_b is not None and end_b is not None and start_b > end_b:
            raise ValueError(
                f"scan start {start!r} sorts after scan end {end!r}")

        source = self._ro_index if self._read_only else self._committed_view()
        matched = []
        for key, value in source.items():
            key_b = key.encode("utf-8")
            if start_b is not None and key_b < start_b:
                continue
            if end_b is not None and key_b >= end_b:
                continue
            matched.append((key_b, key, value))
        matched.sort(key=lambda item: item[0])
        return ScanCursor([(key, value) for _kb, key, value in matched])

    def delete(self, key: str) -> None:
        self._ensure_writable()
        self._check_key(key)
        _write_all(self._fd, _encode_frame(_OP_DELETE, key=key))
        if key not in self._uncommitted:
            self._uncommitted[key] = self._data.get(key)
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
        self._uncommitted.clear()
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
        seq = self._last_seq(frames)
        self._data = state
        self._seq = seq
        self._entries = len(frames)
        self._pending = 0
        self._uncommitted.clear()
        self._discarded = discarded
        self._corrupt = False
        return {"applied": applied, "discarded": discarded, "seq": seq}

    def compact(self) -> dict:
        """Rewrite committed history into one tight, kill-safe log image.

        The log becomes exactly the currently committed state: one put frame
        per live key (sorted by key) followed by a base commit marker that
        carries the pre-compaction sequence number. Deleted keys are gone for
        good, the durable sequence keeps its value (the next commit is
        ``seq + 1``) and the old log/checkpoint space is reclaimed. The
        empty committed state compacts to an empty log.

        Returns the same three-field report shape as :meth:`recover` for the
        compacted log: ``applied`` is the live puts it now contains,
        ``discarded`` is always 0 and ``seq`` is the preserved sequence.

        Uncommitted session changes are rejected (``ValueError``). The image
        is staged as a complete sidecar and published by two atomic
        renames, so a kill at any byte/step -- including a kill repeated any
        number of times -- leaves a state the next open converges by simply
        finishing the same publish, byte-identical to one clean compaction.
        Live read-only stores keep pinning their old complete snapshots
        throughout; compaction never waits on or is disturbed by them.
        """
        self._ensure_writable()
        if self._pending:
            raise ValueError(
                "cannot compact with uncommitted changes in this session")

        # Settle any crash remnant (including an interrupted compaction)
        # before snapshotting the committed state.
        frames, discarded = self._converge()
        state, _applied = self._replay(frames)
        seq = self._last_seq(frames)
        self._data = state
        self._seq = seq
        self._entries = len(frames)
        self._pending = 0
        self._uncommitted.clear()
        self._discarded = discarded
        self._corrupt = False

        image, live = self._build_compact_image(seq)
        self._write_compact_candidate(image)
        self._install_compaction(image, seq)

        self._entries = 0 if seq == 0 else live + 1
        self._pending = 0
        return {"applied": live, "discarded": 0, "seq": seq}

    def _write_compact_candidate(self, image: bytes) -> None:
        """Durably stage the compacted image as the ``wal.cmp`` sidecar."""

        def write(f):
            f.write(image)

        self._atomic_file(self._cmp_path, write)

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
