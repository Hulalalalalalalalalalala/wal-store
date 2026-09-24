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
    the durable sequence number never regresses.

``wal.rec``
    A recovery marker written only after a log needing repair has passed
    full validation and only *before* the atomic rebuild starts. It records
    the clean prefix length and whether a torn record was discarded. Its
    presence makes a rebuild interrupted mid-copy converge on reopen (the
    rebuild is simply repeated) and keeps the reported ``discarded`` count
    stable across reopens and repeated recoveries. It is removed by the next
    successful commit.

``wal.idx``
    An immutable manifest of the committed prefix at the most recent commit.
    It is published below the commit path, atomically and only after the
    commit frame and the checkpoint are durable, so every file it names is a
    complete committed snapshot. Read-only processes open it instead of
    replaying the log: key lookups are one bounded indexed read, independent
    of log history, and a reader never opens the log for writing or modifies
    any file. Replacing the log atomically (recovery) never invalidates a
    published manifest, because the identical prefix also lives in the
    checkpoint; readers verify every frame they actually read. A store from
    before this file existed opens read-only unchanged -- the committed
    prefix is replayed once on open.

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
"""

from __future__ import annotations

import json
import os
import struct
import sys
import zlib

__all__ = ["Store", "CorruptLogError", "inject_tear", "open_readonly"]


def open_readonly(path: str) -> "Store":
    """Open the store at ``path`` as an isolated read-only process.

    Any number of readers may coexist with the single writer. The call
    never creates or modifies a file and never blocks the writer; reads
    observe complete committed snapshots.
    """
    return Store(path, read_only=True)


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
_INDEX_NAME = "wal.idx"

_MAGIC = b"WAL2"
_PREFIX = 12  # 4 bytes magic + 8 bytes payload length
_HEADER = 16   # prefix + 4 bytes crc32 of the prefix
_TRAILER = 4   # crc32 of the payload
_MAX_PAYLOAD = 1 << 40

# Snapshot manifest format: a fixed header, one fixed record per live key,
# the concatenated utf-8 key bytes, and a trailing crc32 of all the above.
_INDEX_MAGIC = b"WIDX1"
_INDEX_HEADER = 32         # >5sBBx I Q Q I : magic,ver,reserved,count,seq,end,entries
_INDEX_ENTRY = 24          # >I I Q Q : key length, flags, frame offset, frame end
_INDEX_TRAILER = 4         # crc32 of header and entries
_INDEX_VERSION = 1

_OP_PUT = "p"
_OP_DELETE = "d"
_OP_COMMIT = "c"


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


# -- read-only snapshot manifest (wal.idx) -------------------------------
#
# The manifest is one self-checksumming binary blob, replaced atomically
# only after the checkpoint covering the same prefix is durable::
#
#     header  32 bytes  magic/version, key count, seq, prefix end, entries
#     entry   24 bytes  key length, flags, frame offset, frame end (per key)
#     keys    utf-8 key bytes concatenated in entry order
#     trailer 4 bytes   crc32 of everything above
#
# It names only byte ranges inside the immutable committed prefix, which
# both wal.log and wal.ckp contain byte for byte, so a reader never depends
# on which file is the current log and never scans frames.

_INDEX_HEADER_FMT = ">5sBBxIQQI"
_INDEX_ENTRY_FMT = ">IIQQ"


def _encode_index(seq: int, end: int, entries: int, records) -> bytes:
    """Build a snapshot manifest from ``(key, offset, frame_end)`` records."""
    ordered = sorted(records)
    parts = [struct.pack(_INDEX_HEADER_FMT, _INDEX_MAGIC, _INDEX_VERSION, 0,
                         len(ordered), seq, end, entries)]
    keyblob = bytearray()
    for key, off, frame_end in ordered:
        kb = key.encode("utf-8")
        parts.append(struct.pack(_INDEX_ENTRY_FMT, len(kb), 0, off,
                                 frame_end))
        keyblob.extend(kb)
    body = b"".join(parts) + bytes(keyblob)
    return body + zlib.crc32(body).to_bytes(4, "big")


def _parse_index(raw: bytes) -> dict:
    """Validate a manifest blob and return its snapshot description."""
    if len(raw) < _INDEX_HEADER + _INDEX_TRAILER:
        raise CorruptLogError("snapshot manifest is truncated")
    if int.from_bytes(raw[-4:], "big") != zlib.crc32(raw[:-4]):
        raise CorruptLogError("snapshot manifest checksum mismatch")
    try:
        magic, version, _flags, count, seq, end, entries = struct.unpack(
            _INDEX_HEADER_FMT, raw[:_INDEX_HEADER])
    except struct.error:
        raise CorruptLogError("snapshot manifest header is invalid")
    if magic != _INDEX_MAGIC or version != _INDEX_VERSION:
        raise CorruptLogError("snapshot manifest has an unknown format")
    if seq < 0 or end < _HEADER + _TRAILER or entries < 0 or count > entries:
        raise CorruptLogError("snapshot manifest has invalid bounds")
    pos = _INDEX_HEADER + _INDEX_ENTRY * count
    keys = {}
    try:
        for i in range(count):
            klen, _flags2, off, frame_end = struct.unpack_from(
                _INDEX_ENTRY_FMT, raw,
                _INDEX_HEADER + _INDEX_ENTRY * i)
            key = raw[pos:pos + klen].decode("utf-8")
            pos += klen
            if (not key or frame_end <= off or frame_end > end
                    or klen == 0):
                raise CorruptLogError(
                    "snapshot manifest has invalid frame bounds")
            keys[key] = (off, frame_end)
    except (struct.error, UnicodeDecodeError):
        raise CorruptLogError("snapshot manifest entries are invalid")
    if pos + _INDEX_TRAILER != len(raw):
        raise CorruptLogError("snapshot manifest length is inconsistent")
    return {"seq": seq, "end": end, "entries": entries, "keys": keys}


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

    With ``read_only=True`` the store opens as an isolated reader: no file
    is ever created, opened for writing or unlinked, so any number of
    readers share a directory with one writer without blocking it. Every
    read observes a complete committed snapshot taken at open time or later;
    uncommitted bytes, torn records and half-finished shrinkage are never
    visible, and killing or hanging a reader never affects the writer.
    """

    def __init__(self, path: str, read_only: bool = False):
        self._dir = path
        self._path = os.path.join(path, LOG_NAME)
        self._ckp_path = os.path.join(path, _CHECKPOINT_NAME)
        self._marker_path = os.path.join(path, _MARKER_NAME)
        self._index_path = os.path.join(path, _INDEX_NAME)

        # Recovery needs a directory to replay; a missing target is an error
        # at open time, not an implicit create. Readers fail the same way.
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"store directory does not exist: {path!r}")
        if not os.path.isdir(path):
            raise OSError(f"store path is not a directory: {path!r}")

        self._closed = False
        if read_only:
            self._read_only = True
            self._fd = None
            self._open_read_only()
            return

        self._read_only = False
        log_created = not os.path.exists(self._path)
        self._fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                           0o644)
        if log_created:
            os.fsync(self._fd)
            _fsync_dir(path)

        self._data: dict[str, bytes] = {}
        self._locate: dict[str, tuple[int, int]] = {}
        self._seq = 0
        self._entries = 0
        self._pending = 0
        self._corrupt = False
        # Torn records dropped in this recovery epoch; wal.rec carries it
        # across processes until the next commit.
        self._discarded = 0

        self._converge_on_open()

    @classmethod
    def open_readonly(cls, path: str) -> "Store":
        """Open ``path`` as an isolated reader; see ``read_only``."""
        return cls(path, read_only=True)

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

    def _remove_stale_temps(self) -> None:
        for name in (self._path + ".tmp", self._ckp_path + ".tmp",
                     self._marker_path + ".tmp", self._index_path + ".tmp"):
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
        self._locate = self._locate_frames(frames)
        self._seq = max(
            (meta["s"] for meta, _v, _s, _e in frames
             if meta.get("t") == _OP_COMMIT),
            default=0)
        self._entries = len(frames)
        self._pending = 0
        self._discarded = discarded
        self._corrupt = False

    @staticmethod
    def _locate_frames(frames) -> dict[str, tuple[int, int]]:
        """Map each live key to the byte span of its last put frame."""
        locate: dict[str, tuple[int, int]] = {}
        for meta, _value, start, end in frames:
            op = meta.get("t")
            if op == _OP_PUT:
                locate[meta["k"]] = (start, end)
            elif op == _OP_DELETE:
                locate.pop(meta["k"], None)
        return locate

    def _write_index(self, seq: int, end: int, entries: int) -> None:
        """Publish the immutable read-only snapshot manifest.

        Runs strictly below the commit path: only after the commit frame and
        the checkpoint covering ``[0, end)`` are durable. The replace is
        atomic, so a reader always sees either the previous or this complete
        manifest; a kill leaves at most a ``.tmp`` file, cleaned on reopen.
        """
        if end <= 0:
            return
        records = ((key, span[0], span[1])
                   for key, span in self._locate.items())
        blob = _encode_index(seq, end, entries, records)

        def write(f):
            f.write(blob)

        self._atomic_file(self._index_path, write)

    def _converge_on_open(self) -> None:
        """Crash cleanup at open; leave a corrupt store inert for recover()."""
        try:
            frames, discarded = self._converge()
        except CorruptLogError:
            self._corrupt = True
            return
        self._publish(frames, discarded)
        # A writer killed between the durable commit frame and the manifest
        # publish leaves a committed prefix with no (or a stale) wal.idx.
        # Republish below the commit path -- convergence has made sure the
        # checkpoint covering this prefix is durable -- so readers can never
        # lag behind a commit that survived a kill.
        end = frames[-1][3] if frames else 0
        if end > 0 and not self._index_matches(self._seq, end, len(frames)):
            self._write_index(self._seq, end, len(frames))

    # -- isolated read-only side ------------------------------------------
    #
    # A reader never holds a writable descriptor (on Windows even a read
    # handle kept open could block the writer's os.replace, so every read
    # opens a fresh short-lived O_RDONLY handle and closes it immediately),
    # never creates or unlinks anything, and takes no lock. Its snapshot is
    # described by wal.idx, which the writer publishes only when the whole
    # committed prefix it names is durable; reads therefore land on a
    # complete committed snapshot even while the writer appends or rebuilds.

    def _open_read_only(self) -> None:
        """Take the opening snapshot without writing a single byte."""
        self._ro_end = 0
        self._ro_seq = 0
        self._ro_entries = 0
        self._ro_keys: dict[str, tuple[int, int]] = {}
        self._ro_fallback: dict[str, bytes] | None = None
        self._ro_index_sig: tuple | None = None
        self._refresh_snapshot(initial=True)
        if self._ro_fallback is None and self._ro_end == 0:
            # No manifest and no log yet (an empty directory, or a missing
            # log): the opening snapshot is simply empty.
            self._ro_fallback = {}

    def _choose_source(self, end: int) -> str | None:
        """Pick a file that actually contains the full ``[0, end)`` prefix.

        Prefer the immutable checkpoint: it is replaced atomically and never
        written in place, so opening it cannot observe a half-written file.
        The log is used only when it (still) contains the whole prefix; a
        reader whose snapshot named bytes later rebuilt away keeps reading
        the checkpoint instead.
        """
        try:
            if os.path.getsize(self._ckp_path) >= end:
                return self._ckp_path
        except OSError:
            pass
        try:
            if os.path.getsize(self._path) >= end:
                return self._path
        except OSError:
            pass
        return None

    def _refresh_snapshot(self, initial: bool = False) -> bool:
        """Advance to a newer committed snapshot if one was published.

        Snapshots only move forward. Returns whether a newer snapshot was
        adopted. The manifest is stat-cache checked first, so a read with no
        new commit costs one ``stat`` -- it never rescans the log and its
        cost is independent of history. The writer replaces the manifest
        atomically, so a damaged manifest is never a transient of its own
        operation: at open it is reported as corruption, while later on the
        reader keeps serving its last good snapshot until a valid manifest
        replaces the bad one.
        """
        try:
            st = os.stat(self._index_path)
        except FileNotFoundError:
            if initial:
                # Pre-index store (or an empty directory): replay the
                # committed prefix once; no file is created or converted.
                self._load_legacy_snapshot()
            return False
        except OSError:
            return False
        signature = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        if signature == self._ro_index_sig and not initial:
            return False
        self._ro_index_sig = signature
        try:
            with open(self._index_path, "rb") as f:
                manifest = _parse_index(f.read())
        except CorruptLogError:
            if initial:
                raise
            return False
        except OSError:
            return False
        if manifest["seq"] <= self._ro_seq and not initial:
            return False
        source = self._choose_source(manifest["end"])
        if source is None:
            return False
        # The manifest is self-checksumming and atomically replaced, so a
        # published manifest is complete and trustworthy; the frame ranges
        # it names are CRC- and key-verified lazily, on the read that uses
        # them, so adopting a snapshot costs no scan of history.
        self._ro_end = manifest["end"]
        self._ro_seq = manifest["seq"]
        self._ro_entries = manifest["entries"]
        self._ro_keys = manifest["keys"]
        self._ro_fallback = None
        return True

    def _load_legacy_snapshot(self) -> None:
        """Open a pre-index store: replay the committed prefix once.

        Old logs are read directly, no conversion or sidecar is written, so
        the read-only boundary holds. The checkpoint supplies the prefix
        when the log is torn inside it, carries a dirty tail or is gone;
        afterwards individual reads serve from the materialized state and
        never touch history.
        """
        corrupt = False
        try:
            frames, terminal = self._read_log(self._path)
        except FileNotFoundError:
            frames = None
        if frames is not None:
            try:
                end, seq = self._validate(frames, terminal)
            except CorruptLogError:
                corrupt = True
                end, seq = -1, 0
        else:
            end, seq = -1, 0
        try:
            ckp_end = self._checkpoint_boundary()
        except CorruptLogError:
            ckp_end = 0
        if ckp_end > max(end, 0):
            frames, terminal = self._read_log(self._ckp_path)
            end, seq = self._validate(frames, terminal)
            if terminal is not None:
                raise CorruptLogError(
                    "checkpoint is not a clean committed prefix")
        elif corrupt:
            raise CorruptLogError("corrupt log frame")
        elif frames is None:
            # No log and no checkpoint: a directory opened before the
            # writer created anything.
            return
        committed = [(m, v) for m, v, _s, e in frames if e <= end]
        state, _applied = self._replay(
            [(m, v, 0, 0) for m, v in committed])
        self._ro_fallback = state
        self._ro_seq = seq
        self._ro_entries = len(committed)
        self._ro_end = max(end, 0)

    def _read_frame_payload(self, off: int, frame_end: int) -> bytes:
        """Read and verify one frame's payload from the snapshot source.

        Tries every file that currently contains the parked prefix (the
        immutable checkpoint first, then the live log); only a failure on
        all of them is corruption. Recovery never leaves a half-written
        either file (both are replaced atomically), so one intact source is
        always enough, and the reader never waits on the writer.
        """
        candidates = []
        for candidate in (self._ckp_path, self._path):
            try:
                if os.path.getsize(candidate) >= self._ro_end:
                    candidates.append(candidate)
            except OSError:
                pass
        last_error: Exception | None = None
        for source in candidates:
            try:
                return self._read_verified_frame(source, off, frame_end)
            except (CorruptLogError, OSError) as exc:
                last_error = exc
                continue
        raise CorruptLogError(
            f"snapshot frame is unreadable: {last_error}")

    @staticmethod
    def _read_verified_frame(source: str, off: int, frame_end: int) -> bytes:
        with open(source, "rb") as f:
            f.seek(off)
            raw = f.read(frame_end - off)
        if len(raw) < _HEADER + _TRAILER or raw[:4] != _MAGIC:
            raise CorruptLogError("snapshot frame is unreadable")
        length = int.from_bytes(raw[4:_PREFIX], "big")
        if len(raw) != _HEADER + length + _TRAILER:
            raise CorruptLogError("snapshot frame is torn")
        if int.from_bytes(raw[_PREFIX:_HEADER], "big") != \
                zlib.crc32(raw[:_PREFIX]):
            raise CorruptLogError("snapshot frame header is corrupt")
        payload = raw[_HEADER:_HEADER + length]
        if int.from_bytes(raw[_HEADER + length:], "big") != \
                zlib.crc32(payload):
            raise CorruptLogError("snapshot frame is corrupt")
        return payload

    def _reader_get(self, key: str) -> bytes | None:
        # A later commit may have been published; advance but never regress.
        self._refresh_snapshot()
        if self._ro_fallback is not None:
            return self._ro_fallback.get(key)
        span = self._ro_keys.get(key)
        if span is None:
            return None
        payload = self._read_frame_payload(*span)
        newline = payload.find(b"\n")
        if newline < 0:
            raise CorruptLogError("snapshot frame has no metadata")
        try:
            meta = json.loads(payload[:newline])
        except (ValueError, UnicodeDecodeError):
            raise CorruptLogError("snapshot frame metadata is corrupt")
        # The manifest cannot be tricked into naming another key's frame:
        # the bytes it points at must be this key's put after all.
        if not isinstance(meta, dict) or meta.get("t") != _OP_PUT \
                or meta.get("k") != key:
            raise CorruptLogError("snapshot manifest does not match frame")
        return payload[newline + 1:]

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
        start = os.fstat(self._fd).st_size
        _write_all(self._fd, _encode_frame(_OP_PUT, value=value, key=key))
        end = os.fstat(self._fd).st_size
        self._data[key] = value
        self._locate[key] = (start, end)
        self._pending += 1
        self._entries += 1

    def get(self, key: str) -> bytes | None:
        self._ensure_open()
        self._check_key(key)
        if self._read_only:
            return self._reader_get(key)
        return self._data.get(key)

    def delete(self, key: str) -> None:
        self._ensure_writable()
        self._check_key(key)
        _write_all(self._fd, _encode_frame(_OP_DELETE, key=key))
        self._data.pop(key, None)
        self._locate.pop(key, None)
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
        # new restorable prefix, expose the matching read-only snapshot, then
        # close the recovery epoch.
        clean_end = os.fstat(self._fd).st_size
        self._write_checkpoint(clean_end)
        had_marker = os.path.exists(self._marker_path)
        self._seq += 1
        self._pending = 0
        self._entries += 1
        self._discarded = 0
        self._write_index(self._seq, clean_end, self._entries)
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
        self._locate = self._locate_frames(frames)
        self._seq = seq
        self._entries = len(frames)
        self._pending = 0
        self._discarded = discarded
        self._corrupt = False
        end = frames[-1][3] if frames else 0
        if end > 0 and not self._index_matches(seq, end, len(frames)):
            self._write_index(seq, end, len(frames))
        return {"applied": applied, "discarded": discarded, "seq": seq}

    def _index_matches(self, seq: int, end: int, entries: int) -> bool:
        """Whether a published manifest already describes this prefix."""
        try:
            with open(self._index_path, "rb") as f:
                manifest = _parse_index(f.read())
        except (FileNotFoundError, CorruptLogError, OSError):
            return False
        return (manifest["seq"] == seq and manifest["end"] == end
                and manifest["entries"] == entries)

    def stats(self) -> dict:
        self._ensure_open()
        if self._read_only:
            # Report the snapshot the reader is parked on; a later commit
            # may advance it the same way get() does. Readers never stat the
            # live log through a writable descriptor.
            self._refresh_snapshot()
            return {
                "seq": self._ro_seq,
                "entries": self._ro_entries,
                "bytes": self._ro_end,
            }
        return {
            "seq": self._seq,
            "entries": self._entries,
            "bytes": os.fstat(self._fd).st_size,
        }

    def close(self) -> None:
        if self._closed:
            return
        if self._fd is not None:
            os.close(self._fd)
        self._closed = True

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
