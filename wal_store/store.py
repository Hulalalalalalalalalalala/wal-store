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

Recovery itself is kill-safe. Converging the log to its committed prefix is
not a single truncate anymore (a kill in the middle of one left a log shrunk
to an arbitrary byte offset, which the next open could misread as a fresh
torn tail). Two sidecar files in the store directory make every step
idempotent; both live strictly below the write path:

``wal.ckp``
    Byte-for-byte copy of the log prefix ending at the most recent durable
    commit marker, published atomically (temp file, fsync, ``os.replace``,
    directory fsync). It is the independent source a log torn at *any*
    offset -- including offset 0, or inside the committed prefix -- is
    rebuilt from, so the durable sequence number can never regress.

``wal.rec``
    Recovery marker, written only after a log that needs repair has passed
    full validation and only *before* the rebuild starts. It pins the clean
    prefix length and whether a torn final record was discarded. A rebuild
    killed at any primitive is simply repeated on the next open, and the
    report (notably ``discarded``) stays frozen until the next successful
    commit closes the epoch and removes the marker.

The durable commit boundary is the maximum of the validated log boundary,
the checkpoint boundary and the marker boundary, so it never moves
backwards. Each repair step is either an atomic rename or safe to repeat,
which is why interrupting recovery any number of times converges to exactly
what one clean recovery would have produced.

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

_MAGIC = b"WAL2"
_PREFIX = 12  # 4 bytes magic + 8 bytes payload length
_HEADER = 16   # prefix + 4 bytes crc32 of the prefix
_TRAILER = 4   # crc32 of the payload
_MAX_PAYLOAD = 1 << 40

_COPY_CHUNK = 1 << 20

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
    """Fsync a directory so a neighbouring rename/unlink is durable.

    Windows refuses to open a directory for fsync with ``PermissionError``
    (access denied); there a rename is made durable by the file's own flush,
    so skip the directory fsync entirely. Some non-Windows mounts reject it
    the same way, and durability of the directory entry is best-effort
    there, so absorb that ``PermissionError`` too.
    """
    if sys.platform == "win32":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            os.fsync(fd)
        except PermissionError:
            pass
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _copy_prefix(fsrc, fdst, length: int) -> None:
    """Copy exactly ``length`` bytes between open files."""
    remaining = length
    while remaining:
        chunk = fsrc.read(min(remaining, _COPY_CHUNK))
        if not chunk:
            raise CorruptLogError(
                "source ended before the committed prefix")
        fdst.write(chunk)
        remaining -= len(chunk)
    fdst.truncate(length)


def inject_tear(source, destination, offset):
    """Produce a log copy cut at exactly ``offset`` bytes.

    Verification aid for bytewise crash checks: the destination holds the
    first ``offset`` bytes of the source log -- the image of a writer killed
    precisely when its append had reached that byte position. The source is
    opened read-only and never modified. Either path argument may be a store
    directory, in which case its ``wal.log`` is used.

    ``offset`` must be an integer with ``0 <= offset <= len(source)``; a
    negative or past-the-end offset raises ``IndexError`` and a non-integer
    offset raises ``TypeError``. ``offset == len(source)`` is the full-length
    clean control copy. The destination must not resolve to the source log.
    Returns the destination log path.

    This is never used by the normal write path.
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
            chunk = fsrc.read(min(remaining, _COPY_CHUNK))
            if not chunk:  # size came from this same file; it cannot shrink
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
    """

    def __init__(self, path: str):
        self._dir = path
        self._path = os.path.join(path, LOG_NAME)
        self._ckp_path = os.path.join(path, _CHECKPOINT_NAME)
        self._marker_path = os.path.join(path, _MARKER_NAME)

        # Recovery needs a directory to replay; a missing target is an error
        # at open time, not an implicit create.
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"store directory does not exist: {path!r}")
        if not os.path.isdir(path):
            raise OSError(f"store path is not a directory: {path!r}")

        log_created = not os.path.exists(self._path)
        self._fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                           0o644)
        if log_created:
            os.fsync(self._fd)
            _fsync_dir(path)

        self._data: dict[str, bytes] = {}
        self._seq = 0
        self._entries = 0
        self._pending = 0
        self._closed = False
        self._corrupt = False
        # Torn records discarded in this recovery epoch; wal.rec carries the
        # decision across processes until the next commit.
        self._discarded = 0

        self._converge_on_open()

    # -- log scanning and validation --------------------------------------

    def _read_log(self, path: str | None = None):
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

        Returns ``(committed_end, last_seq)``: the byte offset just past the
        last commit marker and its sequence number. An ``"invalid"`` terminal
        is corruption and raises ``CorruptLogError``; a ``"torn"`` terminal
        is accepted here and left to the caller, which decides from the
        durable sidecars whether it is a tail to drop or committed bytes to
        restore.
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

    def _atomic_publish(self, target: str, write) -> None:
        """Replace ``target`` atomically: temp file, fsync, rename, dir fsync."""
        tmp = target + ".tmp"
        with open(tmp, "wb") as f:
            write(f)
            os.fsync(f.fileno())
        os.replace(tmp, target)
        _fsync_dir(self._dir)

    def _write_checkpoint(self, end: int) -> None:
        """Persist the log prefix ``[0, end)`` as the durable checkpoint."""
        if end <= 0:
            return

        def write(fdst):
            with open(self._path, "rb") as fsrc:
                _copy_prefix(fsrc, fdst, end)

        self._atomic_publish(self._ckp_path, write)

    def _checkpoint_boundary(self) -> int:
        """Commit boundary covered by the checkpoint; 0 when it is absent.

        A checkpoint that is torn, unparseable, or not exactly one committed
        prefix is corruption of the recovery machinery, not a discardable
        tail.
        """
        try:
            frames, terminal = self._read_log(self._ckp_path)
        except FileNotFoundError:
            return 0
        if not frames:
            if terminal is None and os.path.getsize(self._ckp_path) == 0:
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

        self._atomic_publish(self._marker_path, write)

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
        # Temp files are uniquely named and always truncated/renamed within
        # one convergence pass; anything present is a remnant of a killed
        # pass and safe to unlink before we start.
        for name in (self._path + ".tmp", self._ckp_path + ".tmp",
                     self._marker_path + ".tmp"):
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass

    def _rebuild_log(self, clean_end: int) -> None:
        """Make the log exactly its clean ``[0, clean_end)`` prefix.

        Kill-safe and idempotent. When the checkpoint covers the prefix, the
        new log is fully written and fsynced under a temp name and atomically
        renamed over the old one, so a kill leaves either the old log or the
        complete restored log -- never a file half-written at an arbitrary
        offset -- and rerunning simply repeats the work. Without a checkpoint
        but with the committed bytes still at stable offsets, a synced
        truncate drops the dirty suffix. The empty prefix is a truncate to
        zero.
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

        if ckp_size < clean_end:
            if size <= clean_end:
                # Committed bytes were torn away and nothing durable covers
                # them; the boundary cannot be reconstructed.
                raise CorruptLogError(
                    "committed prefix is torn and no checkpoint covers it")
            # Legacy layout without a checkpoint: the committed bytes still
            # sit at their stable offsets, so a truncate is enough and stays
            # idempotent if killed mid-call.
            try:
                os.fsync(self._fd)
            except OSError:
                pass
            os.ftruncate(self._fd, clean_end)
            os.fsync(self._fd)
            return

        tmp = self._path + ".tmp"
        with open(self._ckp_path, "rb") as fsrc, open(tmp, "wb") as fdst:
            _copy_prefix(fsrc, fdst, clean_end)
            os.fsync(fdst.fileno())
        # Windows cannot rename onto a path held open by the append fd; close
        # it across the rename and reopen the (now clean) log afterwards.
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
        """Validate the log and converge it to the clean committed state.

        Returns ``(frames, discarded)`` for the now-clean log. Raises
        ``CorruptLogError`` without changing anything if the log is corrupt.
        Safe to run repeatedly and safe to kill at any primitive.

        The durable commit boundary is the maximum of the validated log
        boundary, the checkpoint boundary and the marker boundary, so it can
        never regress. Committed bytes torn away (offset 0 included) are
        rebuilt from the checkpoint and do not count as discarded; a torn or
        merely uncommitted suffix past the boundary is dropped, and the
        marker freezes that report until the next commit even when the kill
        lands in the middle of the rebuild.
        """
        self._remove_stale_temps()
        marker = self._read_marker()
        marker_end = -1
        marker_discarded = 0
        if marker is not None:
            marker_end, marker_discarded = marker

        # Validate everything on disk before publishing a plan or touching a
        # byte; genuine corruption raises here and leaves the store as found.
        frames, terminal = self._read_log()
        log_end, _log_seq = self._validate(frames, terminal)
        ckp_end = self._checkpoint_boundary()
        size = os.fstat(self._fd).st_size

        target = max(log_end, ckp_end, marker_end)
        # The target prefix must be reconstructable from a durable source.
        if target > log_end and target > ckp_end:
            raise CorruptLogError("recovery target has no durable source")

        log_clean = terminal is None and size == log_end

        if log_clean and log_end == target:
            # Nothing to repair. A marker whose boundary a newer commit has
            # since passed is stale and goes; otherwise its discarded count
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
            # Committed bytes were torn away: the checkpoint is authoritative.
            discarded = marker_discarded if target == marker_end else 0
            self._rebuild_log(target)
        else:
            # Dirty suffix past the boundary. Decide discarded from the
            # suffix itself: a torn final record counts, complete uncommitted
            # frames do not. Recomputing it here also finishes a rebuild that
            # was killed before it ran (the original suffix is still present)
            # and treats a fresh crash after a prior repair as a new decision
            # rather than replaying the old marker's. Publish the checkpoint
            # first so the prefix is independently restorable, then freeze
            # the plan in the marker before any byte of the log is removed.
            discarded = 1 if terminal is not None else 0
            if ckp_end < target:
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
        """Crash cleanup at open; a corrupt store stays inert for recover()."""
        try:
            frames, discarded = self._converge()
        except CorruptLogError:
            # Keep the corrupt log untouched; recover() surfaces the error.
            self._corrupt = True
            return
        self._publish(frames, discarded)

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
        # clean log to refresh the checkpoint from. Publish the new
        # restorable prefix, then close the open recovery epoch.
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

        Recovery is idempotent and kill-safe: no matter how often it is
        interrupted and rerun it converges to the same state, report and
        sequence as one uninterrupted recovery, and the next commit always
        uses ``seq + 1``.
        """
        self._ensure_open()
        if self._pending:
            raise ValueError(
                "cannot recover with uncommitted changes in this session")

        # Re-validate and re-converge; a genuinely corrupt log raises before
        # anything is applied or removed.
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

    def stats(self) -> dict:
        self._ensure_open()
        return {
            "seq": self._seq,
            "entries": self._entries,
            "bytes": os.fstat(self._fd).st_size,
        }

    def close(self) -> None:
        if self._closed:
            return
        os.close(self._fd)
        self._closed = True

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
