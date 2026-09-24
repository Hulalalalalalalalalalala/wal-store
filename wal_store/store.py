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
import zlib

__all__ = ["Store", "CorruptLogError"]


class CorruptLogError(ValueError):
    """The write-ahead log is corrupt, not merely missing a torn tail.

    A single incomplete final record is a normal crash remnant and is
    discarded during recovery. Anything else that fails framing, checksum,
    metadata or commit-sequence validation is corruption; recovery stops
    without applying or removing anything.
    """


LOG_NAME = "wal.log"

_MAGIC = b"WAL2"
_PREFIX = 12  # 4 bytes magic + 8 bytes payload length
_HEADER = 16   # prefix + 4 bytes crc32 of the prefix
_TRAILER = 4   # crc32 of the payload
_MAX_PAYLOAD = 1 << 40

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
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


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
        # Torn records dropped while replaying at open time; recover() still
        # reports them even though the bytes are already gone.
        self._discarded = 0

        self._load_on_open()

    # -- log scanning and validation --------------------------------------

    def _read_log(self):
        frames = []
        terminal = None
        with open(self._path, "rb") as f:
            for event in _iter_frames(f):
                if event[0] == "frame":
                    _, meta, value, start, end = event
                    frames.append((meta, value, start, end))
                else:
                    terminal = event
        return frames, terminal

    @staticmethod
    def _validate(frames, terminal):
        """Validate every frame and commit ordering.

        Returns ``(committed_end, last_seq, discarded)`` -- the byte offset
        just past the last commit marker, its sequence number, and the number
        of torn tail records dropped (zero or one). Raises
        ``CorruptLogError`` on unknown frames, bad metadata, or any
        gap/duplicate in the strictly increasing commit sequence.
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
        if terminal is not None:
            if terminal[0] == "invalid":
                raise CorruptLogError("corrupt log frame")
            # A torn final record is the only remnant a kill can leave.
            return committed_end, last_seq, 1
        return committed_end, last_seq, 0

    @staticmethod
    def _replay(frames, committed_end) -> tuple[dict[str, bytes], int]:
        state: dict[str, bytes] = {}
        applied = 0
        for meta, value, _start, end in frames:
            if end > committed_end:
                break
            op = meta["t"]
            if op == _OP_COMMIT:
                continue
            if op == _OP_PUT:
                state[meta["k"]] = value
            else:  # _OP_DELETE
                state.pop(meta["k"], None)
            applied += 1
        return state, applied

    def _load_on_open(self) -> None:
        """Crash cleanup: replay the committed prefix, drop the dirty tail.

        When the log proves valid, everything past the last commit marker
        (plus a possible torn final record) is truncated away. If the log is
        corrupt nothing is touched and the store stays inert until a
        ``recover`` call raises the underlying ``CorruptLogError``.
        """
        frames, terminal = self._read_log()
        try:
            committed_end, last_seq, discarded = \
                self._validate(frames, terminal)
        except CorruptLogError:
            # Keep the corrupt log untouched; recover() will raise it.
            self._corrupt = True
            return
        self._discarded = discarded

        state, _applied = self._replay(frames, committed_end)

        size = os.fstat(self._fd).st_size
        if size > committed_end:
            os.ftruncate(self._fd, committed_end)
            os.fsync(self._fd)

        self._data = state
        self._seq = last_seq
        self._entries = sum(1 for _m, _v, _s, end in frames
                            if end <= committed_end)

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
        self._seq += 1
        self._pending = 0
        self._discarded = 0
        self._entries += 1
        return self._seq

    def recover(self) -> dict:
        """Replay the log and return ``{"applied", "discarded", "seq"}``.

        ``applied`` counts the committed put/delete records replayed,
        ``discarded`` is 1 when a single torn final record was dropped (0
        otherwise) and ``seq`` is the durable sequence number after recovery.
        Raises ``CorruptLogError`` (a ``ValueError``) if the log is corrupt;
        nothing is applied or truncated in that case.
        """
        self._ensure_open()
        if self._pending:
            raise ValueError(
                "cannot recover with uncommitted changes in this session")

        frames, terminal = self._read_log()

        # Phase 1: validate the whole log before touching any state.
        committed_end, last_seq, discarded = \
            self._validate(frames, terminal)
        # Open already replays and truncates; keep the torn count it saw so
        # the report is stable across repeated recover() calls.
        discarded = max(discarded, self._discarded)
        self._discarded = discarded

        # Phase 2: replay into a fresh mapping, then publish it.
        state, applied = self._replay(frames, committed_end)

        size = os.fstat(self._fd).st_size
        if size > committed_end:
            os.ftruncate(self._fd, committed_end)
            os.fsync(self._fd)

        self._data = state
        self._seq = last_seq
        self._pending = 0
        self._entries = sum(1 for _m, _v, _s, end in frames
                            if end <= committed_end)
        self._corrupt = False
        return {"applied": applied, "discarded": discarded,
                "seq": last_seq}

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
