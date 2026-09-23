"""Write-ahead log key value store.

Every mutation is first appended to a framed log. A commit marker is the
only thing that advances the durable sequence number. When a store is opened,
any tail that was never committed (including a record torn by a hard kill) is
discarded, so recovery always yields exactly the committed state.

Log frame layout (all integers big-endian)::

    +---------+----------------+----------------------+---------+
    | b"WAL1" | payload_length | payload              | crc32   |
    | 4 bytes | 8 bytes        | metadata\\n<value>    | 4 bytes |
    +---------+----------------+----------------------+---------+

Metadata is a compact JSON object: ``{"t":"p","k":key}`` for puts,
``{"t":"d","k":key}`` for deletes and ``{"t":"c","s":seq}`` for commits.
The raw value bytes follow the first newline, so values need no encoding.
"""

from __future__ import annotations

import json
import os
import zlib

__all__ = ["Store"]

LOG_NAME = "wal.log"

_MAGIC = b"WAL1"
_HEADER = 12   # 4 bytes magic + 8 bytes payload length
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
    header = _MAGIC + len(payload).to_bytes(8, "big")
    return header + payload + zlib.crc32(payload).to_bytes(4, "big")


def _iter_frames(f):
    """Scan an open binary log file from position 0.

    For every intact frame yields
    ``("frame", meta, value, start, end)``. Scanning stops with either no
    terminal event (clean EOF), ``("torn", start)`` for an incomplete final
    frame, or ``("invalid", start)`` for a frame that fails framing,
    checksum or metadata parsing.
    """
    while True:
        start = f.tell()
        header = f.read(_HEADER)
        if not header:
            return
        if len(header) < _HEADER:
            # A crash can leave a few bytes of a frame header: incomplete,
            # not corrupt.
            yield "torn", start
            return
        if header[:4] != _MAGIC:
            yield "invalid", start
            return
        length = int.from_bytes(header[4:], "big")
        if length > _MAX_PAYLOAD:
            yield "invalid", start
            return
        payload = f.read(length)
        checksum = f.read(_TRAILER)
        if len(payload) < length or len(checksum) < _TRAILER:
            yield "torn", start
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

    ``path`` is the store directory; it is created when missing. Opening a
    path that points at a regular file raises ``OSError``.
    """

    def __init__(self, path: str):
        self._dir = path
        self._path = os.path.join(path, LOG_NAME)

        if os.path.lexists(path) and not os.path.isdir(path):
            raise OSError(f"store path is not a directory: {path!r}")
        dir_created = not os.path.isdir(path)
        if dir_created:
            os.makedirs(path, exist_ok=True)

        log_created = not os.path.exists(self._path)
        self._fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                           0o644)
        if log_created:
            os.fsync(self._fd)
        if dir_created or log_created:
            _fsync_dir(path)

        self._data: dict[str, bytes] = {}
        self._seq = 0
        self._entries = 0
        self._pending = 0
        self._closed = False
        self._corrupt = False

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

        Returns ``(committed_end, last_seq)`` -- the byte offset just past
        the last commit marker and its sequence number. Raises ValueError on
        unknown frames, bad metadata, or any gap/duplicate in the strictly
        increasing commit sequence.
        """
        expected_seq = 1
        last_seq = 0
        committed_end = 0
        for meta, _value, _start, end in frames:
            op = meta.get("t")
            if op in (_OP_PUT, _OP_DELETE):
                key = meta.get("k")
                if not isinstance(key, str) or key == "":
                    raise ValueError(f"invalid frame metadata: {meta!r}")
            elif op == _OP_COMMIT:
                seq = meta.get("s")
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise ValueError(f"invalid commit marker: {meta!r}")
                if seq != expected_seq:
                    raise ValueError(
                        f"commit sequence {seq} out of order, "
                        f"expected {expected_seq}")
                last_seq = seq
                expected_seq += 1
                committed_end = end
            else:
                raise ValueError(f"unknown frame type: {op!r}")
        if terminal is not None and terminal[0] == "invalid":
            raise ValueError("corrupt log frame")
        return committed_end, last_seq

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

        When the log proves fully valid, everything past the last commit
        marker (plus a possible torn final frame) is truncated away. If the
        log is corrupt nothing is touched and the store stays empty until a
        ``recover`` call explains the problem.
        """
        frames, terminal = self._read_log()
        try:
            committed_end, last_seq = self._validate(frames, terminal)
        except ValueError:
            # Keep the corrupt log untouched; recover() will report it.
            self._corrupt = True
            return

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
            raise ValueError(
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
        self._entries += 1
        return self._seq

    def recover(self) -> dict:
        self._ensure_open()
        if self._pending:
            raise ValueError(
                "cannot recover with uncommitted changes in this session")

        frames, terminal = self._read_log()

        # Phase 1: validate the whole log before touching any state.
        committed_end, last_seq = self._validate(frames, terminal)

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
        return {"applied": applied, "seq": last_seq}

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
