"""Write-ahead log key value store.

Log format: a sequence of framed records.

    [crc32: 4 bytes][length: 4 bytes][payload: length bytes]

The CRC32 covers ``length`` (packed) plus ``payload`` so a corrupted
length field is detected as corruption instead of looking like a
truncated tail.  Payloads:

    PUT     op=1  u32 key_len, key utf-8, u32 value_len, value bytes
    DELETE  op=2  u32 key_len, key utf-8
    COMMIT  op=3  u64 sequence

A mutation becomes durable only once a COMMIT record after it has been
flushed to disk.  Recovery applies mutations up to the last COMMIT and
truncates the log there, dropping both a crash-truncated tail record
and any records that were never committed.
"""

from __future__ import annotations

import os
import struct
import zlib

_HEADER = struct.Struct(">II")  # crc32, payload length
_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")

_OP_PUT = 1
_OP_DELETE = 2
_OP_COMMIT = 3

LOG_FILE_NAME = "wal.log"


def _frame(payload: bytes) -> bytes:
    crc = zlib.crc32(_U32.pack(len(payload)))
    crc = zlib.crc32(payload, crc) & 0xFFFFFFFF
    return _HEADER.pack(crc, len(payload)) + payload


def _encode_put(key: str, value: bytes) -> bytes:
    key_bytes = key.encode("utf-8")
    return (
        struct.pack(">B", _OP_PUT)
        + _U32.pack(len(key_bytes))
        + key_bytes
        + _U32.pack(len(value))
        + value
    )


def _encode_delete(key: str) -> bytes:
    key_bytes = key.encode("utf-8")
    return struct.pack(">B", _OP_DELETE) + _U32.pack(len(key_bytes)) + key_bytes


def _encode_commit(sequence: int) -> bytes:
    return struct.pack(">B", _OP_COMMIT) + _U64.pack(sequence)


def _decode_payload(payload: bytes):
    """Parse one record payload.

    Returns ("put", key, value), ("delete", key) or ("commit", sequence).
    Raises ValueError when the payload is structurally invalid.
    """
    try:
        op = payload[0]
        if op in (_OP_PUT, _OP_DELETE):
            (key_len,) = _U32.unpack_from(payload, 1)
            key_start = 1 + _U32.size
            key = payload[key_start : key_start + key_len].decode("utf-8")
            if len(payload) < key_start + key_len:
                raise ValueError("short key")
            pos = key_start + key_len
            if op == _OP_DELETE:
                if pos != len(payload):
                    raise ValueError("trailing bytes in delete record")
                return ("delete", key)
            (value_len,) = _U32.unpack_from(payload, pos)
            value_start = pos + _U32.size
            value = payload[value_start : value_start + value_len]
            if value_start + value_len != len(payload):
                raise ValueError("bad value length in put record")
            return ("put", key, value)
        if op == _OP_COMMIT:
            if len(payload) != 1 + _U64.size:
                raise ValueError("bad commit record length")
            (sequence,) = _U64.unpack_from(payload, 1)
            return ("commit", sequence)
        raise ValueError(f"unknown record op {op}")
    except (IndexError, struct.error, UnicodeDecodeError) as exc:
        raise ValueError(f"malformed record payload: {exc}") from exc


def _check_key(key) -> None:
    if not isinstance(key, str):
        raise TypeError(f"key must be str, got {type(key).__name__}")
    if key == "":
        raise ValueError("key must not be empty")


class Store:
    """A key value store backed by a write-ahead log in a directory."""

    def __init__(self, path):
        path = os.fspath(path)
        if not os.path.isdir(path):
            raise FileNotFoundError(f"store directory does not exist: {path}")
        self._path = path
        self._log_path = os.path.join(path, LOG_FILE_NAME)
        self._data: dict[str, bytes] = {}
        self._sequence = 0
        # Make sure the log file exists so stats() and recover() see it.
        with open(self._log_path, "ab"):
            pass

    # -- mutations ------------------------------------------------------

    def put(self, key, value) -> None:
        _check_key(key)
        if not isinstance(value, bytes):
            raise TypeError(f"value must be bytes, got {type(value).__name__}")
        self._append(_encode_put(key, value))
        self._data[key] = value

    def delete(self, key) -> None:
        _check_key(key)
        self._append(_encode_delete(key))
        self._data.pop(key, None)

    def commit(self) -> int:
        """Flush a commit record and return the new durable sequence."""
        self._sequence += 1
        self._append(_encode_commit(self._sequence))
        return self._sequence

    # -- reads ----------------------------------------------------------

    def get(self, key):
        _check_key(key)
        return self._data.get(key)

    def stats(self) -> dict:
        return {
            "sequence": self._sequence,
            "keys": len(self._data),
            "bytes": os.path.getsize(self._log_path),
        }

    # -- recovery -------------------------------------------------------

    def recover(self) -> dict:
        """Replay the log and rebuild the committed in-memory state.

        Returns {"applied": <mutations applied>, "sequence": <durable seq>}.
        A truncated record at the tail (crash mid-write) is discarded
        silently; a checksum or structural failure anywhere else raises
        ValueError and no partial state is applied.
        """
        state: dict[str, bytes] = {}
        pending: list = []
        applied = 0
        sequence = 0
        committed_end = 0  # offset just past the last COMMIT record

        with open(self._log_path, "rb") as log:
            offset = 0
            while True:
                header = log.read(_HEADER.size)
                if not header:
                    break  # clean end of log
                if len(header) < _HEADER.size:
                    break  # truncated tail record: discard silently
                crc, length = _HEADER.unpack(header)
                payload = log.read(length)
                if len(payload) < length:
                    break  # truncated tail record: discard silently
                actual = zlib.crc32(_U32.pack(length))
                actual = zlib.crc32(payload, actual) & 0xFFFFFFFF
                if actual != crc:
                    raise ValueError(
                        f"checksum mismatch in log record at offset {offset}"
                    )
                record = _decode_payload(payload)
                offset += _HEADER.size + length
                if record[0] == "commit":
                    for mutation in pending:
                        if mutation[0] == "put":
                            state[mutation[1]] = mutation[2]
                        else:
                            state.pop(mutation[1], None)
                    applied += len(pending)
                    pending.clear()
                    sequence = record[1]
                    committed_end = offset
                elif record[0] == "put":
                    pending.append(("put", record[1], record[2]))
                else:
                    pending.append(("delete", record[1]))

        # Drop the truncated tail and any records never covered by a
        # commit so later appends cannot resurrect them.
        with open(self._log_path, "r+b") as log:
            log.truncate(committed_end)

        self._data = state
        self._sequence = sequence
        return {"applied": applied, "sequence": sequence}

    # -- internals ------------------------------------------------------

    def _append(self, payload: bytes) -> None:
        with open(self._log_path, "ab") as log:
            log.write(_frame(payload))
            log.flush()
            os.fsync(log.fileno())

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Store(path={self._path!r}, keys={len(self._data)}, sequence={self._sequence})"
