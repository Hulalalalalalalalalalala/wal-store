"""Write-ahead log key value store.

Every mutation is appended to the log before it is applied in memory.
A commit appends a commit record carrying the new durable sequence
number.  Recovery replays the log and applies mutations only up to the
last commit record; anything after it was never committed and is
discarded (and truncated from the log).
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

LOG_NAME = "wal.log"

_PUT = 1
_DEL = 2
_COMMIT = 3

# record framing: type byte, payload length, crc32 of (type byte + payload)
_HEADER = struct.Struct(">BII")
_KEYLEN = struct.Struct(">I")
_SEQ = struct.Struct(">Q")


def _crc(rtype: int, payload: bytes) -> int:
    return zlib.crc32(bytes([rtype]) + payload) & 0xFFFFFFFF


def _encode_key_payload(key: str) -> bytes:
    key_bytes = key.encode("utf-8")
    return _KEYLEN.pack(len(key_bytes)) + key_bytes


def _decode_key_payload(payload: bytes) -> tuple[str, bytes]:
    """Split a payload into (key, remaining bytes)."""
    if len(payload) < _KEYLEN.size:
        raise ValueError("corrupt log: key payload too short")
    (key_len,) = _KEYLEN.unpack(payload[: _KEYLEN.size])
    if len(payload) < _KEYLEN.size + key_len:
        raise ValueError("corrupt log: key extends past payload")
    key_bytes = payload[_KEYLEN.size : _KEYLEN.size + key_len]
    rest = payload[_KEYLEN.size + key_len :]
    return key_bytes.decode("utf-8"), rest


class Store:
    """A key value store backed by a write-ahead log in a directory."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        directory = Path(path)
        if not directory.is_dir():
            raise FileNotFoundError(
                f"store directory does not exist: {directory}"
            )
        self._dir = directory
        self._log_path = directory / LOG_NAME
        self._log_path.touch(exist_ok=True)
        self._log = open(self._log_path, "a+b")
        self._state: dict[str, bytes] = {}
        self._seq = 0

    # -- validation -----------------------------------------------------

    @staticmethod
    def _check_key(key: object) -> str:
        if not isinstance(key, str):
            raise TypeError(f"key must be str, got {type(key).__name__}")
        if key == "":
            raise ValueError("key must not be empty")
        return key

    # -- log primitives ---------------------------------------------------

    def _append(self, rtype: int, payload: bytes) -> None:
        record = _HEADER.pack(rtype, len(payload), _crc(rtype, payload)) + payload
        self._log.write(record)
        self._log.flush()
        os.fsync(self._log.fileno())

    def _read_log(self) -> tuple[list[tuple[int, bytes]], int]:
        """Parse the log.

        Returns (records, committed_length) where committed_length is the
        file offset just past the last commit record.  A truncated record
        at the tail (a crash artifact) is silently dropped; a complete
        record that fails its checksum raises ValueError.
        """
        with open(self._log_path, "rb") as fh:
            data = fh.read()
        records: list[tuple[int, bytes]] = []
        committed_len = 0
        pos = 0
        while pos < len(data):
            if len(data) - pos < _HEADER.size:
                break  # truncated header at tail
            rtype, length, crc = _HEADER.unpack(data[pos : pos + _HEADER.size])
            if rtype not in (_PUT, _DEL, _COMMIT):
                raise ValueError(
                    f"corrupt log: unknown record type {rtype} at offset {pos}"
                )
            end = pos + _HEADER.size + length
            if end > len(data):
                break  # truncated payload at tail
            payload = data[pos + _HEADER.size : end]
            if _crc(rtype, payload) != crc:
                raise ValueError(
                    f"corrupt log: checksum mismatch at offset {pos}"
                )
            records.append((rtype, payload))
            pos = end
            if rtype == _COMMIT:
                committed_len = pos
        return records, committed_len

    # -- public interface -------------------------------------------------

    def put(self, key: str, value: bytes) -> None:
        key = self._check_key(key)
        if not isinstance(value, bytes):
            raise TypeError(f"value must be bytes, got {type(value).__name__}")
        self._append(_PUT, _encode_key_payload(key) + value)
        self._state[key] = value

    def get(self, key: str) -> bytes | None:
        key = self._check_key(key)
        return self._state.get(key)

    def delete(self, key: str) -> None:
        key = self._check_key(key)
        self._append(_DEL, _encode_key_payload(key))
        self._state.pop(key, None)

    def commit(self) -> int:
        self._seq += 1
        self._append(_COMMIT, _SEQ.pack(self._seq))
        return self._seq

    def recover(self) -> dict:
        """Replay the log and return a report of what was applied."""
        records, committed_len = self._read_log()
        state: dict[str, bytes] = {}
        pending: dict[str, bytes] = {}
        pending_count = 0
        applied = 0
        seq = 0
        for rtype, payload in records:
            if rtype == _PUT:
                key, value = _decode_key_payload(payload)
                pending[key] = value
                pending_count += 1
            elif rtype == _DEL:
                key, rest = _decode_key_payload(payload)
                if rest:
                    raise ValueError("corrupt log: delete record has trailing bytes")
                pending.pop(key, None)
                pending_count += 1
            else:  # _COMMIT
                if len(payload) != _SEQ.size:
                    raise ValueError("corrupt log: bad commit record")
                (seq,) = _SEQ.unpack(payload)
                state = dict(pending)
                applied += pending_count
                pending_count = 0
        # Drop any uncommitted tail so it cannot resurface on a later
        # recovery after new records are appended.
        self._log.flush()
        os.ftruncate(self._log.fileno(), committed_len)
        self._log.seek(0, os.SEEK_END)
        self._state = state
        self._seq = seq
        return {"applied": applied, "sequence": seq}

    def stats(self) -> dict:
        self._log.flush()
        return {
            "sequence": self._seq,
            "keys": len(self._state),
            "bytes": self._log_path.stat().st_size,
        }

    def close(self) -> None:
        if not self._log.closed:
            self._log.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
