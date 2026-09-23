"""Core storage implementation: an append-only write-ahead log.

On-disk record layout (all integers unsigned, big-endian)::

    magic   2 bytes  b"WL"
    kind    1 byte   1 = put, 2 = delete, 3 = commit
    seq     8 bytes  durable sequence number the record belongs to
    klen    4 bytes  key length (0 only inside a commit record)
    vlen    4 bytes  value length (0 for delete and commit)
    payload klen + vlen bytes (key bytes then value bytes)
    crc32   4 bytes  over everything above

Mutations of one uncommitted batch all carry ``seq = durable_seq + 1``.
The batch becomes durable exactly when its commit record is appended and
fsynced.

Opening a store replays the log up to the last commit record: an
incomplete trailing record or a batch that never committed is treated as
a normal crash remnant and truncated away. A *complete* record that fails
validation (checksum, framing or sequence order) is corruption: it is
left on disk and the strict :meth:`Store.recover` scan raises
``ValueError`` without touching the committed state.
"""

import os
import struct
import zlib

__all__ = ["Store"]

_MAGIC = b"WL"
_KIND_PUT = 1
_KIND_DELETE = 2
_KIND_COMMIT = 3

_HEADER = struct.Struct(">2sBQII")
_CRC = struct.Struct(">I")
_HEADER_SIZE = _HEADER.size       # 19
_RECORD_FOOTER = _CRC.size        # 4

_LOG_NAME = "wal.log"

_TOMBSTONE = object()


class Store:
    """Append-only write-ahead log key value store rooted at ``path``."""

    def __init__(self, path):
        self._path = os.fspath(path)

        if os.path.exists(self._path) and not os.path.isdir(self._path):
            raise OSError(f"store path is not a directory: {self._path!r}")
        fresh_dir = not os.path.exists(self._path)
        if fresh_dir:
            os.makedirs(self._path, exist_ok=True)

        log_path = os.path.join(self._path, _LOG_NAME)
        # Mutations are appended to the OS immediately; durability is
        # provided by fsync inside commit().
        self._fd = os.open(
            log_path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644
        )
        self._fsync_dir()

        self._data = {}
        self._seq = 0
        self._entries = 0
        self._committed_bytes = 0
        # True when bytes after the last commit fail strict validation.
        self._tail_corrupt = False

        self._pending = {}
        self._pending_seq = None
        self._pending_entries = 0
        self._pending_bytes = 0
        self._dirty = False

        self._load()

    # ---- lifecycle -------------------------------------------------------

    def close(self):
        """Close the underlying log file. Uncommitted changes are dropped."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _fsync_dir(self):
        """Best-effort fsync of the store directory (creation durability)."""
        try:
            dfd = os.open(self._path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dfd)
        except OSError:
            pass
        finally:
            os.close(dfd)

    # ---- validation ------------------------------------------------------

    @staticmethod
    def _check_key(key):
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must not be empty")

    # ---- public API ------------------------------------------------------

    def put(self, key, value):
        """Record a mutation storing ``value`` (bytes) under ``key``."""
        self._check_key(key)
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes")
        self._stage(key, value)

    def get(self, key):
        """Return the current value, or ``None`` if the key is absent."""
        self._check_key(key)
        if key in self._pending:
            staged = self._pending[key]
            return None if staged is _TOMBSTONE else staged
        return self._data.get(key)

    def delete(self, key):
        """Record a removal; deleting a missing key is still logged."""
        self._check_key(key)
        self._stage(key, _TOMBSTONE)

    def commit(self):
        """Make all staged mutations durable and return the durable seq."""
        if not self._dirty:
            return self._seq
        seq = self._pending_seq
        record_len = self._append(_KIND_COMMIT, seq, b"", b"")
        os.fsync(self._fd)

        for key, value in self._pending.items():
            if value is _TOMBSTONE:
                self._data.pop(key, None)
            else:
                self._data[key] = value
        self._seq = seq
        self._entries += self._pending_entries + 1
        self._committed_bytes += self._pending_bytes + record_len

        self._pending = {}
        self._pending_seq = None
        self._pending_entries = 0
        self._pending_bytes = 0
        self._dirty = False
        return self._seq

    def recover(self):
        """Strictly replay the log.

        Returns ``{"applied": committed_mutation_count, "seq":
        durable_seq}``. An incomplete trailing record or an uncommitted
        batch is dropped. A checksum, framing or sequence error raises
        ``ValueError`` before anything is applied or the durable sequence
        advanced.
        """
        if self._dirty:
            raise ValueError(
                "cannot recover with uncommitted changes in session"
            )
        result = self._scan(strict=True)
        state, seq, applied, entries, size = result
        self._data = state
        self._seq = seq
        self._entries = entries
        self._committed_bytes = size
        self._tail_corrupt = False
        self._pending = {}
        self._pending_seq = None
        self._pending_entries = 0
        self._pending_bytes = 0
        return {"applied": applied, "seq": seq}

    def stats(self):
        """Report ``{"seq", "entries", "bytes"}`` of the log file.

        ``seq`` is the durable sequence number; ``entries`` and ``bytes``
        describe the records currently appended to the log (records of a
        commit in progress are dropped on the next open/recover).
        """
        return {
            "seq": self._seq,
            "entries": self._entries + self._pending_entries,
            "bytes": os.fstat(self._fd).st_size,
        }

    # ---- internals -------------------------------------------------------

    def _stage(self, key, value):
        # A complete-but-invalid tail never results from a clean crash; it
        # signals external damage. Refuse to overwrite that evidence until
        # the user runs recover() (which reports the error explicitly).
        if self._tail_corrupt:
            raise ValueError(
                "log tail is corrupt; inspect the store or run recover()"
            )

        if self._pending_seq is None:
            self._pending_seq = self._seq + 1
        key_bytes = key.encode("utf-8")
        if value is _TOMBSTONE:
            record_len = self._append(
                _KIND_DELETE, self._pending_seq, key_bytes, b""
            )
        else:
            record_len = self._append(
                _KIND_PUT, self._pending_seq, key_bytes, value
            )
        self._pending[key] = value
        self._pending_entries += 1
        self._pending_bytes += record_len
        self._dirty = True

    def _append(self, kind, seq, key_bytes, value_bytes):
        header = _HEADER.pack(
            _MAGIC, kind, seq, len(key_bytes), len(value_bytes)
        )
        payload = key_bytes + value_bytes
        record = (
            header
            + payload
            + _CRC.pack(zlib.crc32(header + payload) & 0xFFFFFFFF)
        )
        # O_APPEND positions every write at EOF atomically; still loop to
        # tolerate short writes.
        view = memoryview(record)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]
        return len(record)

    def _load(self):
        """Lenient replay used when opening: make committed state usable."""
        result = self._scan(strict=False)
        state, seq, _applied, entries, size = result
        self._data = state
        self._seq = seq
        self._entries = entries
        self._committed_bytes = size

    @staticmethod
    def _read_exact(fd, offset, size):
        """Read exactly ``size`` bytes at ``offset``.

        Returns ``None`` at clean EOF (no bytes at all) or ``False`` for a
        short read (an incomplete trailing record).
        """
        chunks = []
        remaining = size
        while remaining:
            chunk = os.pread(fd, remaining, offset)
            if not chunk:
                return False if chunks else None
            chunks.append(chunk)
            offset += len(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _scan(self, strict):
        """Parse the log.

        ``strict`` is the recover() mode: every validation failure raises
        ``ValueError`` and the caller discards the (purely local) results.
        In lenient (open) mode a complete-but-invalid record stops the
        scan and the offending tail is preserved on disk and flagged; a
        torn record or an uncommitted batch is truncated away in both
        modes.

        Returns ``(state, durable_seq, applied, committed_entries,
        committed_end)``.
        """
        file_size = os.fstat(self._fd).st_size

        state = {}
        staged = {}
        staged_count = 0
        durable_seq = 0
        pending_seq = None
        applied = 0
        committed_entries = 0
        committed_end = 0
        offset = 0
        hard_error = False

        def fail(reason):
            raise ValueError(f"corrupt write-ahead log: {reason}")

        while offset < file_size:
            record_start = offset
            header = self._read_exact(self._fd, offset, _HEADER_SIZE)
            if header is None or header is False:
                break  # clean EOF or torn header: incomplete trailing record
            magic, kind, seq, klen, vlen = _HEADER.unpack(header)
            if magic != _MAGIC:
                if strict:
                    fail("bad magic")
                hard_error = True
                break
            if kind not in (_KIND_PUT, _KIND_DELETE, _KIND_COMMIT):
                if strict:
                    fail(f"unknown record kind {kind}")
                hard_error = True
                break
            declared_end = (
                record_start
                + _HEADER_SIZE
                + klen
                + vlen
                + _RECORD_FOOTER
            )
            if declared_end > file_size:
                # A well-formed header promising bytes the file does not
                # hold is a torn last record: dropped directly in both modes.
                break

            rest = self._read_exact(
                self._fd,
                offset + _HEADER_SIZE,
                klen + vlen + _RECORD_FOOTER,
            )
            if rest is None or rest is False:
                break  # torn body or trailer: incomplete trailing record

            body, trailer = rest[: klen + vlen], rest[klen + vlen :]
            (stored_crc,) = _CRC.unpack(trailer)
            if stored_crc != zlib.crc32(header + body) & 0xFFFFFFFF:
                if strict:
                    fail("checksum mismatch")
                hard_error = True
                break

            record_end = declared_end

            if kind == _KIND_COMMIT:
                if klen != 0 or vlen != 0:
                    if strict:
                        fail("commit record carries payload")
                    hard_error = True
                    break
                if pending_seq is None:
                    if strict:
                        fail(
                            f"commit for seq {seq} without an open batch"
                        )
                    hard_error = True
                    break
                if seq != pending_seq:
                    if strict:
                        fail(
                            f"commit seq {seq} does not match open batch "
                            f"{pending_seq}"
                        )
                    hard_error = True
                    break
                for key, value in staged.items():
                    if value is _TOMBSTONE:
                        state.pop(key, None)
                    else:
                        state[key] = value
                applied += staged_count
                committed_entries += staged_count + 1
                durable_seq = seq
                committed_end = record_end
                staged = {}
                staged_count = 0
                pending_seq = None
            else:
                if klen == 0:
                    if strict:
                        fail("mutation with empty key")
                    hard_error = True
                    break
                expected = (
                    durable_seq + 1 if pending_seq is None else pending_seq
                )
                if seq < expected:
                    if strict:
                        fail(f"duplicate seq {seq}")
                    hard_error = True
                    break
                if seq > expected:
                    if strict:
                        fail(f"seq gap: expected {expected}, found {seq}")
                    hard_error = True
                    break
                if kind == _KIND_DELETE and vlen != 0:
                    if strict:
                        fail("delete record carries a value")
                    hard_error = True
                    break
                try:
                    key = body[:klen].decode("utf-8", "strict")
                except UnicodeDecodeError:
                    if strict:
                        fail("key is not valid utf-8")
                    hard_error = True
                    break
                if kind == _KIND_PUT:
                    staged[key] = body[klen:]
                else:
                    staged[key] = _TOMBSTONE
                pending_seq = seq
                staged_count += 1

            offset = record_end

        if hard_error:
            # Preserve the tail so strict recover() can report it. The
            # committed prefix remains available for reads.
            self._tail_corrupt = True
        elif committed_end != file_size:
            # Torn record or complete-but-uncommitted batch: normal crash
            # remnant. Drop it so future appends stay contiguous. Reads use
            # pread() and writes use O_APPEND, so the file offset is moot.
            os.ftruncate(self._fd, committed_end)
            os.fsync(self._fd)

        return state, durable_seq, applied, committed_entries, committed_end
