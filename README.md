# wal-store

Key value store that appends every mutation to a write-ahead log first, so a process killed between writes and checkpoints recovers exactly the committed state.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m wal_store --path ./store recover
    python3 -m wal_store --path ./store put <key> --value-file <path>
    python3 -m wal_store --path ./store get <key>

## Public interface

`wal_store.Store(path)` opens the store directory; a missing directory raises `FileNotFoundError`.
- `put(key, value) -> None` records a mutation.
- `get(key) -> bytes | None` reads the current value.
- `delete(key) -> None` records a removal.
- `commit() -> int` advances the durable sequence number.
- `recover() -> dict` replays the log and reports `{"applied": int, "discarded": int, "seq": int}`: committed mutations applied, uncommitted tail records discarded, and the durable sequence number.
- `stats() -> dict` reports sequence, entries and bytes.

A record torn by a kill mid-write is simply discarded at recovery; damage anywhere else in the log raises `wal_store.CorruptLogError` (a `ValueError`). `python3 -m wal_store --path DIR recover` prints the report as one compact JSON line and exits 3 without printing it when the log is corrupt.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

One writer at a time; no cross-process locking.
Values are bytes; encoding is the caller concern.
No replication and no compaction.
