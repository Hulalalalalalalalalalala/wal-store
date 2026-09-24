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

`wal_store.Store(path)` opens the store directory. The directory must
exist; opening a missing path raises `FileNotFoundError` (the `put` command
line subcommand creates it).
- `put(key, value) -> None` records a mutation.
- `get(key) -> bytes | None` reads the current value.
- `delete(key) -> None` records a removal.
- `commit() -> int` advances the durable sequence number.
- `recover() -> dict` replays the log and reports what it applied.
- `stats() -> dict` reports sequence, entries and bytes.

`wal_store.inject_tear(source, destination, offset)` is a verification aid:
it writes a byte-for-byte copy of `source` cut at exactly `offset` bytes (a
replica of a process killed at that write position) to `destination` and
leaves the source untouched. Either argument may be a store directory, in
which case its `wal.log` is used. A negative or past-the-end offset raises
`IndexError`. It is never used by the normal write path.

## Recovery

`recover()` returns a report with three integer fields, in this key order:

- `applied` — committed put/delete records replayed into the state,
- `discarded` — torn tail records dropped (at most one, the unfinished final
  write left by a process killed mid append; `0` on a clean log),
- `seq` — the durable sequence number after recovery.

A record killed halfway through is not a mutation: it is discarded and the
recovered state is exactly the last committed state. An empty log recovers
to an empty state, which is a normal result. Recovery truncates the dirty
tail and the next commit uses `seq + 1`, no matter how often the store is
reopened.

### Recovery may itself be killed

Recovery is an idempotent, kill-safe convergence. Interrupting it at any
point — while the dirty tail is being dropped or the log is being rebuilt —
and reopening any number of times reaches exactly the state and the report
of one clean recovery, with exit code `0`; the durable sequence never
regresses and the next commit is always `seq + 1`.

Two internal sidecar files in the store directory make this work; they are
not part of the public API and are maintained entirely below the write
path:

- `wal.ckp` is an atomically replaced copy of the log prefix ending at the
  most recent commit marker. A log torn at *any* offset — even offset 0, or
  inside the committed prefix — is silently rebuilt from it, so a
  half-finished convergence can never be mistaken for a torn tail or for
  corruption.
- `wal.rec` is a small marker written after validation and before the
  repair, recording the clean prefix length and the discarded count. A
  repair interrupted mid-copy is simply repeated on reopen, and the marker
  keeps `discarded` stable across reopens and repeated recoveries until the
  next successful commit closes the epoch.

Only the final record may be incomplete. Any incomplete record elsewhere,
a length out of bounds, unparseable content, or a duplicate/regressing
sequence is corruption: recovery raises `wal_store.CorruptLogError`
(a subclass of `ValueError`) and stops without applying or removing
anything.

The command line `recover` prints the report as one compact JSON line with
the same key order and a trailing newline, e.g.
`{"applied":2,"discarded":0,"seq":1}`. Discarding a torn tail is not an
error and the exit code is `0`; corruption prints one explanatory line to
standard error and exits `3` without printing the JSON line.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

One writer at a time; no cross-process locking.
Values are bytes; encoding is the caller concern.
No replication and no compaction.
