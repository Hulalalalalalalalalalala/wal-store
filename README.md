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

`wal_store.inject_tear(source, destination, offset)` is a deterministic
verification aid. It writes to `destination` a copy of `source` cut at
exactly `offset` bytes — the image of a writer killed precisely when its
append had reached that byte offset — and never modifies the source. Either
path argument may be a store directory, in which case its `wal.log` is
used. The offset must be an integer with `0 <= offset <= len(source)`; a
negative or past-the-end offset raises `IndexError`, a non-integer raises
`TypeError`, and `offset == len(source)` is the full-length clean control
copy. The destination must not resolve to the source log. It is not used by
the normal write path.

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

### Recovery itself may be killed

Recovery is an idempotent, kill-safe convergence rather than one truncate.
It may be interrupted at any point — while the dirty tail is being dropped
or the committed prefix is being rebuilt — and rerun any number of times;
the result is always exactly the state and the report of one uninterrupted
clean recovery, with exit code `0`. The durable sequence never regresses,
the next commit is always `seq + 1`, and a half-finished shrink left by a
kill is silently converged instead of being mistaken for a torn tail or
for corruption.

Two internal sidecar files in the store directory provide this; they are
not part of the public API and are maintained entirely below the write
path:

- `wal.ckp` is an atomically replaced (temp file, fsync, rename, directory
  fsync) byte-for-byte copy of the log prefix ending at the most recent
  durable commit marker. A log torn at *any* offset — even offset `0`, or
  inside the committed prefix — is rebuilt from it.
- `wal.rec` is a small marker written only after the log passes full
  validation and only *before* the repair starts; it pins the clean prefix
  length and the discarded count. A repair interrupted mid-copy is simply
  repeated on reopen, and the marker keeps `discarded` unchanged across
  reopens and repeated recoveries until the next successful commit closes
  the epoch and removes it.

The durable commit boundary is the maximum of the validated log boundary,
the checkpoint boundary and the marker boundary, so it can never move
backwards. Every repair step is either an atomic rename or safe to repeat.

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
