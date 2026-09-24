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
- `compact() -> dict` rewrites committed history into one compact log and
  frees the old space; see [Compaction](#compaction).
- `stats() -> dict` reports sequence, entries and bytes.

`wal_store.Store(path, read_only=True)` opens an isolated reader. Any
number of read-only processes may coexist with the single writer in the
same directory. A reader takes no lock and never creates or changes a
file; it pins a snapshot at open time and every `get` reads from that one
complete committed snapshot, so uncommitted mutations, a half-written
commit, a torn record or a half-finished shrink are never visible.
Successive reads from the same reader may advance across snapshots only by
reopening; each read is of one fully committed snapshot. Reads do not
replay the log and do not block the writer: killing or suspending a reader
never affects the writer's commits, recovery or stats. `put`, `delete`,
`commit` and `recover` are rejected on a read-only store; `stats` reports
the pinned snapshot and `get` is the normal way to read it. Readers serve
from the atomically replaced `wal.ckp` sidecar (and the committed log
prefix), so directories written by older versions open directly.

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

## Compaction

`compact()` rewrites the committed history in place into one compact log
that holds exactly one put record per key alive in the current committed
snapshot, followed by a single *base* commit marker
(`{"t":"c","s":seq,"b":1}`) carrying the **same** durable sequence number,
and then releases the old log's space. After compaction:

- the key/value state is byte-for-byte the last committed state;
- the durable sequence number is unchanged and the next commit is
  `seq + 1`;
- keys that only ever existed in deleted history do not come back, and a
  store that never committed compacts to an empty log;
- `recover()` still reports its three integers (`applied` then counts the
  live records, `discarded` is 0, `seq` is preserved).

Compaction shares the store directory, commands and framing with writing,
reading, deleting, committing, recovering and stats; it adds no
subcommand and changes none of those semantics. It is rejected while the
session has uncommitted changes. It is crash-safe at any byte position and
idempotent: the compacted image is fully staged and fsynced as `wal.cmp`
with a plan marker `wal.cpr` published atomically before any reader-visible
file is replaced; the checkpoint is replaced first and the log immediately
after, each by an atomic rename of a fully fsynced temp file. A kill at any
point leaves only whole files, and reopening (or calling `compact()` again)
converges to exactly the bytes one clean compaction produces, regardless of
how many times it was interrupted. Read-only processes never open the
staging files and serve their pinned snapshot from memory, so a reader alive
before and after the compaction reads one complete committed snapshot each
time, never a mixture, a half state or released bytes; a killed reader does
not affect compaction and a compaction killed mid-publish does not affect
readers. Directories written by older versions open and compact without
conversion.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

One writer at a time; no cross-process locking.
Values are bytes; encoding is the caller concern.
No replication.
