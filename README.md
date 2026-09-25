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
- `compact() -> dict` rewrites committed history into one tight log and
  reclaims the old space, without changing the committed state or the
  durable sequence.
- `scan(start=None, end=None) -> ScanCursor` iterates the pinned committed
  snapshot in raw key-byte order over the half-open range `[start, end)`.
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

## Range scans

`scan(start=None, end=None)` — on a normal or a read-only store — returns
an ordered cursor over the committed snapshot pinned at that instant. The
cursor yields `(key, value)` pairs in raw key-byte order over the
half-open range `[start, end)`: the start key is included, the end key
excluded, and a `None` endpoint leaves that side unbounded. Keys and
values come back exactly as stored, with no encoding conversion; keys
with high bytes order by their raw bytes. Keys that only ever appeared in
delete history never show up, a key overwritten any number of times
yields only its last committed value, and empty values are returned like
any other.

The cursor materialises the matching live keys once, at open, and is then
independent of the store: commits, compaction or crash recovery that
happen while it is being read never change what it yields, and it keeps
reading after the log space its snapshot came from has been reclaimed.
Scanning the same committed state before and after a compaction yields
the identical key/value sequence. Scan cost tracks the number of live
keys, never the length of the log history; a scan never replays the log,
takes a lock, or writes a file, so read-only stores scan their pinned
snapshot without touching anything on disk. A `start` that sorts after
`end` raises `ValueError`, as does reading from a closed cursor;
non-string endpoints raise `TypeError`.

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

`compact()` reclaims history without sacrificing any committed state. When
it finishes, the log contains exactly the live committed key/values: one
put frame per currently committed key, in sorted key order, followed by a
base commit marker that carries the pre-compaction sequence. Deleted keys
are gone for good — a deleted key can never reappear — and overwritten keys
keep only their final value. The durable sequence number is unchanged, so
the next `commit()` writes `seq + 1` and subsequent commits continue the
strict sequence as if no rewrite had happened. The empty committed state
(including "everything was deleted") compacts to an empty log.

`compact()` returns the same three-field report shape as `recover()`:
`applied` is the number of live puts the new log contains, `discarded` is
always `0` and `seq` is the preserved sequence. Uncommitted session changes
make it raise `ValueError`; commit or discard them first.

Compaction is a kill-safe, idempotent convergence like recovery. The new
image is assembled in memory and durably staged as the `wal.cmp` sidecar
(an atomically replaced complete file, never an in-place edit), then
published in two atomic steps — `wal.ckp` first and `wal.log` second —
before the staging file is removed. A process killed at any point, any
number of times, leaves a store that finishes the identical publish on the
next open: the recovered state is exactly the last committed state, the
sequence never regresses and the resulting log/sidecar bytes are
byte-identical to one uninterrupted compaction. Repeating compaction on an
already compact store produces the same bytes.

Live read-only stores are unaffected: they keep serving the complete
snapshot they pinned before, during and after compaction (it lands on the
other half of the same log-versus-checkpoint pair readers already choose
between), while fresh readers open the compacted snapshot. No reader ever
sees a mix of old and new bytes, a half-written file, or content whose
space was reclaimed; killing a reader does not disturb compaction and
killing the compactor does not disturb a reader. Compaction introduces no
new limits beyond the existing single-writer rule, and directories written
by older versions compact directly with no conversion step.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

One writer at a time; no cross-process locking.
Values are bytes; encoding is the caller concern.
No replication; compaction is a writer-initiated rewrite, not background
garbage collection.
