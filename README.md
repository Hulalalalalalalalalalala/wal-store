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
- `get(key) -> bytes | None` reads the value at the last committed snapshot.
- `delete(key) -> None` records a removal.
- `delete_range(start=None, end=None) -> None` records a batch removal of
  every committed key in a half-open byte range (`delete(start, end)` is
  the same call).
- `commit() -> int` advances the durable sequence number. Every call writes
  a commit marker and advances the sequence by exactly one, including a
  commit with no pending changes; the first commit after a compaction is the
  pre-compaction sequence plus one.
- `recover() -> dict` replays the log and reports what it applied.
- `compact() -> dict` rewrites committed history into one tight log and
  reclaims the old space, without changing the committed state or the
  durable sequence.
- `scan(start=None, end=None) -> ScanCursor` opens an ordered read-only
  cursor over the committed snapshot pinned at that moment.
- `ScanCursor.token() -> bytes` (alias `mark()`) serialises the cursor's
  current position into a resumable scan token; `scan(token=tok)`
  (alias `scan(resume=tok)`) continues that scan from the token.
- `stats() -> dict` reports sequence, entries and bytes.

## Scanning

`scan()` works on both open forms and never touches the write path. The
returned cursor is an iterator of `(key, value)` pairs in bytewise key
order — keys compare by their raw UTF-8 bytes and values come back as the
exact stored bytes, with no encoding conversion — covering `start`
inclusive through `end` exclusive; a `None` endpoint leaves that side
unbounded, so `scan()` walks everything. Keys that only ever appeared in
delete records never show up, a key overwritten any number of times yields
only its last committed value, and empty values scan like any other. A
writer's uncommitted changes are not part of the snapshot — they are
invisible to `scan()` and to `get()` alike, both of which read the last
committed snapshot; a read-only store scans the snapshot it pinned at open.

The snapshot is materialised once when the cursor opens, so later commits,
compaction, crash recovery and reclamation of the old log space never
change what the cursor yields, and the same snapshot scans to the identical
sequence before and after compaction. Scanning never replays the log,
takes no lock and keeps no history in memory, so its cost is independent
of the log's length. A `start` that sorts after `end` raises `ValueError`,
as does reading from a cursor after `close()` (the cursor is also a
context manager).

## Resumable scans

A scan can be paused and later continued from exactly where it stopped, in
the same process or after reopening the store (writer or read-only). At any
point a cursor can serialise its current position into an opaque `bytes`
token:

- `ScanCursor.token()` returns the token; `mark()` is an alias.
- `Store.scan(token=tok)` opens a new cursor that continues the identical
  range from the identical position over the identical snapshot;
  `scan(resume=tok)` is an alias. The token already carries its range, so
  passing `start`/`end` together with a token raises `ValueError`.

Splitting one scan at a token and concatenating the pieces yields exactly
the pairs a single uninterrupted scan would have yielded, byte for byte:
the resumed stream is the tail of the original scan.

The token names the snapshot it pins — the durable sequence number and a
content identity over the snapshot's canonical image — together with the
scan range and the next position. It stays valid across later commits,
compaction, crash recovery and reclamation of the old log space. When a
writer mints a token it durably publishes the pinned snapshot as an
immutable, content-addressed `wal.s<seq>.<id>` sidecar (an atomic write,
never edited in place), so the token keeps resolving after the underlying
log bytes are compacted away; snapshots that were never published can also
be reconstructed from the committed log prefix or the checkpoint. A
read-only store never publishes a snapshot copy when minting a token;
such a token still resolves because the writer has published (or can
reconstruct) that snapshot. The reader's only on-disk artifact is the
short-lived lease registered when its store or cursor opened, which keeps
that snapshot in use across processes for as long as the resumed scan is
alive.

Deleted keys never come back. A token pins one exact committed snapshot:
resuming it reads precisely that snapshot, whether or not the same keys were
later deleted, and a range tombstone that compaction has since reclaimed
neither removes nor revives anything on the resumed path. A fresh scan of a
snapshot after the delete never shows the key.

A token is opaque and strictly validated. Anything forged, truncated,
corrupted (checksum mismatch), carrying an out-of-range position, naming a
reversed range, or referring to a snapshot this store does not hold (a
foreign store, or a snapshot whose every copy has been reclaimed) raises
`ValueError`; nothing is guessed or repaired. A non-`bytes` token raises
`TypeError` — `bytearray` and `memoryview` are not accepted and are never
coerced. The token format is platform-independent: the same snapshot at
the same position always mints byte-identical tokens on Windows and Linux
(all integers are big-endian and no bytes are translated), so tokens can
be handed across platforms and processes.


## Historical snapshot lifecycle

Minting a token publishes one immutable `wal.s<seq>.<id>` snapshot copy;
without lifecycle management those copies pile up forever, one per
minted snapshot. Reclamation fixes which copies survive and runs after
every commit and every compaction (and is finished at every writer open,
so a sweep killed mid-run simply completes on reopen):

- **In use** — a copy is never reclaimed while any open cursor (including
  one merely iterating, with no token minted), any live read-only store or
  any resumed scan is serving that snapshot, in this process or in *any
  other process*. Every cursor and resume session — on a writer or a
  read-only store alike — and every read-only store registers a
  short-lived lease sidecar
  (`wal.lease.<pid>.<rand>`, the only kind of file a reader ever writes):
  it lists the snapshots that process has in use with a heartbeat, and the
  writer reads every lease before removing a copy (re-reading them
  immediately before each unlink, so a lease taken mid-sweep still
  protects its copy). An orderly close removes the lease; a process killed
  without closing stops heartbeating, and after the lease TTL its lease
  pins nothing and is swept. An in-use token keeps pointing at the same
  snapshot across commits, compactions and crash recovery; resuming it is
  byte-for-byte the tail of the original one-shot scan. Reclamation
  deletes only redundant copies — the reads and resumes in flight do not
  change by a byte, and a deleted key never comes back.
- **Always retained** — the current committed snapshot and the newest
  three published predecessor generations are never reclaimed.
- **Everything else** — copies outside the retention window with no user
  (no in-process pin and no fresh lease in any process) are deleted.

A copy disappearing does not by itself invalidate a token: the token
keeps working for as long as that snapshot can still be rebuilt from the
committed log prefix or the checkpoint. Only once every copy is gone and
neither the surviving log prefix nor the checkpoint can rebuild the
snapshot does resuming an old token raise `ValueError`. Forged,
truncated, corrupted, out-of-range or cross-snapshot tokens raise
`ValueError` just as before; the store never guesses or repairs them.

A copy's name carries its content identity, and the name is never taken
on faith: at writer open, when a token is resumed and whenever the writer
decides a copy is usable, the file's actual content is checked against
that identity. A copy whose content does not match is damaged — it is
never trusted as a source, never guessed and never repaired in place, and
it changes no committed content and no key any read returns. Reads and
resumed scans rebuild the pinned snapshot from the committed log prefix
or the checkpoint instead, so the result is byte-for-byte identical to a
resume served by an intact copy; only when the copy, the log prefix and
the checkpoint can none of them rebuild the snapshot does an old token
raise `ValueError`. The writer reclaims a damaged copy like any dead one
(and atomically replaces the current snapshot's copy when republishing),
so publishing and replacing copies stays atomic and kill-safe: a kill at
any point and a reopen converge to the same copy set and the same in-use
snapshots.

Reclamation is an idempotent convergence: independent file unlinks with
a directory sync at the end, so a kill at any point leaves a state the
next open converges to the identical file set. It cannot delete an
in-use copy (one named by an in-process pin or by any fresh lease),
cannot move the durable sequence or the committed state, and never
touches `wal.log`, `wal.ckp` or any record; it removes only dead
`wal.s<seq>.<id>` copies and, once expired, stale `wal.lease.*`
sidecars. Each lease sidecar is written only by its owner and is
atomically replaced (never edited in place), so registering, expiring
and reclaiming converge after a kill in any process to one in-use set
and one copy set. It adds no command-line subcommand: everything lives
in the same store directory behind the existing `scan`/token calls, and
the lease is the one kind of new file — a reader writes nothing else.


## Range deletes

`delete_range(start=None, end=None)` removes every committed key a scan
of the same endpoints would return: keys compare by their raw UTF-8
bytes, the range is half-open (`start` inclusive, `end` exclusive) and a
`None` endpoint leaves that side unbounded, so `delete_range()` clears
the whole store while `delete_range("a", "a")` removes nothing. The
shorthand `delete(start, end)` is the same call; `delete(key)` still
removes one key. Endpoint validation is shared with `scan`: non-string
endpoints raise `TypeError` and a `start` sorting after `end` raises
`ValueError`.

Like every other mutation the tombstone is only staged in the session:
it is invisible to scans and read-only stores until `commit()`, and a
writer that is killed first reopens at the last committed state. Once
committed, the covered keys disappear from single-key reads and scans
alike, and a key that only ever appears in delete history can never come
back. Writing a key inside a previously deleted range simply stores the
new value — after commit it reads back as that last committed value,
with keys and values still handled as raw bytes.

Recovery resolves a range tombstone by removing the covered keys; the
report counts the tombstone record under `applied`. Compaction reclaims
it together with the rest of the dead history: the compacted image
contains one live put per surviving key and no tombstones at all, so
reclaiming the record can never resurrect a deleted key, and a snapshot
scans to the identical bytes before and after compaction.

## Read-only stores

`wal_store.Store(path, read_only=True)` opens an isolated reader. Any
number of read-only processes may coexist with the single writer in the
same directory. A reader takes no lock and never touches the log, a
checkpoint or a snapshot copy; the only file it creates is its own
short-lived lease sidecar, registering the snapshots it has in use so a
writer in another process keeps them (best-effort — a read-only
directory simply has no lease, and the lease is removed again on close
or expires by heartbeat if the process is killed). It pins a snapshot at
open time and every `get` reads from that one complete committed
snapshot, so uncommitted mutations, a half-written commit, a torn record
or a half-finished shrink are never visible. Successive reads from the
same reader may advance across snapshots only by reopening; each read is
of one fully committed snapshot. Reads do not replay the log and do not
block the writer: killing or suspending a reader never affects the
writer's commits, recovery or stats (a killed reader leaves only a lease
that is ignored once stale and then swept). `put`, `delete`, `commit`
and `recover` are rejected on a read-only store; `stats` reports the
pinned snapshot and `get` is the normal way to read it. Readers serve
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

`compact()` reclaims history without sacrificing any committed state. When
it finishes, the log contains exactly the live committed key/values: one
put frame per currently committed key, in sorted key order, followed by a
base commit marker that carries the pre-compaction sequence. Deleted keys
are gone for good — a deleted key can never reappear — and overwritten keys
keep only their final value. The durable sequence number is unchanged, so
the next `commit()` writes `seq + 1`; every commit advances, including an
empty one, and subsequent commits continue the strict sequence as if no
rewrite had happened. The empty committed state
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
