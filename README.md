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
writer mints a token it durably publishes the pinned snapshot under the
immutable, content-addressed `wal.s<seq>.<id>` replica name (an atomic
write, never edited in place): that replica is a small *manifest* listing
the ordered `wal.b<id>` blocks of the snapshot, and each block is one
canonical key/value frame named by its own content identity and shared
across every generation containing that same pair, so the token keeps
resolving after the underlying log bytes are compacted away without the
store holding one full image per generation; snapshots that were never
published can also be reconstructed from the committed log prefix or the
checkpoint. A replica written by an older build — a whole image rather
than a block manifest under the same name — stays readable and
reclaimable with no conversion. A read-only store never publishes a
snapshot replica or a block when minting a token; such a token still
resolves because the writer has published (or can reconstruct) that
snapshot. The reader's only on-disk artifact is the short-lived lease
registered when its store or cursor opened, which keeps that snapshot in
use across processes for as long as the resumed scan is alive.

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

Published snapshots are stored chunked and content-addressed, so the
store directory does not grow one full image per generation:

- **Blocks (`wal.b<id>`)** — the snapshot image is split into its
  canonical per-key frames, and each frame is one immutable block named
  by the 32-byte sha256 identity of its own bytes. The same key/value
  pair in any number of generations is the identical frame and therefore
  one block shared by every replica listing it. The total number of
  blocks grows with distinct frame content, not with generations.
- **Replica manifests (`wal.s<seq>.<id>`)** — the replica for one
  generation keeps its existing name and its existing content identity
  (the snapshot identity carried in tokens is unchanged). Its bytes are
  now a small framed manifest: a header carrying the sequence and
  snapshot identity, followed by one reference per ordered block whose
  concatenated frames are that image. A manifest is published atomically
  (temp file, fsync, rename, directory sync) and never edited in place.
  A whole-image replica written by an older build under the same name is
  told apart by its header frame and remains readable and reclaimable
  with no conversion.

Minting a token publishes one such manifest (plus, for frames not
already on disk, their blocks); without lifecycle management the
replicas and the blocks still pile up. Reclamation fixes what survives
and is a reference-safe two-phase convergence that runs after every
commit and every compaction (and is finished at every writer open, so a
sweep killed mid-run simply completes on reopen):

- **In use** — a replica is never reclaimed while any open cursor
  (including one merely iterating, with no token minted), any live
  read-only store or any resumed scan is serving that snapshot, in this
  process or in *any other process*. Cross-process users register a
  short-lived lease sidecar (`wal.lease.<pid>.<rand>`, the only kind of
  file a reader ever writes): it lists the snapshots that process has in
  use with a heartbeat, and the writer reads every lease before removing
  a replica (re-reading them immediately before each unlink, so a lease
  taken mid-sweep still protects its replica). Cursors and resume
  sessions register leases on either open form of the store — a writer's
  cursor or resumed scan registers with exactly the same scope as a
  read-only one. An orderly close removes the lease; a process killed
  without closing stops heartbeating, and expiry is then detected two
  ways — the heartbeat goes stale after the lease TTL, and a lease whose
  owning process no longer exists expires immediately — after which the
  lease pins nothing and is swept. An in-use token keeps pointing at the
  same snapshot across commits, compactions and crash recovery; resuming
  it is byte-for-byte the tail of the original one-shot scan. Reclamation
  changes in-flight reads and resumes by no byte, and a deleted key
  never comes back.
- **Always retained** — the current committed snapshot and the newest
  three published predecessor generations are never reclaimed.
- **Phase one (manifests)** — manifests outside the retention window
  with no user (no in-process pin and no fresh lease in any process) are
  deleted; legacy whole-image replicas follow the same rules. Only after
  the manifest set has converged and been directory-synced does phase
  two run.
- **Phase two (blocks)** — a block is deleted exactly when no surviving
  manifest references it. By the phase-one rules such a block is outside
  the retention window and referenced by no in-use snapshot and no fresh
  lease. A block shared by even one surviving replica stays; a block no
  replica names any more is removed. Each phase ends with one directory
  sync, a block is fully read at most once during a sweep, and block
  publishes are batch-synced, so the number of directory syncs stays
  bounded regardless of how many blocks or replicas exist.

Every use also verifies content against identity: when the store is
opened, when a token is resumed and when the writer decides whether a
replica or a block is usable, a block's bytes must hash to its name and
the blocks a manifest lists must compose to the snapshot identity in its
name. All hashing is incremental over a fixed-size buffer — blocks are
streamed as they are checked and the snapshot identity is composed block
by block, each distinct block at most once per sweep — so verification
never holds a whole snapshot in memory. A block or manifest whose
content does not match its identity is damaged: it is never trusted as a
source, never guessed at and never repaired in place, and it changes
neither the committed state nor any read by a byte. Reads and resumed
scans rebuild the snapshot from the committed log prefix or the
checkpoint instead — byte-for-byte the result a healthy replica would
have served — and the writer reclaims the damaged manifest in its
ordinary sweep; a block that is damaged but still referenced is left to
that reference's lifecycle (the next publish replaces it atomically),
while an unreferenced damaged block is deleted in phase two. A damaged
replica of the *current* snapshot is atomically replaced with the
authoritative manifest on the next publish.

A replica disappearing does not by itself invalidate a token: the token
keeps working for as long as that snapshot can still be rebuilt from the
committed log prefix or the checkpoint. Damaged blocks and a damaged
manifest count as no replica at all — they are never read from and are
reclaimed by the writer. Only once every replica is gone or damaged and
neither the surviving log prefix nor the checkpoint can rebuild the
snapshot does resuming an old token raise `ValueError`. Forged,
truncated, corrupted, out-of-range or cross-snapshot tokens raise
`ValueError` just as before; the store never guesses or repairs them.

Two-phase reclamation is an idempotent convergence: independent file
unlinks with one directory sync per phase, so a kill at any point leaves
a state the next open converges to the identical file set — the same
manifests, the same in-use blocks and the same replica files — and the
durable sequence only advances. It cannot delete a block a surviving
manifest names, cannot remove a usable in-use replica (one named by an
in-process pin or by any fresh lease — a damaged file serves no one and
is removed regardless), cannot move the durable sequence or the
committed state, and never touches `wal.log`, `wal.ckp` or any record;
it removes only dead or damaged `wal.b<id>` blocks,
`wal.s<seq>.<id>` replicas and, once expired, stale `wal.lease.*`
sidecars. Files with any other (illegal) name are neither parsed nor
deleted. Each lease sidecar is written only by its owner and is
atomically replaced (never edited in place), so registering, expiring
and reclaiming converge after a kill in any process to one in-use set,
one block set and one replica set. It adds no command-line subcommand:
everything lives in the same store directory behind the existing
`scan`/token calls; a reader writes only its short-lived lease.

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
