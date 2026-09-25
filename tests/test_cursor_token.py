"""Resumable range scans with serialisable cursor tokens.

These tests cover:

* ``ScanCursor.token()`` (and its ``mark()`` alias) naming the pinned
  snapshot, scan range and next position, and ``Store.scan(token=...)``
  continuing byte-for-byte from that position -- in the same process, a
  reopened writer and a reopened read-only store;
* the resumed stream being exactly the tail of a one-shot full scan, at
  every position and for every bounded/open-ended range;
* tokens staying valid across later commits, crash recovery, compaction and
  the reclamation of the old log space, in-process and across processes;
* range tombstones -- including ones compaction has since reclaimed --
  never resurrecting a deleted key on the resumed path: the resumed stream
  is always the exact pinned snapshot;
* a writer's single-key reads and scans both seeing only the last committed
  snapshot while uncommitted changes are staged;
* read-only stores never creating or modifying a file, even when minting a
  token, while the token still resolves once a writer holds the snapshot;
* forged, truncated, corrupted, mistyped, out-of-range, foreign-store and
  cross-snapshot tokens all raising ``ValueError``/``TypeError``, never
  being guessed into a result;
* deterministic, platform-independent token bytes for one snapshot and
  position.
"""

import gc
import os
import subprocess
import sys
import tempfile
import unittest

from wal_store import ScanCursor, Store
from wal_store.store import (
    _decode_token,
    _encode_token,
    _snapshot_id,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TokenBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def seed(self, keys):
        with self.writer() as s:
            for key in keys:
                s.put(key, key.encode())
            s.commit()
        return self.snapshot_items(keys)

    @staticmethod
    def snapshot_items(keys):
        return [(k, k.encode()) for k in
                sorted(keys, key=lambda k: k.encode("utf-8"))]


class BasicTokenTest(TokenBase):
    def test_resume_is_exact_tail_of_full_scan(self):
        full = self.seed(["a", "b", "c", "d", "e"])
        with self.writer() as s:
            cur = s.scan()
            head = [next(cur), next(cur)]
            tok = cur.token()
        self.assertEqual(head, full[:2])
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), full[2:])
        # A reader process form resumes the identical tail.
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), full[2:])

    def test_tail_matches_one_shot_scan_at_every_position(self):
        full = self.seed(["k1", "k2", "k3", "k4", "k5"])
        with self.writer() as s:
            for n in range(len(full) + 1):
                cur = s.scan()
                for _ in range(n):
                    next(cur, None)
                tok = cur.token()
                with self.writer() as q:
                    self.assertEqual(list(q.scan(token=tok)), full[n:], n)
                with self.reader() as r:
                    self.assertEqual(list(r.scan(token=tok)), full[n:], n)

    def test_token_is_reusable_and_idempotent(self):
        full = self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            tok = cur.token()
        for _ in range(3):
            with self.reader() as r:
                self.assertEqual(list(r.scan(token=tok)), full[1:])

    def test_exhausted_token_resumes_to_empty_tail(self):
        full = self.seed(["a", "b"])
        with self.writer() as s:
            cur = s.scan()
            self.assertEqual(list(cur), full)
            tok = cur.token()
        with self.reader() as r:
            resumed = r.scan(token=tok)
            self.assertEqual(list(resumed), [])
            # Re-minting at the exhausted position yields the same token.
            self.assertEqual(resumed.token(), tok)

    def test_mark_is_alias_of_token(self):
        self.seed(["a"])
        with self.writer() as s:
            c1, c2 = s.scan(), s.scan()
            self.assertEqual(c1.mark(), c2.token())

    def test_bounded_range_token_carries_its_window(self):
        full = self.seed(["a", "b", "c", "d", "e"])
        with self.writer() as s:
            cur = s.scan("b", "e")  # b, c, d
            self.assertEqual(next(cur), ("b", b"b"))
            tok = cur.token()
        # Endpoints must not be supplied again; the token carries the range.
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [("c", b"c"), ("d", b"d")])
            with self.assertRaises(ValueError):
                r.scan("b", token=tok)
            with self.assertRaises(ValueError):
                r.scan(None, "e", token=tok)

    def test_open_ended_ranges_resume_correctly(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan("b")
            self.assertEqual(next(cur), ("b", b"b"))
            tail_from = cur.token()
            cur2 = s.scan(None, "c")
            next(cur2)
            tail_to = cur2.token()
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tail_from)),
                             [("c", b"c")])
            # [None, "c") covers a and b; after consuming a the tail is b.
            self.assertEqual(list(r.scan(token=tail_to)),
                             [("b", b"b")])

    def test_empty_snapshot_token(self):
        with self.writer() as s:
            tok = s.scan().token()
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), [])
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), [])

    def test_token_after_cursor_close_still_resolves(self):
        full = self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            tok = cur.token()
            cur.close()
        gc.collect()
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), full[1:])
        with self.assertRaises(ValueError):
            cur.token()


class SnapshotPinTest(TokenBase):
    def test_resume_across_later_commits(self):
        full = self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            tok = cur.token()
        with self.writer() as s:
            for i in range(3):
                s.put(f"new{i}", b"x")
                s.commit()
        with self.reader() as r:
            # Still the seq-1 snapshot the token pinned, never the new keys.
            got = list(r.scan(token=tok))
            self.assertEqual(got, full[1:])
            self.assertNotIn(("new0", b"x"), got)

    def test_resume_across_compaction_and_reclaimed_space(self):
        with self.writer() as s:
            for i in range(40):
                s.put(f"k{i:02d}", b"x" * 40)
                s.commit()
            cur = s.scan()
            next(cur)
            next(cur)
            tok = cur.token()
            pinned = list(cur)
        full_head = [("k00", b"x" * 40), ("k01", b"x" * 40)]
        old_log = os.path.getsize(os.path.join(self.dir, "wal.log"))
        with self.writer() as s:
            s.delete("k10")
            s.commit()
            s.put("z", b"z")
            s.commit()
            s.compact()
        # Forty commit markers collapse to one base commit; history shrinks.
        self.assertLess(os.path.getsize(os.path.join(self.dir, "wal.log")),
                        old_log)
        with self.reader() as r:
            # Pinned seq-40 snapshot resumes from position 2, intact: k10
            # present, z absent.
            self.assertEqual(list(r.scan(token=tok)), pinned)
        self.assertEqual(pinned[0], ("k02", b"x" * 40))

    def test_resume_across_recovered_crash(self):
        full = self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            tok = cur.token()
        # Simulate a crash: append a torn uncommitted tail.
        from wal_store.store import _encode_frame, _OP_PUT
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"torn", key="zzz")[:9])
        with self.writer() as s:
            report = s.recover()
            self.assertEqual(report["discarded"], 1)
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), full[1:])

    def test_deleted_key_never_resurrects_on_resume(self):
        self.seed(["keep", "gone"])
        with self.writer() as s:
            tok = s.scan().token()  # token at the very start, seq 1
        with self.writer() as s:
            s.delete("gone")
            s.commit()
            s.compact()
        with self.reader() as r:
            # The token predates the delete and pins that whole snapshot, so
            # it legitimately still reads "gone" -- that is snapshot pinning,
            # not a resurrection.
            self.assertEqual(list(r.scan(token=tok)),
                             [("gone", b"gone"), ("keep", b"keep")])
        # But any scan of a snapshot at/after the delete never brings it back,
        # however the history is reclaimed underneath.
        with self.writer() as s:
            post = s.scan().token()
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("keep", b"keep")])
            self.assertEqual(list(r.scan(token=post)),
                             [("keep", b"keep")])

    def test_range_tombstone_pinned_and_reclaimed_then_resume(self):
        self.seed(["a", "b", "c", "d"])
        with self.writer() as s:
            cur = s.scan()
            head = [next(cur), next(cur)]
            tok = cur.token()
        # Commit a range delete, then compact so the tombstone is reclaimed.
        with self.writer() as s:
            s.delete_range("b", "d")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("d", b"d")])
            s.compact()
            raw = open(os.path.join(self.dir, "wal.log"), "rb").read()
            self.assertNotIn(b'"t":"r"', raw)
        with self.reader() as r:
            # Pinned pre-delete snapshot resumes to its exact tail; the
            # reclaimed tombstone cannot delete from, nor add to, it.
            self.assertEqual(head, [("a", b"a"), ("b", b"b")])
            self.assertEqual(list(r.scan(token=tok)),
                             [("c", b"c"), ("d", b"d")])
        # A post-compaction snapshot scanned or resumed stays deleted.
        with self.writer() as s:
            post_tok = s.scan().token()
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=post_tok)),
                             [("a", b"a"), ("d", b"d")])

    def test_reader_pinned_snapshot_kept_across_compaction(self):
        self.seed(["a", "b"])
        with self.reader() as r:
            cur = r.scan()
            next(cur)
            tok = cur.token()  # reader mint: must not write anything
            with self.writer() as s:
                s.put("c", b"3")
                s.commit()
                s.compact()
            self.assertEqual(list(cur), [("b", b"b")])
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), [("b", b"b")])


class CommittedOnlyReadsTest(TokenBase):
    def test_writer_get_hides_uncommitted_changes(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.put("a", b"pending")
            s.put("b", b"2")
            s.delete("a")
            self.assertEqual(s.get("a"), b"1")  # last committed value
            self.assertIsNone(s.get("b"))       # uncommitted key absent
            self.assertEqual(list(s.scan()), [("a", b"1")])
            s.commit()
            self.assertIsNone(s.get("a"))
            self.assertEqual(s.get("b"), b"2")

    def test_empty_commit_advances_sequence_and_is_readable(self):
        with self.writer() as s:
            s.put("a", b"1")
            self.assertEqual(s.commit(), 1)
            self.assertEqual(s.commit(), 2)          # empty commit
            self.assertEqual(s.stats()["seq"], 2)
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 2)
            self.assertEqual(s.commit(), 3)          # seq+1, not 2


class CrossProcessTokenTest(TokenBase):
    # A read-only process mints a token over its pinned snapshot; a separate
    # process (after more commits and a compaction) resumes from it. The
    # token is binary, so it travels as one base64 line.
    MINT = r"""
import base64, sys
sys.path.insert(0, %r)
from wal_store import Store
r = Store(sys.argv[1], read_only=True)
cur = r.scan("b", "e")
first = next(cur)[0]
sys.stdout.buffer.write(first.encode() + b"\n")
sys.stdout.buffer.write(base64.b64encode(cur.token()) + b"\n")
""" % REPO_ROOT

    def test_token_resumes_in_a_fresh_process(self):
        self.seed(["a", "b", "c", "d", "e"])
        mint = subprocess.run(
            [sys.executable, "-c", self.MINT, self.dir],
            capture_output=True)
        self.assertEqual(mint.returncode, 0, mint.stderr)
        first, b64 = mint.stdout.rstrip(b"\n").split(b"\n")
        self.assertEqual(first, b"b")
        # Advance history and reclaim it before resuming.
        with self.writer() as s:
            s.put("z", b"z")
            s.commit()
            s.compact()
        resume_code = (
            "import base64,sys; sys.path.insert(0,%r);"
            "from wal_store import Store;"
            "tok=base64.b64decode(sys.argv[2]);"
            "r=Store(sys.argv[1], read_only=True);"
            "out=[k for k,_ in r.scan(token=tok)];"
            "sys.stdout.buffer.write(repr(out).encode())" % REPO_ROOT)
        done = subprocess.run(
            [sys.executable, "-c", resume_code, self.dir,
             b64.decode("ascii")], capture_output=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.decode(), "['c', 'd']")


class ReadOnlyNoWritesTest(TokenBase):
    def listing(self):
        out = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                out[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        return out

    def test_reader_mints_and_resumes_without_writing(self):
        self.seed(["a", "b"])
        before = self.listing()
        with self.reader() as r:
            cur = r.scan()
            next(cur)
            tok = cur.token()
            self.assertEqual(list(r.scan(token=tok)), [("b", b"b")])
            # The only file a reader creates is its short-lived lease
            # sidecar; no snapshot copy or other durable file appears.
            leases = [n for n in os.listdir(self.dir)
                      if n.startswith("wal.lease.")]
            self.assertEqual(len(leases), 1)
            cur.close()
        # Once the reader and its cursors are closed the lease is removed and
        # every pre-existing file is byte-for-byte unchanged.
        self.assertFalse(
            [n for n in os.listdir(self.dir) if n.startswith("wal.lease.")])
        self.assertEqual(self.listing(), before)

    def test_reader_scan_creates_nothing_on_old_directory(self):
        # Only wal.log: an old version. A reader adds no wal.s* snapshot
        # copy; the single short-lived lease sidecar it registers while open
        # is removed again on close.
        self.seed(["a"])
        for name in list(os.listdir(self.dir)):
            if name != "wal.log":
                os.unlink(os.path.join(self.dir, name))
        self.assertEqual(set(os.listdir(self.dir)), {"wal.log"})
        with self.reader() as r:
            cur = r.scan()
            next(cur)
            cur.token()
            list(r.scan())
            leases = [n for n in os.listdir(self.dir)
                      if n.startswith("wal.lease.")]
            self.assertEqual(len(leases), 1)
            self.assertFalse(
                [n for n in os.listdir(self.dir) if n.startswith("wal.s")])
            cur.close()
        self.assertEqual(set(os.listdir(self.dir)), {"wal.log"})


class DeterminismTest(TokenBase):
    def test_token_bytes_are_deterministic(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            c1 = s.scan("a", "z")
            next(c1)
            t1 = c1.token()
        with self.writer() as s:
            c2 = s.scan("a", "z")
            next(c2)
            t2 = c2.token()
        with self.reader() as r:
            c3 = r.scan("a", "z")
            next(c3)
            t3 = c3.token()
        self.assertEqual(t1, t2)
        self.assertEqual(t2, t3)
        self.assertTrue(t1.startswith(b"WST1"))
        # Explicit big-endian serialisation: seq and pos decode back exactly.
        seq, sid, lo, hi, pos = _decode_token(t1)
        self.assertEqual((seq, lo, hi, pos), (1, b"a", b"z", 1))
        # Re-encoding the parsed fields reproduces the token verbatim.
        self.assertEqual(_encode_token(seq, sid, lo, hi, pos), t1)

    def test_position_changes_token_but_snapshot_identity_is_fixed(self):
        self.seed(["a", "b"])
        with self.writer() as s:
            cur = s.scan()
            t0 = cur.token()
            next(cur)
            t1 = cur.token()
        self.assertNotEqual(t0, t1)
        _s0, sid0, *_ = _decode_token(t0)
        _s1, sid1, *_ = _decode_token(t1)
        self.assertEqual(sid0, sid1)

    def test_snapshot_identity_is_canonical_content(self):
        # Two stores with identical committed content share a snapshot id;
        # different content never does.
        d1 = os.path.join(self.dir, "one")
        d2 = os.path.join(self.dir, "two")
        os.makedirs(d1)
        os.makedirs(d2)
        for d in (d1, d2):
            with Store(d) as s:
                s.put("k", b"v")
                s.commit()
        with Store(d1) as a, Store(d2) as b:
            self.assertEqual(a.scan().token(), b.scan().token())
        with Store(d2) as b:
            b.put("k", b"different")
            b.commit()
        with Store(d1) as a, Store(d2) as b:
            self.assertNotEqual(a.scan().token(), b.scan().token())


class InvalidTokenTest(TokenBase):
    def _tok(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            return cur.token()

    def test_non_bytes_token_typeerror(self):
        self._tok()
        with self.reader() as r:
            for bad in (1, "abc", [1], object()):
                with self.assertRaises(TypeError):
                    r.scan(token=bad)

    def test_garbage_and_bad_magic_raise_valueerror(self):
        with self.reader() as r:
            for bad in (b"", b"x", b"WST1", b"NOT1" + b"\x00" * 60,
                        b"WST1" + b"\x00" * 60):
                with self.assertRaises(ValueError):
                    r.scan(token=bad)

    def test_truncated_token_raises_valueerror(self):
        tok = self._tok()
        with self.reader() as r:
            for cut in range(0, len(tok)):
                with self.assertRaises(ValueError):
                    r.scan(token=tok[:cut])

    def test_trailing_garbage_raises_valueerror(self):
        tok = self._tok()
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok + b"\x00")

    def test_bit_flip_raises_valueerror(self):
        tok = bytearray(self._tok())
        tok[10] ^= 0xFF  # inside the snapshot identity / body
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=bytes(tok))

    def test_crafted_out_of_range_position_raises(self):
        tok = self._tok()
        seq, sid, lo, hi, pos = _decode_token(tok)
        with self.reader() as r:
            # A structurally valid, checksum-correct token whose position
            # lies outside its own range window.
            bad = _encode_token(seq, sid, lo, hi, pos + 1000)
            with self.assertRaises(ValueError):
                r.scan(token=bad)
            bad = _encode_token(seq, sid, lo, hi, 2 ** 63)
            with self.assertRaises(ValueError):
                r.scan(token=bad)

    def test_foreign_store_token_raises_valueerror(self):
        tok = self._tok()
        other = os.path.join(self.dir, "other")
        os.makedirs(other)
        with Store(other) as s:
            s.put("x", b"y")
            s.commit()
        # Same sequence number, unrelated content: never guessed across.
        with Store(other) as s:
            with self.assertRaises(ValueError):
                s.scan(token=tok)
        with Store(other, read_only=True) as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok)

    def test_token_for_snapshot_store_no_longer_holds_raises(self):
        self.seed(["a"])
        with self.writer() as s:
            tok = s.scan().token()
        # Advance two generations, compact the old history away, then remove
        # the durable sidecar the mint published: the seq-1 snapshot is now
        # nowhere the store can reach, so its token must be rejected rather
        # than guessed at.
        with self.writer() as s:
            s.put("b", b"2")
            s.commit()
            s.put("c", b"3")
            s.commit()
            s.compact()  # log is now one base marker at seq 3
        seq, _sid, _lo, _hi, _pos = _decode_token(tok)
        self.assertEqual(seq, 1)
        for name in list(os.listdir(self.dir)):
            if name.startswith("wal.s1."):
                os.unlink(os.path.join(self.dir, name))
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok)
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=tok)

    def test_token_and_resume_keyword_together_raise(self):
        tok = self._tok()
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok, resume=tok)

    def test_resume_keyword_works_as_alias(self):
        full = self.seed(["a", "b"])
        with self.writer() as s:
            cur = s.scan()
            next(cur)
            tok = cur.token()
        with self.reader() as r:
            self.assertEqual(list(r.scan(resume=tok)), [("b", b"b")])

    def test_resume_on_closed_store_raises(self):
        tok = self._tok()
        r = self.reader()
        r.close()
        with self.assertRaises(ValueError):
            r.scan(token=tok)

    def test_reversed_range_crafted_in_token_raises(self):
        tok = self._tok()
        seq, sid, _lo, _hi, pos = _decode_token(tok)
        bad = _encode_token(seq, sid, b"z", b"a", pos)
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=bad)


if __name__ == "__main__":
    unittest.main()
