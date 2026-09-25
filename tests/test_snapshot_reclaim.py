"""Lifecycle management for historical snapshot copies.

These tests cover the reclamation rules added on top of resumable scans:

* the store directory holding only the current snapshot and the newest
  three published predecessor generations, no matter how many commits,
  compactions or token mints happen;
* an open cursor or a live read-only store pinning its snapshot so its
  copy is never reclaimed and a token minted on it keeps resuming
  byte-for-byte; the pin releasing when the cursor/reader closes;
* a token staying valid after its copy is gone while the snapshot can
  still be rebuilt from the log prefix/checkpoint, and raising
  ``ValueError`` once no source remains;
* reclamation converging after a kill mid-sweep, never regressing the
  durable sequence or changing committed state;
* only genuine ``bytes`` being accepted as a token (``TypeError`` for
  ``bytearray``/``memoryview`` and other types).
"""

import gc
import os
import subprocess
import sys
import tempfile
import unittest

from wal_store import Store
from wal_store.store import _decode_token

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ReclaimBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def copy_seqs(self):
        return {int(n.split(".")[1][1:])
                for n in os.listdir(self.dir) if n.startswith("wal.s")}

    def commits(self, n):
        with self.writer() as s:
            for i in range(n):
                s.put(f"k{i:02d}", b"v")
                s.commit()


class RetentionTest(ReclaimBase):
    def test_only_current_and_three_predecessors_survive_commits(self):
        self.commits(10)
        self.assertEqual(self.copy_seqs(), {7, 8, 9, 10})
        self.commits(5)  # seq 11..15 in a new session
        self.assertEqual(self.copy_seqs(), {12, 13, 14, 15})

    def test_directory_does_not_grow_with_minting(self):
        with self.writer() as s:
            for i in range(12):
                s.put(f"k{i}", b"v")
                s.commit()
                s.scan().token()
        self.assertEqual(self.copy_seqs(), {9, 10, 11, 12})
        # Repeated opens neither create nor delete past the stable set.
        for _ in range(3):
            with self.writer():
                pass
        self.assertEqual(self.copy_seqs(), {9, 10, 11, 12})

    def test_compaction_also_reclaims(self):
        self.commits(10)
        with self.writer() as s:
            s.compact()
            # One compacted base at seq 10 plus the retention window.
            self.assertEqual(self.copy_seqs(), {7, 8, 9, 10})
            for i in range(4):
                s.put(f"q{i}", b"q")
                s.commit()
            s.compact()
        self.assertEqual(self.copy_seqs(), {11, 12, 13, 14})

    def test_reclaim_converges_on_plain_open(self):
        self.commits(10)
        settled = self.copy_seqs()
        # Plain reopen triggers a sweep but changes nothing stable.
        for _ in range(3):
            with self.writer():
                pass
        self.assertEqual(self.copy_seqs(), settled)


class InUsePinTest(ReclaimBase):
    def test_open_cursor_pins_across_commits(self):
        self.commits(5)
        with self.writer() as s:
            cursor = s.scan()
            token = cursor.token()
            items = list(cursor)
            for i in range(6):
                s.put(f"z{i}", b"z")
                s.commit()  # seq 11; gen 5 is far outside the window
            self.assertIn(5, self.copy_seqs())
            with self.reader() as r:
                self.assertEqual(list(r.scan(token=token)), items)
        cursor.close()
        gc.collect()
        with self.writer():
            pass
        self.assertNotIn(5, self.copy_seqs())

    def test_unminted_open_cursor_is_pinned_too(self):
        self.commits(5)
        with self.writer() as s:
            cursor = s.scan()
            next(cursor)
            for i in range(6):
                s.put(f"z{i}", b"z")
                s.commit()
            self.assertIn(5, self.copy_seqs())
            cursor.close()
        with self.writer():
            pass
        self.assertNotIn(5, self.copy_seqs())

    def test_live_reader_pins_its_snapshot(self):
        self.commits(3)
        r = self.reader()
        self.assertEqual(r.stats()["seq"], 3)
        with self.writer() as s:
            for i in range(6):
                s.put(f"z{i}", b"z")
                s.commit()
        self.assertIn(3, self.copy_seqs())
        self.assertEqual(r.get("k00"), b"v")
        self.assertIsNone(r.get("z0"))
        r.close()
        with self.writer():
            pass
        self.assertNotIn(3, self.copy_seqs())

    def test_dropped_cursor_releases_pin_on_gc(self):
        self.commits(5)
        with self.writer() as s:
            cur = s.scan()
            next(cur)
        del cur
        gc.collect()
        with self.writer() as s:
            for i in range(6):
                s.put(f"z{i}", b"z")
                s.commit()
        self.assertNotIn(5, self.copy_seqs())

    def test_resumed_cursor_pins_its_snapshot(self):
        self.commits(6)
        with self.writer() as s:
            token = s.scan().token()
        with self.reader() as r:
            resumed = r.scan(token=token)
            next(resumed)
            with self.writer() as s:
                for i in range(6):
                    s.put(f"z{i}", b"z")
                    s.commit()
            self.assertIn(6, self.copy_seqs())
            resumed.close()
        with self.writer():
            pass
        self.assertNotIn(6, self.copy_seqs())


class TokenSurvivalTest(ReclaimBase):
    def test_token_resolves_from_log_after_copy_deleted(self):
        with self.writer() as s:
            for i in range(5):
                s.put(f"k{i}", b"v")
                s.commit()
            token = s.scan().token()  # seq 5
        # Push seq 5 out of the retention window, but the prefix ending at
        # its commit is still part of the uncompacted log.
        self.commits(4)
        self.assertEqual(self.copy_seqs(), {6, 7, 8, 9})
        with self.reader() as r:
            self.assertEqual(
                list(r.scan(token=token)),
                [(f"k{i}", b"v") for i in range(5)])

    def test_token_fails_when_every_source_is_gone(self):
        with self.writer() as s:
            s.put("gone", b"g")
            s.commit()
            token = s.scan().token()  # seq 1, one committed key
        self.commits(5)
        with self.writer() as s:
            s.compact()
        for name in list(os.listdir(self.dir)):
            if name.startswith("wal.s1."):
                os.unlink(os.path.join(self.dir, name))
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=token)
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=token)

    def test_reclaim_does_not_change_served_reads(self):
        with self.writer() as s:
            for i in range(6):
                s.put(f"k{i}", b"v")
                s.commit()
            cursor = s.scan("k2", "k5")
            head = next(cursor)
            for i in range(6):
                s.put(f"z{i}", b"z")
                s.delete_range("k0", "k9")
                s.commit()
                s.compact()
            self.assertEqual(head, ("k2", b"v"))
            self.assertEqual(list(cursor),
                             [("k3", b"v"), ("k4", b"v")])


class StrictTokenTypeTest(ReclaimBase):
    def test_only_bytes_accepted(self):
        with self.writer() as s:
            token = s.scan().token()
        ba = bytearray(token)
        mv = memoryview(token)
        with self.reader() as r:
            for bad in (ba, mv, token.decode("latin1"), 1, [token]):
                with self.assertRaises(TypeError):
                    r.scan(token=bad)
            # The genuine bytes still works.
            self.assertEqual(list(r.scan(token=token)), [])


class KillSafeReclaimTest(ReclaimBase):
    # Crash on the nth snapshot-copy unlink while merely *opening* the
    # store (open finishes a reclamation sweep), so a kill can land in the
    # middle of the unlink sequence rather than just before/after it.
    CRASH = (
        "import os,sys\n"
        "sys.path.insert(0,%r)\n"
        "real=os.unlink; c=[0]\n"
        "def w(*a):\n"
        " if 'wal.s' in a[0]:\n"
        "  c[0]+=1\n"
        "  if c[0]==int(sys.argv[2]): os._exit(9)\n"
        " return real(*a)\n"
        "os.unlink=w\n"
        "from wal_store import Store\n"
        "Store(sys.argv[1]).close()\n"
    ) % REPO_ROOT

    def test_kill_at_every_reclaim_unlink_converges(self):
        import shutil
        self.commits(10)
        # Six extra old-generation copies, all outside the retention
        # window; the opening sweep deletes exactly these.
        fake = "ab" * 32
        for seq in range(1, 7):
            with open(os.path.join(self.dir, f"wal.s{seq}.{fake}"),
                      "wb") as f:
                f.write(b"stale")
        for point in range(1, 8):
            work = os.path.join(self._tmp.name, f"k{point}")
            shutil.copytree(self.dir, work)
            proc = subprocess.run(
                [sys.executable, "-c", self.CRASH, work, str(point)])
            self.assertIn(proc.returncode, (0, 9))
            # Finish the sweep and move one commit forward.
            with Store(work) as s:
                s.put("x", b"x")
                self.assertEqual(s.commit(), 11)
            settled = set(os.listdir(work))
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], 11)
                self.assertEqual(s.get("x"), b"x")
            self.assertEqual(set(os.listdir(work)), settled)
            seqs = {int(n.split(".")[1][1:]) for n in settled
                    if n.startswith("wal.s")}
            self.assertEqual(seqs, {8, 9, 10, 11})
            shutil.rmtree(work)


if __name__ == "__main__":
    unittest.main()
