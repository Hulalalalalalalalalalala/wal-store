"""Lifecycle management of published snapshot copies (``wal.s<seq>.<id>``).

These tests cover:

* the store directory not growing with the number of commits: copies that
  lose every reference and fall outside the retention window are deleted;
* the retention rule itself: the current committed snapshot and the newest
  three published generations are never reclaimed;
* in-use snapshots -- pinned by an open cursor or a read-only store --
  never being reclaimed, across commits, compaction and reopening;
* a token whose copy was reclaimed still resolving from the committed log
  prefix or the checkpoint, byte-for-byte, and raising ``ValueError`` only
  once every copy and every reconstruction path is gone;
* reclamation running after every commit and every compaction, and a pass
  killed at any point converging to the identical set of files on reopen,
  with the committed state, the durable sequence and the recovery report
  untouched;
* read-only stores never creating, modifying or deleting any file.
"""

import gc
import os
import tempfile
import unittest

from wal_store import Store
from wal_store.store import _parse_snapshot_name


def sidecars(directory):
    out = []
    for name in os.listdir(directory):
        if _parse_snapshot_name(name) is not None:
            out.append(name)
    return sorted(out)


def seqs(directory):
    return sorted(_parse_snapshot_name(name)[0]
                  for name in sidecars(directory))


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

    def commit_keys(self, store, *keys):
        for key in keys:
            store.put(key, key.encode())
        store.commit()


class RetentionTest(ReclaimBase):
    def test_directory_does_not_grow_with_commits(self):
        with self.writer() as s:
            for i in range(25):
                self.commit_keys(s, f"k{i:02d}")
                self.assertLessEqual(len(sidecars(self.dir)), 3,
                                     sidecars(self.dir))
        self.assertEqual(len(sidecars(self.dir)), 3)

    def test_current_and_newest_three_generations_are_kept(self):
        with self.writer() as s:
            for i in range(6):
                self.commit_keys(s, f"k{i}")
        self.assertEqual(seqs(self.dir), [4, 5, 6])
        # The current committed snapshot is among them and resolves.
        with self.writer() as s:
            tok = s.scan().token()
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [(f"k{i}", f"k{i}".encode()) for i in range(6)])

    def test_empty_commits_stay_within_the_window(self):
        with self.writer() as s:
            s.put("a", b"1")
            for _ in range(6):
                s.commit()
        self.assertEqual(len(sidecars(self.dir)), 3)
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 6)
            self.assertEqual(s.get("a"), b"1")

    def test_reclaim_runs_on_compact(self):
        with self.writer() as s:
            for i in range(8):
                self.commit_keys(s, f"k{i}")
            s.compact()
        self.assertEqual(len(sidecars(self.dir)), 3)

    def test_reopen_converges_to_the_same_files(self):
        with self.writer() as s:
            for i in range(8):
                self.commit_keys(s, f"k{i}")
        before = sidecars(self.dir)
        # Reopening runs the pass again; it is a no-op on a converged store.
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 8)
        self.assertEqual(sidecars(self.dir), before)

    def test_reclaim_leaves_state_sequence_and_report_untouched(self):
        with self.writer() as s:
            for i in range(9):
                self.commit_keys(s, f"k{i}")
            stats = s.stats()
            report = s.recover()
        self.assertEqual(report, {"applied": 9, "discarded": 0, "seq": 9})
        with self.writer() as s:
            self.assertEqual(s.stats(), stats)
            self.assertEqual(s.commit(), 10)  # sequence only advances
            self.assertEqual(list(s.scan()),
                             [(f"k{i}", f"k{i}".encode())
                              for i in range(9)])


class InUseProtectionTest(ReclaimBase):
    def test_open_cursor_snapshot_is_never_reclaimed(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b")
            cur = s.scan()
            next(cur)
            tok = cur.token()
            for i in range(8):
                self.commit_keys(s, f"k{i}")
            self.assertTrue(any(n.startswith("wal.s1.")
                                for n in sidecars(self.dir)))
            # The open cursor still reads its pinned snapshot.
            self.assertEqual(list(cur), [("b", b"b")])
            cur.close()
            del cur
            gc.collect()
            self.commit_keys(s, "z")
            self.assertFalse(any(n.startswith("wal.s1.")
                                 for n in sidecars(self.dir)))
        # The minted token's copy is gone; with the full log present it
        # still resolves from the committed prefix.
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [("b", b"b")])

    def test_unminted_open_cursor_counts_as_in_use(self):
        with self.writer() as s:
            self.commit_keys(s, "a")
            cur = s.scan()  # never minted, still open
            for i in range(6):
                self.commit_keys(s, f"k{i}")
            self.assertTrue(any(n.startswith("wal.s1.")
                                for n in sidecars(self.dir)))
            self.assertEqual(list(cur), [("a", b"a")])
            cur.close()

    def test_reader_pinned_snapshot_is_never_reclaimed(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b")
        r = self.reader()
        with self.writer() as s:
            for i in range(6):
                self.commit_keys(s, f"k{i}")
            self.assertTrue(any(n.startswith("wal.s1.")
                                for n in sidecars(self.dir)))
            # The reader still serves its pinned snapshot.
            self.assertEqual(r.get("a"), b"a")
            self.assertIsNone(r.get("k0"))
        r.close()
        del r
        gc.collect()
        with self.writer() as s:
            self.commit_keys(s, "z")
            self.assertFalse(any(n.startswith("wal.s1.")
                                 for n in sidecars(self.dir)))

    def test_in_use_snapshot_survives_compaction(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b")
            cur = s.scan()
            tok = cur.token()
            for i in range(5):
                self.commit_keys(s, f"k{i}")
            s.compact()
            self.assertTrue(any(n.startswith("wal.s1.")
                                for n in sidecars(self.dir)))
            cur.close()
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [("a", b"a"), ("b", b"b")])


class TokenAfterReclaimTest(ReclaimBase):
    def test_token_resolves_from_log_prefix_after_copy_reclaimed(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b", "c")
            cur = s.scan()
            next(cur)
            tok = cur.token()
            cur.close()
            del cur
            gc.collect()
            for i in range(5):
                self.commit_keys(s, f"k{i}")
        # The seq-1 copy was reclaimed, but the log still holds the full
        # committed history: the resumed stream is the exact tail.
        self.assertFalse(any(n.startswith("wal.s1.")
                             for n in sidecars(self.dir)))
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [("b", b"b"), ("c", b"c")])

    def test_token_dies_only_when_nothing_can_rebuild_it(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b", "c")
            cur = s.scan()
            next(cur)
            tok = cur.token()
            cur.close()
            del cur
            gc.collect()
            for i in range(5):
                self.commit_keys(s, f"k{i}")
            s.compact()
        # Every copy is gone and the compacted log starts at a base commit
        # past the pinned snapshot: resuming raises ValueError.
        self.assertFalse(any(n.startswith("wal.s1.")
                             for n in sidecars(self.dir)))
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=tok)
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok)

    def test_recent_token_survives_reclaim_and_compaction(self):
        with self.writer() as s:
            for i in range(4):
                self.commit_keys(s, f"k{i}")
            cur = s.scan()
            expected = list(cur)
            tok = s.scan().token()  # pins the current (seq 4) snapshot
            for i in range(4, 7):
                self.commit_keys(s, f"k{i}")
            s.compact()
            cur.close()
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), expected)

    def test_resumed_stream_is_byte_identical_across_reclaim(self):
        with self.writer() as s:
            self.commit_keys(s, "a", "b", "c", "d")
            full = list(s.scan())
            cur = s.scan()
            head = [next(cur), next(cur)]
            tok = cur.token()
            cur.close()
            del cur
            gc.collect()
            for i in range(4):
                self.commit_keys(s, f"k{i}")
        with self.reader() as r:
            self.assertEqual(head + list(r.scan(token=tok)), full)


class ReclaimKillSafetyTest(ReclaimBase):
    def test_interrupted_pass_converges_on_reopen(self):
        import shutil
        stash = os.path.join(self.dir, "stash")
        os.makedirs(stash)
        with self.writer() as s:
            for i in range(7):
                self.commit_keys(s, f"k{i}")
        # Save the about-to-be-reclaimed copies on the side.
        for name in os.listdir(self.dir):
            if name.startswith("wal.s"):
                shutil.copy(os.path.join(self.dir, name),
                            os.path.join(stash, name))
        with self.writer() as s:
            for i in range(3):
                self.commit_keys(s, f"n{i}")
            converged = sidecars(self.dir)
        # Simulate a pass killed before it finished: the old copies are
        # still on disk. Reopening finishes exactly their deletion.
        for name in os.listdir(stash):
            shutil.copy(os.path.join(stash, name),
                        os.path.join(self.dir, name))
        shutil.rmtree(stash)
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 10)
        self.assertEqual(sidecars(self.dir), converged)
        self.assertEqual(seqs(self.dir), [8, 9, 10])

    def test_stale_temp_copies_are_cleaned_on_open(self):
        with self.writer() as s:
            self.commit_keys(s, "a")
        tmp = os.path.join(self.dir, "wal.s1." + "0" * 64 + ".tmp")
        with open(tmp, "wb") as f:
            f.write(b"half-published")
        with self.writer() as s:
            pass
        self.assertFalse(os.path.exists(tmp))


class ReadOnlyNoReclaimTest(ReclaimBase):
    @staticmethod
    def _contents(directory):
        out = {}
        for name in os.listdir(directory):
            with open(os.path.join(directory, name), "rb") as f:
                out[name] = f.read()
        return out

    def test_reader_never_deletes_copies(self):
        with self.writer() as s:
            for i in range(6):
                self.commit_keys(s, f"k{i}")
        # Plant copies a writer pass would reclaim; a reader changes nothing.
        planted = os.path.join(self.dir, "wal.s1." + "zz" * 32)
        with open(planted, "wb") as f:
            f.write(b"stale")
        before = self._contents(self.dir)
        with self.reader() as r:
            r.get("k0")
            list(r.scan())
        self.assertEqual(before, self._contents(self.dir))
        # A writer pass leaves the unparseable name alone but reclaims
        # well-formed copies outside the window.
        with self.writer() as s:
            self.commit_keys(s, "z")
        self.assertTrue(os.path.exists(planted))
        self.assertEqual(len(sidecars(self.dir)), 3)


if __name__ == "__main__":
    unittest.main()
