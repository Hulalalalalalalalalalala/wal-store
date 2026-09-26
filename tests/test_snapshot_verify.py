"""Content verification of published snapshot copies.

A copy's name carries its content identity (``wal.s<seq>.<id>``), and every
use of a copy verifies the file's actual bytes against it: when the store
is opened, when a token is resumed and when the writer decides whether a
copy is usable. These tests cover:

* a damaged copy never being trusted as a source: reads and resumed scans
  rebuild the snapshot from the committed log prefix or the checkpoint,
  byte-for-byte identical to a resume served by a healthy copy, and no
  read key/value changes (deleted keys never resurrect);
* the writer reclaiming damaged copies in its ordinary sweep, and
  atomically replacing a damaged current copy on publish, without
  touching the committed content or the durable sequence;
* a token whose snapshot no copy, log prefix or checkpoint can rebuild
  raising ``ValueError``;
* verification converging across reopens to the same copy set.
"""

import gc
import os
import tempfile
import unittest

from wal_store import Store


class VerifyBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        gc.collect()
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def copies(self):
        return sorted(n for n in os.listdir(self.dir)
                      if n.startswith("wal.s"))

    def copy_for_seq(self, seq):
        prefix = "wal.s%d." % seq
        matches = [n for n in self.copies() if n.startswith(prefix)]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def damage(self, name):
        """Corrupt a copy on disk without touching its name."""
        path = os.path.join(self.dir, name)
        with open(path, "r+b") as f:
            data = bytearray(f.read())
            self.assertGreater(len(data), 20)
            # Flip bytes inside the framing and deep in the payload, so no
            # interpretation of the file can match the name's identity.
            data[3] ^= 0xFF
            data[-3] ^= 0xFF
            f.seek(0)
            f.write(bytes(data))
            f.truncate()
        return path


class DamagedCopyResumeTest(VerifyBase):
    def test_resume_rebuilds_from_log_byte_for_byte(self):
        with self.writer() as s:
            for i in range(5):
                s.put("k%d" % i, ("v%d" % i).encode())
            s.commit()
            tok = s.scan().token()  # seq 1
        self.damage(self.copy_for_seq(1))
        expected = [("k%d" % i, ("v%d" % i).encode()) for i in range(5)]
        with self.reader() as r:
            resumed = r.scan(token=tok)
            # Re-minting at the same position yields the identical token.
            self.assertEqual(resumed.token(), tok)
            self.assertEqual(list(resumed), expected)
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), expected)

    def test_damaged_copy_changes_no_read_and_revives_nothing(self):
        with self.writer() as s:
            s.put("gone", b"g")
            s.put("keep", b"k")
            s.commit()
            s.delete("gone")
            s.commit()  # seq 2, current copy
        self.damage(self.copy_for_seq(2))
        with self.writer() as s:
            self.assertIsNone(s.get("gone"))
            self.assertEqual(s.get("keep"), b"k")
            self.assertEqual(list(s.scan()), [("keep", b"k")])
        with self.reader() as r:
            self.assertIsNone(r.get("gone"))
            self.assertEqual(list(r.scan()), [("keep", b"k")])

    def test_open_cursor_unaffected_by_damaged_copy(self):
        with self.writer() as s:
            for i in range(4):
                s.put("k%d" % i, b"v")
            s.commit()
            cur = s.scan()
            head = next(cur)
            # Damage the very copy the cursor's snapshot was published as;
            # the cursor serves its materialised snapshot regardless.
            self.damage(self.copy_for_seq(1))
            for i in range(6):
                s.put("z%d" % i, b"z")
                s.commit()
            self.assertEqual(head, ("k0", b"v"))
            self.assertEqual(list(cur),
                             [("k%d" % i, b"v") for i in range(1, 4)])
            cur.close()

    def test_old_snapshot_rebuilt_from_log_prefix(self):
        # The token's snapshot is no longer the current one and its copy is
        # damaged: the resume rebuilds it from the committed log prefix,
        # byte-for-byte the tail a healthy copy would have served.
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            cur = s.scan()
            self.assertEqual(next(cur), ("a", b"1"))
            tok = cur.token()  # seq 1, position 1
        with self.writer() as s:
            s.put("c", b"3")
            s.commit()
            s.put("d", b"4")
            s.commit()  # seq 3; the log prefix still reaches back to seq 1
        self.damage(self.copy_for_seq(1))
        with self.reader() as r:
            self.assertEqual(r.stats()["seq"], 3)
            self.assertEqual(list(r.scan(token=tok)), [("b", b"2")])
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), [("b", b"2")])

    def test_token_fails_when_no_source_can_rebuild(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            tok = s.scan().token()  # seq 1
        self.damage(self.copy_for_seq(1))
        with self.writer() as s:
            for i in range(5):
                s.put("k%02d" % i, b"v")
                s.commit()
            s.compact()  # the log prefix can no longer rebuild seq 1
        # The damaged copy was reclaimed in the sweep and neither the log
        # prefix nor the checkpoint reaches back to seq 1.
        self.assertFalse(any(n.startswith("wal.s1.") for n in self.copies()))
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=tok)
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=tok)


class DamagedCopyReclaimTest(VerifyBase):
    def test_damaged_retained_copy_is_reclaimed_on_open(self):
        with self.writer() as s:
            for i in range(3):
                s.put("k%d" % i, b"v")
                s.commit()
        victim = self.copy_for_seq(2)  # inside the retention window
        self.damage(victim)
        with self.writer() as s:
            # The damaged copy is gone; the healthy generations survive, the
            # durable sequence did not move and the content is intact.
            self.assertNotIn(victim, self.copies())
            self.assertEqual({n.split(".")[1] for n in self.copies()},
                             {"s1", "s3"})
            self.assertEqual(s.stats()["seq"], 3)
            self.assertEqual(s.get("k1"), b"v")
        # Reopening converges to the same file set.
        settled = sorted(os.listdir(self.dir))
        with self.writer():
            pass
        self.assertEqual(sorted(os.listdir(self.dir)), settled)

    def test_damaged_current_copy_is_replaced_on_publish(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        name = self.copy_for_seq(1)
        with open(os.path.join(self.dir, name), "rb") as f:
            healthy = f.read()
        self.damage(name)
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 1)
        # The current snapshot's copy was atomically re-published with the
        # authoritative bytes, not repaired in place and not removed.
        with open(os.path.join(self.dir, name), "rb") as f:
            self.assertEqual(f.read(), healthy)

    def test_damaged_copy_reclaimed_on_commit_and_compaction(self):
        with self.writer() as s:
            for i in range(3):
                s.put("k%d" % i, b"v")
                s.commit()
        victim = self.copy_for_seq(1)
        self.damage(victim)
        with self.writer() as s:
            s.put("x", b"x")
            s.commit()
            self.assertNotIn(victim, self.copies())
        victim = self.copy_for_seq(2)
        self.damage(victim)
        with self.writer() as s:
            s.compact()
            self.assertNotIn(victim, self.copies())
            self.assertEqual(s.stats()["seq"], 4)

    def test_damaged_copy_never_repaired_in_place(self):
        # A damaged non-current copy is removed, never rewritten: after the
        # sweep its name is simply absent from the directory.
        with self.writer() as s:
            for i in range(2):
                s.put("k%d" % i, b"v")
                s.commit()
        victim = self.copy_for_seq(1)
        path = self.damage(victim)
        with self.writer():
            pass
        self.assertFalse(os.path.exists(path))

    def test_verification_ignores_non_copy_names(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        decoys = ["wal.s", "wal.sx", "wal.s1.zzz", "wal.s1.", "wal.s0." + "ab" * 32]
        for n in decoys:
            with open(os.path.join(self.dir, n), "wb") as f:
                f.write(b"junk")
        with self.writer():
            pass
        for n in decoys:
            self.assertIn(n, os.listdir(self.dir))


if __name__ == "__main__":
    unittest.main()
