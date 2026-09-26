"""Content verification of historical snapshot copies.

A published ``wal.s<seq>.<id>`` copy's name carries the snapshot's content
identity, and the name is never taken on faith. These tests cover:

* a copy whose content does not match the identity in its name being
  treated as damaged -- never trusted, never guessed, never repaired in
  place -- at writer open, at token resume and before reclamation;
* reads and resumed scans rebuilding the pinned snapshot from the
  committed log prefix or the checkpoint instead, byte-for-byte identical
  to resolving through an intact copy, with deleted keys never reviving;
* the writer reclaiming damaged copies like any dead copy and atomically
  replacing the current snapshot's damaged copy when republishing;
* a token raising ``ValueError`` only when the copy, the log prefix and
  the checkpoint can none of them rebuild its snapshot;
* writer-side cursors and resume sessions registering the same
  cross-process lease a read-only cursor registers, released on close.
"""

import gc
import os
import tempfile
import unittest

from wal_store import Store
from wal_store.store import _LEASE_PREFIX


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

    def copies(self, seq):
        return [n for n in os.listdir(self.dir)
                if n.startswith(f"wal.s{seq}.")]

    def damage(self, seq):
        """Flip a byte inside the seq copy's content, keeping its name."""
        path = os.path.join(self.dir, self.copies(seq)[0])
        with open(path, "r+b") as f:
            raw = bytearray(f.read())
            raw[-3] ^= 0xFF
            f.seek(0)
            f.write(raw)
        return path

    def lease_names(self):
        return sorted(n for n in os.listdir(self.dir)
                      if n.startswith(_LEASE_PREFIX))


class DamagedCopyResumeTest(VerifyBase):
    def test_damaged_copy_resume_rebuilds_identically(self):
        with self.writer() as s:
            for i in range(5):
                s.put(f"k{i}", f"v{i}".encode())
                s.commit()
            token = s.scan().token()  # seq 5
            expected = list(s.scan(token=token))
        # Push seq 5 out of the retention window; its copy stays on disk
        # only if retained, so damage whatever copy the token names.
        self.damage(5)
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=token)), expected)
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=token)), expected)

    def test_damaged_copy_does_not_change_any_read(self):
        with self.writer() as s:
            for i in range(3):
                s.put(f"k{i}", f"v{i}".encode())
                s.commit()
            s.delete("k1")
            s.commit()  # seq 4: k1 is gone
            token = s.scan().token()
        self.damage(4)
        with self.writer() as s:
            # The committed state and every read are exactly as before.
            self.assertIsNone(s.get("k1"))
            self.assertEqual(s.get("k0"), b"v0")
            self.assertEqual(list(s.scan(token=token)),
                             [("k0", b"v0"), ("k2", b"v2")])

    def test_foreign_content_in_copy_is_never_served(self):
        # A validly framed copy of a *different* snapshot under this name
        # fails the identity check just like a torn one.
        with self.writer() as s:
            s.put("real", b"r")
            s.commit()
            token = s.scan().token()  # seq 1
        other = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(other))
        with Store(other) as s:
            s.put("fake", b"f")
            s.commit()
        foreign = [n for n in os.listdir(other) if n.startswith("wal.s1.")]
        self.assertTrue(foreign)
        target = os.path.join(self.dir, self.copies(1)[0])
        with open(os.path.join(other, foreign[0]), "rb") as f:
            blob = f.read()
        with open(target, "wb") as f:
            f.write(blob)
        with self.writer() as s:
            # The rebuilt snapshot is the real one; "fake" never appears.
            self.assertEqual(list(s.scan(token=token)), [("real", b"r")])
            self.assertIsNone(s.get("fake"))

    def test_damaged_copy_and_no_source_raises_valueerror(self):
        with self.writer() as s:
            for i in range(6):
                s.put(f"k{i}", b"v")
                s.commit()
                if i == 3:
                    token = s.scan().token()  # seq 4
            s.compact()  # the log now rebuilds only the seq-6 snapshot
        self.assertTrue(self.copies(4))  # retained generation
        self.damage(4)
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=token)
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=token)


class DamagedCopyReclaimTest(VerifyBase):
    def test_damaged_copy_is_reclaimed_at_open(self):
        with self.writer() as s:
            for i in range(4):
                s.put(f"k{i}", b"v")
                s.commit()
        self.damage(3)  # an old but retained generation
        with self.writer():
            pass
        self.assertEqual(self.copies(3), [])
        # Intact copies are untouched.
        self.assertTrue(self.copies(4))
        self.assertTrue(self.copies(2))

    def test_damaged_current_copy_is_replaced_at_open(self):
        with self.writer() as s:
            for i in range(3):
                s.put(f"k{i}", f"v{i}".encode())
                s.commit()
            token = s.scan().token()  # seq 3, the current snapshot
        name = self.copies(3)[0]
        self.damage(3)
        with self.writer():
            pass
        # Same content-addressed name, now with the correct content.
        self.assertEqual(self.copies(3), [name])
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=token)),
                             [(f"k{i}", f"v{i}".encode()) for i in range(3)])

    def test_damaged_copy_reclaimed_after_commit(self):
        with self.writer() as s:
            for i in range(4):
                s.put(f"k{i}", b"v")
                s.commit()
        self.damage(2)
        with self.writer() as s:
            s.put("x", b"x")
            s.commit()
        self.assertEqual(self.copies(2), [])

    def test_committed_state_untouched_by_damaged_copies(self):
        with self.writer() as s:
            for i in range(4):
                s.put(f"k{i}", b"v")
                s.commit()
        for seq in (2, 3, 4):
            self.damage(seq)
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 4)
            for i in range(4):
                self.assertEqual(s.get(f"k{i}"), b"v")
        # Reopening converges: damaged copies are gone, the log and the
        # checkpoint are byte-for-byte what a clean store holds.
        with self.writer() as s:
            self.assertEqual(s.stats()["seq"], 4)


class WriterLeaseTest(VerifyBase):
    def test_writer_cursor_registers_and_releases_a_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        s = self.writer()
        self.assertEqual(self.lease_names(), [])
        cursor = s.scan()
        self.assertEqual(len(self.lease_names()), 1)
        cursor.close()
        self.assertEqual(self.lease_names(), [])
        s.close()

    def test_writer_resume_session_registers_a_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            token = s.scan().token()
        s = self.writer()
        resumed = s.scan(token=token)
        self.assertEqual(len(self.lease_names()), 1)
        self.assertEqual(list(resumed), [("a", b"1")])
        resumed.close()
        self.assertEqual(self.lease_names(), [])
        s.close()

    def test_dropped_writer_cursor_releases_lease_on_gc(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        s = self.writer()
        cursor = s.scan()
        next(cursor)
        self.assertEqual(len(self.lease_names()), 1)
        del cursor
        gc.collect()
        self.assertEqual(self.lease_names(), [])
        s.close()

    def test_writer_store_alone_still_creates_no_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.compact()
        self.assertEqual(self.lease_names(), [])

    def test_empty_snapshot_writer_scan_takes_no_lease(self):
        s = self.writer()
        cursor = s.scan()
        self.assertEqual(self.lease_names(), [])
        cursor.close()
        s.close()


if __name__ == "__main__":
    unittest.main()
