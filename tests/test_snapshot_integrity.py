"""Content-integrity verification of published snapshot copies.

A ``wal.s<seq>.<id>`` copy's name carries the content identity of the
snapshot it holds. These tests cover the verification layer on top of the
resumable-scan machinery:

* a copy whose content does not match the identity in its name is damaged:
  its bytes are never served -- the snapshot is rebuilt from the committed
  log prefix or the checkpoint instead, byte-identical to a one-shot scan;
* a damaged copy is swept by reclamation whatever the retention window
  says, and a fresh publish atomically replaces it with the correct image;
* when the copy, the log prefix and the checkpoint can none of them
  rebuild the snapshot, resuming the old token raises ``ValueError``;
* writer-side cursors and resume sessions register cross-process leases
  exactly like read-only ones, and a lease whose owner process has exited
  expires without waiting out the heartbeat TTL.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from wal_store import Store
from wal_store.store import _LEASE_PREFIX

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class IntegrityBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def copies(self):
        return sorted(n for n in os.listdir(self.dir)
                      if n.startswith("wal.s") and not n.endswith(".tmp"))

    def copy_of(self, seq):
        matches = [n for n in self.copies()
                   if n.startswith("wal.s%d." % seq)]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def damage(self, name):
        """Corrupt a copy's content while keeping its name."""
        path = os.path.join(self.dir, name)
        with open(path, "rb") as f:
            raw = bytearray(f.read())
        raw[len(raw) // 2] ^= 0xFF
        raw[-1] ^= 0xFF
        with open(path, "wb") as f:
            f.write(raw)

    def lease_names(self):
        return sorted(n for n in os.listdir(self.dir)
                      if n.startswith(_LEASE_PREFIX))


class DamagedCopyResolutionTest(IntegrityBase):
    def seed_with_token(self):
        """Commits 1..5, a token on the seq-5 snapshot, then commits 6,7."""
        with self.writer() as s:
            for i in range(5):
                s.put("k%d" % i, b"v%d" % i)
                s.commit()
            cur = s.scan()
            next(cur)
            token = cur.token()
            tail = list(cur)
            for i in range(5, 7):
                s.put("k%d" % i, b"v%d" % i)
                s.commit()
        return token, tail

    def test_damaged_copy_resume_rebuilds_from_log(self):
        token, tail = self.seed_with_token()
        self.damage(self.copy_of(5))
        # The copy is damaged, but the committed log prefix still rebuilds
        # the pinned snapshot: the resume is the exact one-shot tail.
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=token)), tail)
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=token)), tail)

    def test_mismatched_bytes_are_never_served(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            token = s.scan().token()  # seq 1: [("a", b"1")]
            s.put("b", b"2")
            s.commit()
        # Replace the seq-1 copy's content with the seq-2 copy's bytes:
        # a well-formed blob of the *wrong* snapshot under the seq-1 name.
        first = self.copy_of(1)
        second = self.copy_of(2)
        with open(os.path.join(self.dir, second), "rb") as f:
            wrong = f.read()
        with open(os.path.join(self.dir, first), "wb") as f:
            f.write(wrong)
        with self.reader() as r:
            # Never the foreign bytes: the pinned seq-1 snapshot is rebuilt
            # from the log prefix instead.
            self.assertEqual(list(r.scan(token=token)), [("a", b"1")])

    def test_token_fails_when_copy_damaged_and_no_source(self):
        with self.writer() as s:
            s.put("gone", b"g")
            s.commit()
            token = s.scan().token()  # seq 1
            s.put("b", b"2")
            s.commit()
            s.put("c", b"3")
            s.commit()
            s.compact()  # log is now one base marker at seq 3
        # The seq-1 copy survives inside the retention window but is
        # damaged, and neither the log prefix nor the checkpoint can
        # rebuild seq 1 any more: the token must be rejected.
        self.damage(self.copy_of(1))
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan(token=token)
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan(token=token)


class DamagedCopyLifecycleTest(IntegrityBase):
    def test_damaged_copy_is_swept_inside_retention_window(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.commit()
        damaged = self.copy_of(1)
        self.damage(damaged)
        with self.writer():
            pass
        self.assertNotIn(damaged, self.copies())
        # The genuine current copy survives the same sweep.
        self.assertEqual(len(self.copies()), 1)

    def test_damaged_current_copy_is_republished_on_open(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
        name = self.copy_of(1)
        with open(os.path.join(self.dir, name), "rb") as f:
            genuine = f.read()
        self.damage(name)
        with self.writer():
            pass
        with open(os.path.join(self.dir, name), "rb") as f:
            self.assertEqual(f.read(), genuine)

    def test_rebuild_result_matches_one_shot_scan_byte_for_byte(self):
        with self.writer() as s:
            for i in range(6):
                s.put("k%02d" % i, bytes([i]) * (i + 1))
                s.commit()
            one_shot = list(s.scan("k01", "k05"))
            cur = s.scan("k01", "k05")
            next(cur)
            token = cur.token()
            s.put("z", b"z")
            s.commit()
        self.damage(self.copy_of(6))
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=token)), one_shot[1:])


class WriterLeaseTest(IntegrityBase):
    def test_writer_cursor_registers_a_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            self.assertEqual(self.lease_names(), [])
            cur = s.scan()
            next(cur)
            leases = self.lease_names()
            self.assertEqual(len(leases), 1)
            cur.close()
            self.assertEqual(self.lease_names(), [])

    def test_writer_resume_session_registers_a_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            token = s.scan().token()
        with self.writer() as s:
            resumed = s.scan(token=token)
            self.assertEqual(len(self.lease_names()), 1)
            resumed.close()
            self.assertEqual(self.lease_names(), [])

    def test_writer_without_cursors_creates_no_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.compact()
        self.assertEqual(self.lease_names(), [])


class CrossProcessWriterLeaseTest(IntegrityBase):
    # A writer in another process holds a cursor over the seq-2 snapshot,
    # then closes its store: the cursor's lease outlives the store and
    # keeps the pinned copy alive across this writer's commits.
    HOLD = r"""
import sys, time
sys.path.insert(0, %r)
from wal_store import Store
s = Store(sys.argv[1])
cur = s.scan()
next(cur, None)
s.close()
print("ready", flush=True)
time.sleep(float(sys.argv[2]))
""" % REPO_ROOT

    def test_writer_cursor_lease_pins_across_processes(self):
        with self.writer() as s:
            for i in range(2):
                s.put("k%d" % i, b"v")
                s.commit()
        holder = subprocess.Popen(
            [sys.executable, "-c", self.HOLD, self.dir, "8"],
            stdout=subprocess.PIPE)
        try:
            self.assertEqual(holder.stdout.readline().strip(), b"ready")
            holder.stdout.close()
            time.sleep(0.4)
            with self.writer() as s:
                for i in range(6):
                    s.put("z%d" % i, b"z")
                    s.commit()  # seq 8: seq 2 far outside the window
            with self.writer():
                pass
            self.assertTrue(
                any(n.startswith("wal.s2.") for n in os.listdir(self.dir)))
        finally:
            holder.send_signal(signal.SIGKILL)
            holder.wait()
        # The owner process is gone: its lease expires without waiting out
        # the TTL and the copy is reclaimed.
        deadline = time.time() + 6
        while time.time() < deadline:
            with self.writer():
                pass
            if not any(n.startswith("wal.s2.")
                       for n in os.listdir(self.dir)):
                break
            time.sleep(0.1)
        self.assertFalse(
            any(n.startswith("wal.s2.") for n in os.listdir(self.dir)))
        self.assertEqual(self.lease_names(), [])


if __name__ == "__main__":
    unittest.main()
