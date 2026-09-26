"""Cross-process lease registration for historical snapshot copies.

These tests cover the lease layer that lifts the in-use decision out of one
process:

* a read-only store, one of its cursors and a resumed scan registering a
  lease on open, sharing one short-lived ``wal.lease.*`` sidecar per
  process and directory, and removing it on close (or on garbage
  collection when dropped without ``close()``);
* a writer's cursor and resume session registering leases with the same
  scope as a read-only one, protecting the pinned snapshot from a writer
  in another process;
* a snapshot pinned by a reader/cursor in *another process* surviving a
  writer's commits, compactions and reopens, then being reclaimed once the
  holder exits cleanly or is killed -- expiry detected by the stale
  heartbeat or immediately by the owner process no longer existing;
* the writer re-reading the leases before every unlink and never removing
  a copy a fresh lease names, while a stale, malformed or foreign lease
  neither protects anything nor breaks the sweep;
* files whose name is not a legal lease sidecar being ignored entirely --
  never parsed, never deleted;
* a reader in a read-only directory working with no lease and no error;
* a writer itself never creating a lease sidecar without a cursor.
"""

import base64
import gc
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from wal_store import Store
from wal_store.store import (
    _LEASE_PREFIX,
    _LEASE_REC_LEN,
    _LEASE_TTL,
    _LeaseManager,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _lease_names(directory):
    return sorted(n for n in os.listdir(directory)
                  if n.startswith(_LEASE_PREFIX))


# A reader that resumes an old token and then stays alive: it pins the
# token's snapshot across processes while it sleeps.
HOLD = r"""
import sys, time, base64
sys.path.insert(0, %r)
from wal_store import Store
r = Store(sys.argv[1], read_only=True)
cur = r.scan(token=base64.b64decode(sys.argv[2]))
print(repr(list(cur)), flush=True)
time.sleep(float(sys.argv[3]))
""" % REPO_ROOT

# The same holder, but it returns immediately so its atexit cleanup runs.
HOLD_EXIT = r"""
import sys, base64
sys.path.insert(0, %r)
from wal_store import Store
r = Store(sys.argv[1], read_only=True)
cur = r.scan(token=base64.b64decode(sys.argv[2]))
print(repr(list(cur)), flush=True)
""" % REPO_ROOT


class LeaseBase(unittest.TestCase):
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

    def seed_old_token(self):
        """Commit once, mint a token at seq 1, leaving the copy published."""
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            return s.scan().token()

    def advance_past_retention(self):
        with self.writer() as s:
            for i in range(6):
                s.put("k%02d" % i, b"v")
                s.commit()  # seq 2..7: seq 1 falls out of the window

    def old_copy_present(self):
        return any(n.startswith("wal.s1.") for n in os.listdir(self.dir))

    def spawn_holder(self, token, duration=8.0):
        p = subprocess.Popen(
            [sys.executable, "-c", HOLD, self.dir,
             base64.b64encode(token).decode(), str(duration)],
            stdout=subprocess.PIPE)
        self.assertEqual(p.stdout.readline().strip(), b"[('old', b'o')]")
        p.stdout.close()
        return p

    def _write_fake_lease(self, seq, sid, ts, pid=999999, raw=None):
        name = "%s%d.%s" % (_LEASE_PREFIX, pid, "ab" * 16)
        path = os.path.join(self.dir, name)
        if raw is None:
            raw = _LeaseManager._record(seq, sid, ts)
        with open(path, "wb") as f:
            f.write(raw)
        return name


class LeaseFileLifecycleTest(LeaseBase):
    def test_reader_creates_one_lease_and_close_removes_it(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        before = set(os.listdir(self.dir))
        r = self.reader()
        leases = _lease_names(self.dir)
        self.assertEqual(len(leases), 1)
        # No snapshot copy or other durable file appears from reading.
        self.assertEqual(set(os.listdir(self.dir)) - before, set(leases))
        r.close()
        self.assertEqual(_lease_names(self.dir), [])
        self.assertEqual(set(os.listdir(self.dir)), before)

    def test_records_are_fixed_width(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r = self.reader()
        name = _lease_names(self.dir)[0]
        with open(os.path.join(self.dir, name), "rb") as f:
            raw = f.read()
        self.assertEqual(len(raw), _LEASE_REC_LEN)
        self.assertTrue(raw.endswith(b"\n"))
        r.close()

    def test_two_readers_share_one_sidecar_then_both_close(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r1 = self.reader()
        r2 = self.reader()
        leases = _lease_names(self.dir)
        # Same process, same directory, same pinned snapshot: one sidecar.
        self.assertEqual(len(leases), 1)
        raw = None
        with open(os.path.join(self.dir, leases[0]), "rb") as f:
            raw = f.read()
        self.assertEqual(len(raw), _LEASE_REC_LEN)
        r1.close()
        self.assertEqual(len(_lease_names(self.dir)), 1)
        r2.close()
        self.assertEqual(_lease_names(self.dir), [])

    def test_dropped_reader_releases_lease_on_gc(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r = self.reader()
        self.assertEqual(len(_lease_names(self.dir)), 1)
        del r
        gc.collect()
        self.assertEqual(_lease_names(self.dir), [])

    def test_dropped_cursor_releases_its_lease_on_gc(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r = self.reader()
        cur = r.scan()
        next(cur)
        del cur
        gc.collect()
        # The reader's own open-time lease still stands.
        self.assertEqual(len(_lease_names(self.dir)), 1)
        r.close()
        self.assertEqual(_lease_names(self.dir), [])

    def test_writer_never_creates_a_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            for i in range(3):
                s.put("k%d" % i, b"v")
                s.commit()
            s.compact()
        with self.writer():
            pass
        self.assertEqual(_lease_names(self.dir), [])


class CrossProcessLeaseTest(LeaseBase):
    def test_live_holder_in_other_process_pins_old_copy(self):
        token = self.seed_old_token()
        p = self.spawn_holder(token)
        try:
            time.sleep(0.4)
            self.advance_past_retention()
            # Reopen the writer to finish any reclamation; the copy pinned by
            # the reader process must still be there.
            with self.writer():
                pass
            self.assertTrue(self.old_copy_present())
        finally:
            p.send_signal(signal.SIGKILL)
            p.wait()

    def test_cleanly_exited_holder_releases_immediately(self):
        token = self.seed_old_token()
        done = subprocess.run(
            [sys.executable, "-c", HOLD_EXIT, self.dir,
             base64.b64encode(token).decode()],
            capture_output=True, timeout=20)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.strip(), b"[('old', b'o')]")
        # Normal process exit ran the atexit cleanup: no lease remains.
        self.assertEqual(_lease_names(self.dir), [])
        self.advance_past_retention()
        self.assertFalse(self.old_copy_present())

    def test_killed_holder_expires_after_ttl(self):
        token = self.seed_old_token()
        p = self.spawn_holder(token)
        try:
            time.sleep(0.4)
            self.advance_past_retention()
            with self.writer():
                pass
            self.assertTrue(self.old_copy_present())
        finally:
            p.send_signal(signal.SIGKILL)
            p.wait()
        # The dead process stops heartbeating; after the TTL its lease is
        # stale and the copy is reclaimed, the stale sidecar swept.
        deadline = time.time() + _LEASE_TTL + 6
        while time.time() < deadline:
            if not _lease_names(self.dir) and not self.old_copy_present():
                break
            time.sleep(0.2)
            with self.writer():
                pass
        self.assertEqual(_lease_names(self.dir), [])
        self.assertFalse(self.old_copy_present())

    def test_expired_lease_is_ignored_using_short_ttl(self):
        # Exercise the expiry path without a real TTL wait: a lease whose
        # heartbeat is old pins nothing even though its file is present.
        import wal_store.store as mod
        token = self.seed_old_token()
        p = self.spawn_holder(token)
        p.send_signal(signal.SIGKILL)
        p.wait()
        time.sleep(0.3)  # let the heartbeat file settle post-kill
        old_ttl = mod._LEASE_TTL
        try:
            mod._LEASE_TTL = 0.05
            time.sleep(0.15)
            self.advance_past_retention()
            self.assertFalse(self.old_copy_present())
            self.assertEqual(_lease_names(self.dir), [])
        finally:
            mod._LEASE_TTL = old_ttl


class LeaseContentTest(LeaseBase):
    def test_fresh_foreign_lease_protects_named_copy(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            # The seq 1 copy and its identity.
            copy = [n for n in os.listdir(self.dir) if n.startswith("wal.s1.")][0]
            sid = bytes.fromhex(copy.split(".")[2])
        # A live owner's fresh lease (this process's pid) protects the copy.
        self._write_fake_lease(1, sid, time.time(), pid=os.getpid())
        self.advance_past_retention()
        self.assertTrue(self.old_copy_present())

    def test_stale_foreign_lease_is_swept_and_copy_reclaimed(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            copy = [n for n in os.listdir(self.dir) if n.startswith("wal.s1.")][0]
            sid = bytes.fromhex(copy.split(".")[2])
        self._write_fake_lease(1, sid, time.time() - _LEASE_TTL - 10)
        self.advance_past_retention()
        self.assertFalse(self.old_copy_present())
        self.assertEqual(_lease_names(self.dir), [])

    def test_lease_naming_other_snapshot_does_not_protect(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
        # A live lease for some other identity never matches the seq-1 copy.
        self._write_fake_lease(1, b"\x11" * 32, time.time())
        self.advance_past_retention()
        self.assertFalse(self.old_copy_present())

    def test_malformed_lease_sidecar_is_ignored_and_swept(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
        name = self._write_fake_lease(0, b"", 0.0, raw=b"this is not a lease\xff")
        self.advance_past_retention()
        # Garbage in a well-named file pins nothing and is swept; the writer
        # never raised.
        self.assertNotIn(name, os.listdir(self.dir))
        self.assertFalse(self.old_copy_present())

    def test_non_lease_names_are_never_touched(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
        decoys = ["wal.lease", "wal.lease.bak", "wal.lease.12.nothex",
                  "wal.leasex", "random.txt"]
        for n in decoys:
            with open(os.path.join(self.dir, n), "wb") as f:
                f.write(b"junk")
        self.advance_past_retention()
        for n in decoys:
            self.assertTrue(os.path.exists(os.path.join(self.dir, n)), n)


class WriterLeaseTest(LeaseBase):
    """Writer-side cursors and resume sessions register leases too."""
    def test_writer_cursor_registers_and_releases_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            self.assertEqual(_lease_names(self.dir), [])
            cur = s.scan()
            self.assertEqual(len(_lease_names(self.dir)), 1)
            cur.close()
            self.assertEqual(_lease_names(self.dir), [])
        self.assertEqual(_lease_names(self.dir), [])

    def test_writer_resume_session_registers_lease(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            tok = s.scan().token()
            gc.collect()
            self.assertEqual(_lease_names(self.dir), [])
            cur = s.scan(token=tok)
            self.assertEqual(len(_lease_names(self.dir)), 1)
            self.assertEqual(list(cur), [("a", b"1")])
            cur.close()
            self.assertEqual(_lease_names(self.dir), [])

    def test_dropped_writer_cursor_releases_lease_on_gc(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            cur = s.scan()
            next(cur)
            self.assertEqual(len(_lease_names(self.dir)), 1)
            del cur
            gc.collect()
            self.assertEqual(_lease_names(self.dir), [])

    def test_writer_cursor_lease_protects_across_processes(self):
        # A writer's open cursor pins its snapshot for a writer running in
        # *another* process through the lease sidecar alone.
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            cur = s.scan()
            self.assertEqual(next(cur), ("old", b"o"))
            advance = (
                "import sys; sys.path.insert(0, %r);"
                "from wal_store import Store;"
                "s = Store(sys.argv[1]);"
                "[(s.put('k%%02d' %% i, b'v'), s.commit()) for i in range(6)];"
                "s.close()" % REPO_ROOT)
            done = subprocess.run([sys.executable, "-c", advance, self.dir],
                                  capture_output=True)
            self.assertEqual(done.returncode, 0, done.stderr)
            # seq 1 is far outside the retention window; only this
            # process's writer-cursor lease keeps its copy.
            self.assertTrue(self.old_copy_present())
            cur.close()
        gc.collect()
        with self.writer():
            pass
        self.assertFalse(self.old_copy_present())


class ExitDetectionTest(LeaseBase):
    def test_killed_holder_expires_by_exit_detection(self):
        token = self.seed_old_token()
        p = self.spawn_holder(token)
        p.send_signal(signal.SIGKILL)
        p.wait()
        # The heartbeat is still fresh (nowhere near the TTL), but the
        # owning process is gone: the lease expires by exit detection and
        # the copy is reclaimed without waiting out the TTL.
        self.advance_past_retention()
        self.assertEqual(_lease_names(self.dir), [])
        self.assertFalse(self.old_copy_present())

    def test_dead_owner_fake_lease_protects_nothing(self):
        with self.writer() as s:
            s.put("old", b"o")
            s.commit()
            copy = [n for n in os.listdir(self.dir) if n.startswith("wal.s1.")][0]
            sid = bytes.fromhex(copy.split(".")[2])
        # A fresh heartbeat from a pid that does not exist expires at once.
        self._write_fake_lease(1, sid, time.time(), pid=999999)
        self.advance_past_retention()
        self.assertFalse(self.old_copy_present())
        self.assertEqual(_lease_names(self.dir), [])


class LeasePruneKillSafeTest(LeaseBase):
    # Kill the process on the nth stale-lease unlink while merely opening the
    # store (an open finishes a reclaim, which first prunes stale leases).
    CRASH = (
        "import os,sys\n"
        "sys.path.insert(0,%r)\n"
        "real=os.unlink; c=[0]\n"
        "def w(*a):\n"
        " if 'wal.lease.' in a[0]:\n"
        "  c[0]+=1\n"
        "  if c[0]==int(sys.argv[2]): os._exit(9)\n"
        " return real(*a)\n"
        "os.unlink=w\n"
        "import wal_store.store as M\n"
        "M._LEASE_TTL=0.02\n"
        "from wal_store import Store\n"
        "Store(sys.argv[1]).close()\n"
    ) % REPO_ROOT

    def test_kill_mid_lease_prune_converges(self):
        import shutil
        token = self.seed_old_token()
        # Several killed holders, each leaving a stale lease sidecar.
        for _ in range(4):
            p = self.spawn_holder(token, duration=30)
            p.send_signal(signal.SIGKILL)
            p.wait()
        time.sleep(0.3)
        stale = _lease_names(self.dir)
        self.assertEqual(len(stale), 4)
        # Force every lease stale, then finish the prune through crashing
        # opens at several unlink points; every reopen must converge.
        import wal_store.store as mod
        old_ttl = mod._LEASE_TTL
        mod._LEASE_TTL = 0.02
        try:
            time.sleep(0.1)
            for point in range(1, 6):
                work = os.path.join(self._tmp.name, "k%d" % point)
                shutil.copytree(self.dir, work)
                proc = subprocess.run(
                    [sys.executable, "-c", self.CRASH, work, str(point)])
                self.assertIn(proc.returncode, (0, 9))
                # A clean finishing open sweeps the rest and reclaims.
                with Store(work) as s:
                    s.put("x", b"x")
                    s.commit()
                self.assertEqual(_lease_names(work), [])
                settled = set(os.listdir(work))
                with Store(work) as s:
                    self.assertEqual(s.get("x"), b"x")
                self.assertEqual(set(os.listdir(work)), settled)
                shutil.rmtree(work)
        finally:
            mod._LEASE_TTL = old_ttl


class ReadOnlyDirectoryLeaseTest(LeaseBase):
    def test_reader_in_readonly_directory_has_no_lease_but_works(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        os.chmod(self.dir, 0o555)
        try:
            r = self.reader()
            self.assertEqual(r.get("a"), b"1")
            cur = r.scan()
            self.assertEqual(next(cur), ("a", b"1"))
            cur.close()
            r.close()
        finally:
            os.chmod(self.dir, 0o755)
        self.assertEqual(_lease_names(self.dir), [])


if __name__ == "__main__":
    unittest.main()
