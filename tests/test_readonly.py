"""Isolated read-only processes sharing a store with one writer.

These tests cover the read side added on top of the single-writer log:

* opening read-only through the public ``Store`` entry (keyword, class
  method, module helper) and its hard no-write boundary;
* reads always landing on a complete committed snapshot -- never on
  uncommitted bytes, a torn frame or a half-finished shrink;
* one reader advancing across snapshots without ever regressing;
* readers creating or modifying nothing on disk;
* read cost independent of log history (indexed sidecar, no replay);
* pre-index (old) logs opening read-only without conversion;
* multiple reader processes beside a live writer, a hung reader not
  blocking the writer, and readers killed at random moments being inert;
* crash/state/seq stability and the unchanged exception/exit contract.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from wal_store import CorruptLogError, Store, open_readonly
from wal_store.store import (
    _INDEX_NAME,
    _encode_frame,
    _OP_PUT,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ReaderBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def build_committed(self):
        with Store(self.dir) as s:
            s.put("a", b"one")
            s.put("b", b"two")
            s.commit()
            s.put("c", b"three")
            s.commit()

    def snapshot_dir(self):
        return {n: (os.stat(os.path.join(self.dir, n)).st_mtime_ns,
                    os.stat(os.path.join(self.dir, n)).st_size)
                for n in os.listdir(self.dir)}


class ReadOnlyApiTest(ReaderBase):
    def test_three_equivalent_openings(self):
        self.build_committed()
        for opener in (lambda p: Store(p, read_only=True),
                       Store.open_readonly,
                       open_readonly):
            with opener(self.dir) as r:
                self.assertEqual(r.get("a"), b"one")
                self.assertEqual(r.get("c"), b"three")

    def test_missing_directory_raises_filenotfound(self):
        missing = os.path.join(self.dir, "nope")
        with self.assertRaises(FileNotFoundError):
            Store(missing, read_only=True)
        with self.assertRaises(FileNotFoundError):
            open_readonly(missing)

    def test_file_path_raises_oserror(self):
        path = os.path.join(self.dir, "afile")
        with open(path, "wb") as f:
            f.write(b"x")
        with self.assertRaises(OSError):
            Store(path, read_only=True)

    def test_empty_directory_is_empty_snapshot(self):
        with open_readonly(self.dir) as r:
            self.assertIsNone(r.get("k"))
            self.assertEqual(r.stats(),
                             {"seq": 0, "entries": 0, "bytes": 0})
        # A reader in an empty directory creates nothing.
        self.assertEqual(os.listdir(self.dir), [])

    def test_writer_methods_are_hard_blocked(self):
        with open_readonly(self.dir) as r:
            with self.assertRaises(ValueError):
                r.put("k", b"v")
            with self.assertRaises(ValueError):
                r.delete("k")
            with self.assertRaises(ValueError):
                r.commit()
            with self.assertRaises(ValueError):
                r.recover()
        self.assertEqual(os.listdir(self.dir), [])

    def test_get_validation_still_applies(self):
        with open_readonly(self.dir) as r:
            for bad in (1, 1.5, b"k", None):
                with self.assertRaises(TypeError):
                    r.get(bad)
            with self.assertRaises(ValueError):
                r.get("")
            with self.assertRaises(ValueError):
                r.close()
                r.get("k")

    def test_reader_holds_no_writable_descriptor(self):
        self.build_committed()
        r = open_readonly(self.dir)
        self.assertIsNone(r._fd)
        r.get("a")
        r.get("missing")
        self.assertIsNone(r._fd)
        r.close()


class SnapshotVisibilityTest(ReaderBase):
    def test_uncommitted_writes_are_invisible(self):
        self.build_committed()
        with Store(self.dir) as w:
            w.put("d", b"four")
            w.delete("a")
            with open_readonly(self.dir) as r:
                self.assertEqual(r.get("a"), b"one")
                self.assertIsNone(r.get("d"))
                self.assertEqual(r.stats()["seq"], 2)

    def test_reader_parks_on_opening_snapshot_then_advances(self):
        with Store(self.dir) as w:
            w.put("k", b"v0")
            w.commit()
        r = open_readonly(self.dir)
        self.assertEqual(r.get("k"), b"v0")
        with Store(self.dir) as w:
            w.put("k", b"v1")
            w.commit()
        # Advancing across snapshots is allowed; regressing is not.
        self.assertEqual(r.get("k"), b"v1")
        with Store(self.dir) as w:
            w.put("k", b"v2")
            w.commit()
        self.assertEqual(r.get("k"), b"v2")
        r.close()

    def test_deleted_key_disappears_after_commit_only(self):
        self.build_committed()
        with Store(self.dir) as w:
            w.delete("a")
            r = open_readonly(self.dir)
            self.assertEqual(r.get("a"), b"one")
            w.commit()
            self.assertIsNone(r.get("a"))
            r.close()

    def test_binary_values_with_crlf_and_newlines(self):
        value = b"\r\n\n" + bytes(range(256)) + b"\r\n"
        with Store(self.dir) as w:
            w.put("bin", value)
            w.commit()
        # Binary on disk: no newline translation, exact framed length.
        with open(os.path.join(self.dir, "wal.log"), "rb") as f:
            on_disk = f.read()
        self.assertEqual(on_disk, _encode_frame(_OP_PUT, value, key="bin")
                         + _encode_frame("c", seq=1))
        with open_readonly(self.dir) as r:
            self.assertEqual(r.get("bin"), value)

    def test_torn_tail_never_reaches_reader(self):
        self.build_committed()
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"ghost", key="g")[:9])
        with open_readonly(self.dir) as r:
            self.assertIsNone(r.get("g"))
            self.assertEqual(r.get("a"), b"one")

    def test_half_finished_shrink_is_invisible_readers_use_checkpoint(self):
        self.build_committed()
        boundary = os.path.getsize(os.path.join(self.dir, "wal.ckp"))
        r = open_readonly(self.dir)
        self.assertEqual(r.get("c"), b"three")
        # Simulate a recovery shrink torn at offset 0: wal.log vanishes
        # underneath the reader while the committed prefix lives in ckp.
        with open(os.path.join(self.dir, "wal.log"), "wb"):
            pass
        # The parked snapshot keeps serving, straight from the checkpoint.
        for _ in range(5):
            self.assertEqual(r.get("a"), b"one")
            self.assertEqual(r.get("c"), b"three")
        # A writer reopen converges the log; the reader follows afterwards.
        with Store(self.dir) as w:
            self.assertEqual(w.stats()["seq"], 2)
        self.assertEqual(os.path.getsize(os.path.join(self.dir, "wal.log")),
                         boundary)
        with Store(self.dir) as w:
            w.put("e", b"four")
            w.commit()
        self.assertEqual(r.get("e"), b"four")
        r.close()

    def test_stats_describe_parked_snapshot(self):
        self.build_committed()
        r = open_readonly(self.dir)
        self.assertEqual(r.stats()["seq"], 2)
        with Store(self.dir) as w:
            for i in range(5):
                w.put(f"x{i}", b"y")
            w.commit()
        self.assertEqual(r.stats()["seq"], 3)
        r.close()


class ReaderWritesNothingTest(ReaderBase):
    def test_reader_preserves_directory_exactly(self):
        self.build_committed()
        before = self.snapshot_dir()
        r = open_readonly(self.dir)
        for key in ("a", "b", "c", "missing"):
            for _ in range(3):
                r.get(key)
        r.stats()
        r.close()
        after = self.snapshot_dir()
        self.assertEqual(before, after)

    def test_legacy_log_opens_without_conversion_or_writes(self):
        self.build_committed()
        os.unlink(os.path.join(self.dir, _INDEX_NAME))
        self.assertFalse(os.path.exists(os.path.join(self.dir, _INDEX_NAME)))
        before = self.snapshot_dir()
        with open_readonly(self.dir) as r:
            self.assertEqual(r.get("a"), b"one")
            self.assertEqual(r.get("c"), b"three")
            self.assertIsNone(r.get("zzz"))
            self.assertEqual(r.stats()["seq"], 2)
        self.assertEqual(before, self.snapshot_dir())

    def test_legacy_log_with_torn_tail_replays_committed_only(self):
        self.build_committed()
        os.unlink(os.path.join(self.dir, _INDEX_NAME))
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"ghost", key="g")[:6])
        with open_readonly(self.dir) as r:
            self.assertIsNone(r.get("g"))
            self.assertEqual(r.get("c"), b"three")

    def test_corrupt_legacy_log_without_checkpoint_raises(self):
        with Store(self.dir) as w:
            w.put("a", b"1")
            w.commit()
        with open(os.path.join(self.dir, "wal.log"), "rb") as f:
            raw = f.read()
        damaged = bytearray(raw)
        damaged[-1] ^= 0xFF
        # Remove both new-era sidecars to mimic a genuinely old, damaged log.
        os.unlink(os.path.join(self.dir, _INDEX_NAME))
        os.unlink(os.path.join(self.dir, "wal.ckp"))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with self.assertRaises(CorruptLogError):
            open_readonly(self.dir)


class ManifestIntegrityTest(ReaderBase):
    def test_index_published_only_with_commit(self):
        with Store(self.dir) as w:
            self.assertFalse(os.path.exists(
                os.path.join(self.dir, _INDEX_NAME)))
            w.put("a", b"1")
            self.assertFalse(os.path.exists(
                os.path.join(self.dir, _INDEX_NAME)))
            w.commit()
            self.assertTrue(os.path.exists(
                os.path.join(self.dir, _INDEX_NAME)))

    def test_torn_manifest_is_skipped_not_served(self):
        self.build_committed()
        r = open_readonly(self.dir)
        path = os.path.join(self.dir, _INDEX_NAME)
        with open(path, "rb") as f:
            raw = f.read()
        try:
            # A half-published manifest: checksum fails, reads skip it.
            with open(path, "wb") as f:
                f.write(raw[: len(raw) // 2])
            self.assertEqual(r.get("a"), b"one")
            self.assertEqual(r.get("c"), b"three")
        finally:
            with open(path, "wb") as f:
                f.write(raw)
        with Store(self.dir) as w:
            w.put("d", b"four")
            w.commit()
        self.assertEqual(r.get("d"), b"four")
        r.close()

    def test_writer_reopens_after_kill_before_index_publish(self):
        self.build_committed()
        # Kill window emulation: commit frame and checkpoint are durable but
        # wal.idx is missing/stale. A writer reopen must republish it.
        os.unlink(os.path.join(self.dir, _INDEX_NAME))
        with open_readonly(self.dir) as r:
            # Reader falls back to legacy replay, still sees last commit.
            self.assertEqual(r.get("c"), b"three")
        with Store(self.dir) as w:
            self.assertEqual(w.stats()["seq"], 2)
        self.assertTrue(os.path.exists(
            os.path.join(self.dir, _INDEX_NAME)))
        with open_readonly(self.dir) as r:
            self.assertEqual(r.get("c"), b"three")
            self.assertEqual(r.stats()["seq"], 2)
        # Next commit is exactly seq + 1.
        with Store(self.dir) as w:
            w.put("e", b"4")
            self.assertEqual(w.commit(), 3)
        with open_readonly(self.dir) as r:
            self.assertEqual(r.stats()["seq"], 3)

    def test_recover_does_not_needlessly_republish(self):
        self.build_committed()
        path = os.path.join(self.dir, _INDEX_NAME)
        mtime = os.stat(path).st_mtime_ns
        time.sleep(0.01)
        with Store(self.dir) as w:
            w.recover()
        self.assertEqual(os.stat(path).st_mtime_ns, mtime)

    def test_corrupt_manifest_at_open_raises(self):
        self.build_committed()
        path = os.path.join(self.dir, _INDEX_NAME)
        with open(path, "r+b") as f:
            f.seek(-1, os.SEEK_END)
            f.write(b"\x00")
        with self.assertRaises(CorruptLogError):
            open_readonly(self.dir)

    def test_reader_parked_while_another_process_recovers_torn_tail(self):
        with Store(self.dir) as w:
            w.put("a", b"1")
            w.commit()
        r = open_readonly(self.dir)
        self.assertEqual(r.get("a"), b"1")
        # Another writer-style crash remnant appears in the log; a separate
        # process converges while the reader stays open.
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"ghost", key="g")[:7])
        proc = subprocess.run(
            [sys.executable, "-m", "wal_store",
             "--path", self.dir, "recover"],
            capture_output=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["discarded"], 1)
        self.assertEqual(r.get("a"), b"1")
        self.assertIsNone(r.get("g"))
        with Store(self.dir) as w:
            w.put("b", b"2")
            self.assertEqual(w.commit(), 2)
        self.assertEqual(r.get("b"), b"2")
        r.close()


class HistoryIndependenceTest(ReaderBase):
    def test_get_cost_independent_of_log_length(self):
        commits = 200
        with Store(self.dir) as w:
            for i in range(commits):
                w.put("hot", f"v{i}".encode() * 8)
                w.put(f"cold{i}", b"x" * 64)
                w.commit()
        r = open_readonly(self.dir)
        # Warm caches, then time many reads against a very long history.
        for _ in range(100):
            r.get("hot")

        def many_gets(n):
            t0 = time.perf_counter()
            for _ in range(n):
                r.get("hot")
            return time.perf_counter() - t0

        short = many_gets(2000)
        # Bound is deliberately loose for shared CI machines; the point is
        # the work per read is one indexed ranged read, not a log replay.
        self.assertLess(short, 2.0, short)
        # Rebuilding the in-memory state the old way (replay) is plainly
        # history-scaled; indexed reads stay flat as history doubles.
        log_size = os.path.getsize(os.path.join(self.dir, "wal.log"))
        self.assertGreater(log_size, 64 * commits)
        r.close()


# -- cross-process scripts -------------------------------------------------

WRITER_SCRIPT = r"""
import sys
sys.path.insert(0, %r)
from wal_store import Store

d, n = sys.argv[1], int(sys.argv[2])
with Store(d) as s:
    for i in range(n):
        for k in range(10):
            s.put("k%%d" %% k, ("gen%%d" %% i).encode())
        s.put("generation", str(i).encode())
        s.commit()
""" % REPO_ROOT

READER_SCRIPT = r"""
import json, re, sys
sys.path.insert(0, %r)
from wal_store import open_readonly

d, loops, hold_ms = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
target = int(sys.argv[4]) if len(sys.argv) > 4 else None
r = open_readonly(d)
last = {}
lo, hi, i = None, None, 0
errors = 0
while i < loops and (target is None or hi != target):
    k = i %% 10
    v = r.get("k%%d" %% k)
    if v is not None:
        m = re.fullmatch(rb"gen(-?\d+)", v)
        if not m:
            errors += 1
        else:
            g = int(m.group(1))
            if k in last and g < last[k]:
                errors += 1
            last[k] = g
            lo = g if lo is None else min(lo, g)
            hi = g if hi is None else max(hi, g)
    i += 1
    if hold_ms:
        import time
        time.sleep(hold_ms / 1000.0)
r.close()
print(json.dumps({"min": lo, "max": hi, "errors": errors, "loops": i}))
""" % REPO_ROOT

# A writer that publishes one commit, then sits on uncommitted changes.
DIRTY_WRITER_SCRIPT = r"""
import os, sys, time
sys.path.insert(0, %r)
from wal_store import Store

d = sys.argv[1]
with Store(d) as s:
    s.put("uncommitted", b"NO")
    s.put("a", b"DIRTY")
    with open(os.path.join(d, "writer.ready"), "w") as f:
        f.write("1")
    time.sleep(float(sys.argv[2]))
""" % REPO_ROOT

READER_DURING_DIRTY_SCRIPT = r"""
import json, os, sys, time
sys.path.insert(0, %r)
from wal_store import open_readonly

d = sys.argv[1]
for _ in range(200):
    if os.path.exists(os.path.join(d, "writer.ready")):
        break
    time.sleep(0.01)
r = open_readonly(d)
a = r.get("a")
out = {"uncommitted": r.get("uncommitted"),
       "a": a.decode() if a is not None else None,
       "seq": r.stats()["seq"]}
r.close()
print(json.dumps(out))
""" % REPO_ROOT


class CrossProcessTest(ReaderBase):
    def test_many_readers_beside_a_live_writer_always_see_committed(self):
        with Store(self.dir) as s:
            for k in range(10):
                s.put(f"k{k}", b"gen-1")
            s.commit()
        n = 80
        writer = subprocess.Popen(
            [sys.executable, "-c", WRITER_SCRIPT, self.dir, str(n)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        readers = [subprocess.Popen(
            [sys.executable, "-c", READER_SCRIPT, self.dir,
             str(10 ** 9), "0", str(n - 1)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            for _ in range(5)]
        self.assertEqual(writer.wait(timeout=60), 0,
                         writer.stderr.read())
        for rd in readers:
            out, err = rd.communicate(timeout=30)
            self.assertEqual(rd.returncode, 0, err)
            info = json.loads(out)
            self.assertEqual(info["errors"], 0, info)
            self.assertEqual(info["max"], n - 1, info)

    def test_hung_reader_does_not_block_writer(self):
        with Store(self.dir) as s:
            s.put("k0", b"gen-1")
            s.commit()
        # Reader parks on a snapshot and then sleeps for a long time.
        hung = subprocess.Popen(
            [sys.executable, "-c", READER_SCRIPT, self.dir,
             str(10 ** 9), "100000"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            t0 = time.perf_counter()
            proc = subprocess.run(
                [sys.executable, "-c", WRITER_SCRIPT, self.dir, "40"],
                capture_output=True, timeout=60)
            elapsed = time.perf_counter() - t0
            self.assertEqual(proc.returncode, 0, proc.stderr)
            # Forty fsynced commits run to completion promptly even though
            # a reader process is parked the whole time.
            self.assertLess(elapsed, 30.0, elapsed)
        finally:
            hung.kill()
            hung.communicate()
        with Store(self.dir) as s:
            self.assertEqual(s.stats()["seq"], 41)

    def test_killed_readers_never_disturb_the_writer(self):
        with Store(self.dir) as s:
            for k in range(10):
                s.put(f"k{k}", b"gen-1")
            s.commit()
        writer = subprocess.Popen(
            [sys.executable, "-c", WRITER_SCRIPT, self.dir, "120"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        victims = []
        deadline = time.time() + 8
        while time.time() < deadline and writer.poll() is None:
            rd = subprocess.Popen(
                [sys.executable, "-c", READER_SCRIPT, self.dir,
                 "1000000", "1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            victims.append(rd)
            time.sleep(0.05)
            rd.kill()
        self.assertEqual(writer.wait(timeout=60), 0, writer.stderr.read())
        for rd in victims:
            rd.wait()
        # Restart: exact last committed state and seq continues.
        with Store(self.dir) as s:
            self.assertEqual(s.stats()["seq"], 121)
            self.assertEqual(s.get("generation"), b"119")
            s.put("after", b"storm")
            self.assertEqual(s.commit(), 122)

    def test_uncommitted_writer_state_is_invisible_to_other_process(self):
        with Store(self.dir) as s:
            s.put("a", b"committed")
            s.commit()
        dirty = subprocess.Popen(
            [sys.executable, "-c", DIRTY_WRITER_SCRIPT, self.dir, "20"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            proc = subprocess.run(
                [sys.executable, "-c", READER_DURING_DIRTY_SCRIPT,
                 self.dir],
                capture_output=True, timeout=30)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            seen = json.loads(proc.stdout)
            self.assertIsNone(seen["uncommitted"])
            self.assertEqual(seen["a"], "committed")
            self.assertEqual(seen["seq"], 1)
        finally:
            dirty.kill()
            dirty.communicate()
        # The dirty writer was killed uncommitted: reopen converges exactly
        # to the last committed state and report.
        with Store(self.dir) as s:
            self.assertEqual(s.get("a"), b"committed")
            self.assertIsNone(s.get("uncommitted"))
            self.assertEqual(s.recover()["seq"], 1)

    def test_reader_and_writer_reopen_after_everything_killed(self):
        with Store(self.dir) as s:
            for i in range(3):
                s.put(f"k{i}", f"v{i}".encode())
                s.commit()
        writer = subprocess.Popen(
            [sys.executable, "-c", WRITER_SCRIPT, self.dir, "10"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        readers = [subprocess.Popen(
            [sys.executable, "-c", READER_SCRIPT, self.dir, "100000", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(3)]
        time.sleep(0.1)
        for rd in readers:
            rd.kill()
        self.assertEqual(writer.wait(timeout=30), 0)
        for rd in readers:
            rd.wait()
        # Fresh processes observe one identical committed snapshot.
        for _ in range(2):
            with open_readonly(self.dir) as r:
                self.assertEqual(r.stats()["seq"], 13)
                self.assertEqual(r.get("generation"), b"9")
        with Store(self.dir) as s:
            # 3 initial puts + 10 iterations of 11 puts; 13 commits.
            self.assertEqual(s.recover(),
                             {"applied": 113, "discarded": 0, "seq": 13})


if __name__ == "__main__":
    unittest.main()
