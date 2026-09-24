"""Crash-safe log compaction.

These tests cover:

* ``Store.compact()`` rewriting the committed history into one compact log
  holding exactly one committed record per live key plus one base commit
  marker carrying the *same* sequence number, and releasing the old space;
* the durable sequence never moving, the next commit always being
  ``seq + 1``, deleted keys never resurrecting and an empty log compacting
  to an empty state;
* repeated/interrupted compactions converging byte-for-byte to one clean
  compaction, with a stable three-field recovery report;
* read-only processes pinning complete snapshots before and after a
  compaction, never seeing mixed/half/released bytes and never touching the
  compaction staging files;
* a kill at every compaction primitive, chained interruptions and random
  signal kills all converging;
* directories written by older versions (no checkpoint) opening and
  compacting directly;
* compacted base-commit framing still rejecting sequence gaps, duplicates
  and stray base markers as ``CorruptLogError``.
"""

import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from wal_store import CorruptLogError, Store
from wal_store.store import _encode_frame, _OP_COMMIT, _OP_PUT

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CompactionBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def build_history(self, d=None):
        """Two commits with rewrites, an empty value and a delete."""
        d = d or self.dir
        with Store(d) as s:
            s.put("once", b"one")
            s.put("repeat", b"first")
            s.put("repeat", b"second")
            s.put("empty", b"")
            s.commit()                            # seq 1
            s.delete("once")
            s.put("once", b"rewritten")
            s.commit()                            # seq 2
        return {"once": b"rewritten",
                "repeat": b"second",
                "empty": b""}

    def log_size(self, d=None):
        return os.path.getsize(os.path.join(d or self.dir, "wal.log"))

    @staticmethod
    def _dir_bytes(d):
        out = {}
        for name in os.listdir(d):
            with open(os.path.join(d, name), "rb") as f:
                out[name] = f.read()
        return out

    def assert_state(self, d, expected, seq):
        with Store(d) as s:
            for key, value in expected.items():
                self.assertEqual(s.get(key), value, key)
            self.assertEqual(s.stats()["seq"], seq)


class BasicCompactionTest(CompactionBase):
    def test_compact_rewrites_to_live_keys_and_preserves_seq(self):
        expected = self.build_history()
        before = self.log_size()
        with Store(self.dir) as s:
            stats = s.compact()
        after = self.log_size()
        self.assertLess(after, before)
        self.assertEqual(stats["seq"], 2)
        self.assertEqual(stats["bytes"], after)
        self.assert_state(self.dir, expected, 2)
        with Store(self.dir) as s:
            self.assertEqual(
                s.recover(),
                {"applied": 3, "discarded": 0, "seq": 2})

    def test_next_commit_after_compaction_is_seq_plus_one(self):
        self.build_history()
        with Store(self.dir) as s:
            s.compact()
            s.put("new", b"v")
            self.assertEqual(s.commit(), 3)
        with Store(self.dir) as s:
            self.assertEqual(s.get("new"), b"v")
            self.assertEqual(s.stats()["seq"], 3)
            self.assertEqual(s.commit(), 3)

    def test_deleted_keys_do_not_resurrect(self):
        with Store(self.dir) as s:
            s.put("keep", b"1")
            s.put("gone", b"x")
            s.commit()
            s.delete("gone")
            s.commit()
        with Store(self.dir) as s:
            s.compact()
        with Store(self.dir) as s:
            self.assertEqual(s.get("keep"), b"1")
            self.assertIsNone(s.get("gone"))

    def test_all_keys_deleted_compacts_to_base_commit_only(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.delete("a")
            s.delete("b")
            s.commit()
        with Store(self.dir) as s:
            stats = s.compact()
        # One base commit frame, no put frames.
        self.assertEqual(stats["entries"], 1)
        with Store(self.dir) as s:
            self.assertIsNone(s.get("a"))
            self.assertIsNone(s.get("b"))
            self.assertEqual(
                s.recover(),
                {"applied": 0, "discarded": 0, "seq": 2})
            s.put("c", b"3")
            self.assertEqual(s.commit(), 3)

    def test_empty_store_compacts_to_empty_and_stays_empty(self):
        with Store(self.dir) as s:
            self.assertEqual(
                s.compact(), {"seq": 0, "entries": 0, "bytes": 0})
        self.assertEqual(self.log_size(), 0)
        with Store(self.dir) as s:
            self.assertEqual(
                s.recover(),
                {"applied": 0, "discarded": 0, "seq": 0})
            s.put("a", b"1")
            self.assertEqual(s.commit(), 1)

    def test_compact_is_idempotent_and_byte_identical(self):
        self.build_history()
        with Store(self.dir) as s:
            s.compact()
        first = {n: self._read(n) for n in os.listdir(self.dir)}
        with Store(self.dir) as s:
            s.compact()
            s.compact()
        second = {n: self._read(n) for n in os.listdir(self.dir)}
        self.assertEqual(first, second)

    def _read(self, name):
        with open(os.path.join(self.dir, name), "rb") as f:
            return f.read()

    def test_compact_requires_no_pending_changes(self):
        with Store(self.dir) as s:
            s.put("k", b"v")
            s.commit()
            s.put("pending", b"x")
            with self.assertRaises(ValueError):
                s.compact()
            self.assertEqual(s.stats()["seq"], 1)

    def test_compact_rejected_when_corrupt(self):
        self.build_history()
        raw = self._read("wal.log")
        damaged = bytearray(raw)
        damaged[-1] ^= 0xFF
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.compact()

    def test_staging_files_are_removed_after_compaction(self):
        self.build_history()
        with Store(self.dir) as s:
            s.compact()
        names = set(os.listdir(self.dir))
        self.assertNotIn("wal.cmp", names)
        self.assertNotIn("wal.cpr", names)
        self.assertFalse(any(n.endswith(".tmp") for n in names))

    def test_raw_byte_values_survive_compaction(self):
        value = bytes(range(256)) + b"\x00\xff\nWAL2binary\n\x00"
        with Store(self.dir) as s:
            s.put("bin", value)
            s.commit()
            s.put("bin", value + b"more")
            s.commit()
        with Store(self.dir) as s:
            s.compact()
        with Store(self.dir) as s:
            self.assertEqual(s.get("bin"), value + b"more")


class ReaderPinningTest(CompactionBase):
    def test_reader_before_and_after_reads_full_committed_snapshot(self):
        expected = self.build_history()
        pinned = Store(self.dir, read_only=True)
        self.assertEqual(pinned.stats()["seq"], 2)
        # First read, before compaction.
        for key, value in expected.items():
            self.assertEqual(pinned.get(key), value)
        with Store(self.dir) as s:
            s.compact()
        # The surviving pinned reader still serves every byte of the old
        # snapshot whose disk space was released.
        for key, value in expected.items():
            self.assertEqual(pinned.get(key), value)
        self.assertEqual(pinned.stats()["seq"], 2)
        # A fresh reader sees the compacted snapshot.
        fresh = Store(self.dir, read_only=True)
        for key, value in expected.items():
            self.assertEqual(fresh.get(key), value)
        self.assertEqual(fresh.stats()["seq"], 2)
        fresh.close()
        pinned.close()

    def test_reader_never_touches_staging_sidecars(self):
        self.build_history()
        with Store(self.dir) as s:
            s.compact()
        before = self._snapshot_listing()
        r = Store(self.dir, read_only=True)
        r.get("once")
        r.get("missing")
        r.stats()
        r.close()
        self.assertEqual(self._snapshot_listing(), before)

    def _snapshot_listing(self):
        out = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                out[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        return out

    def test_reader_opening_during_interrupted_compaction_sees_whole(self):
        # With a plan + staged image present but nothing published yet, a
        # read-only open must see a complete committed snapshot (the old
        # log/checkpoint) and must not read the staging files.
        expected = self.build_history()
        with Store(self.dir) as s:
            frames, _ = s._converge()
            state, _ = s._replay(frames)
            image = b"".join(
                [_encode_frame(_OP_PUT, value=v, key=k)
                 for k, v in state.items()]
                + [_encode_frame(_OP_COMMIT, seq=2, base=True)])
            log_size = os.path.getsize(os.path.join(self.dir, "wal.log"))
            ckp_size = os.path.getsize(os.path.join(self.dir, "wal.ckp"))
        with open(os.path.join(self.dir, "wal.cmp"), "wb") as f:
            f.write(image)
        with open(os.path.join(self.dir, "wal.cpr"), "wb") as f:
            f.write(json.dumps(
                {"s": 2, "n": len(image), "log": log_size,
                 "ckp": ckp_size}, separators=(",", ":")).encode())
        r = Store(self.dir, read_only=True)
        for key, value in expected.items():
            self.assertEqual(r.get(key), value)
        self.assertEqual(r.stats()["seq"], 2)
        r.close()


# Crash harness: arm primitive counting only after the store is open, then
# compact; the nth wrapped primitive (replace/fsync/ftruncate/unlink) kills
# the process with SIGKILL.
CRASH_COMPACT = r"""
import sys, os
sys.path.insert(0, "@ROOT@")
n = int(sys.argv[2]); count = [0]; armed = [False]
primitives = (("replace", os.replace), ("fsync", os.fsync),
              ("ftruncate", os.ftruncate), ("unlink", os.unlink))
for real_name, real_fn in primitives:
    def make(fn):
        def inner(*a, **k):
            if armed[0]:
                count[0] += 1
                if count[0] == n:
                    os._exit(9)
            return fn(*a, **k)
        return inner
    setattr(os, real_name, make(real_fn))
from wal_store import Store
s = Store(sys.argv[1])
armed[0] = True
s.compact()
s.close()
""".replace("@ROOT@", REPO_ROOT)

COMPACT_ONCE = r"""
import sys
sys.path.insert(0, "@ROOT@")
from wal_store import Store
with Store(sys.argv[1]) as s:
    s.compact()
""".replace("@ROOT@", REPO_ROOT)


class CompactionKillSafeTest(CompactionBase):
    EXPECTED = {"once": b"rewritten", "repeat": b"second",
                "empty": b""}

    def _pristine(self, root):
        d = os.path.join(root, "pristine")
        os.makedirs(d)
        self.build_history(d)
        return d

    def _reference_bytes(self, pristine, root):
        ref = os.path.join(root, "ref")
        shutil.copytree(pristine, ref)
        with Store(ref) as s:
            s.compact()
        return self._dir_bytes(ref)

    def _crash_compact(self, d, point):
        return subprocess.run(
            [sys.executable, "-c", CRASH_COMPACT, d, str(point)],
            capture_output=True)

    def _finish(self, d):
        p = subprocess.run(
            [sys.executable, "-c", COMPACT_ONCE, d], capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr)

    def test_kill_at_every_primitive_converges_identically(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        pristine = self._pristine(root)
        reference = self._reference_bytes(pristine, root)
        for point in range(1, 40):
            d = os.path.join(root, f"p{point}")
            shutil.copytree(pristine, d)
            for _ in range(10):
                p = self._crash_compact(d, point)
                if p.returncode == 0:
                    break
                self.assertIn(p.returncode, (-9, 9), p.stderr)
            # Every reopen exposes exactly the last committed snapshot.
            self.assert_state(d, self.EXPECTED, 2)
            self._finish(d)
            got = self._dir_bytes(d)
            self.assertEqual(got, reference, point)
            with Store(d) as s:
                self.assertEqual(
                    s.recover(),
                    {"applied": 3, "discarded": 0, "seq": 2})
                self.assertEqual(
                    s.recover(),
                    {"applied": 3, "discarded": 0, "seq": 2})
                s.put("new", b"v")
                self.assertEqual(s.commit(), 3)
            self.assertEqual(
                set(os.listdir(d)) <= {"wal.log", "wal.ckp", "wal.rec"},
                True, os.listdir(d))

    def test_chained_interruptions_converge(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        pristine = self._pristine(root)
        reference = self._reference_bytes(pristine, root)
        for chain in ((1, 2), (3, 7, 1), (9, 4, 4, 2), (6, 5, 8, 3, 1)):
            d = os.path.join(root, "chain")
            if os.path.exists(d):
                shutil.rmtree(d)
            shutil.copytree(pristine, d)
            for point in chain:
                self._crash_compact(d, point)
            self._finish(d)
            got = self._dir_bytes(d)
            self.assertEqual(got, reference, chain)
            self.assert_state(d, self.EXPECTED, 2)

    def test_random_signal_kills_always_converge(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        pristine = self._pristine(root)
        reference = self._reference_bytes(pristine, root)
        rng = random.Random(20240925)
        for trial in range(30):
            d = os.path.join(root, f"t{trial}")
            shutil.copytree(pristine, d)
            for _ in range(rng.randrange(0, 4)):
                p = subprocess.Popen(
                    [sys.executable, "-c", COMPACT_ONCE, d])
                deadline = time.time() + rng.uniform(0.0, 0.05)
                while time.time() < deadline and p.poll() is None:
                    time.sleep(0.0004)
                if p.poll() is None:
                    p.send_signal(signal.SIGKILL)
                p.wait()
            self.assert_state(d, self.EXPECTED, 2)
            self._finish(d)
            got = self._dir_bytes(d)
            self.assertEqual(got, reference, trial)

    def test_torn_staged_image_is_regenerated(self):
        d = os.path.join(self._tmp.name, "torn")
        os.makedirs(d)
        self.build_history(d)
        with Store(d) as s:
            frames, _ = s._converge()
            state, _ = s._replay(frames)
            image = b"".join(
                [_encode_frame(_OP_PUT, value=v, key=k)
                 for k, v in state.items()]
                + [_encode_frame(_OP_COMMIT, seq=2, base=True)])
            log_size = os.path.getsize(os.path.join(d, "wal.log"))
            ckp_size = os.path.getsize(os.path.join(d, "wal.ckp"))
        with open(os.path.join(d, "wal.cmp"), "wb") as f:
            f.write(image[: len(image) // 2])  # torn staged image
        with open(os.path.join(d, "wal.cpr"), "wb") as f:
            f.write(json.dumps(
                {"s": 2, "n": len(image), "log": log_size,
                 "ckp": ckp_size}, separators=(",", ":")).encode())
        with Store(d) as s:  # regenerates the image and finishes
            self.assert_state(d, self.EXPECTED, 2)
            self.assertEqual(os.path.getsize(os.path.join(d, "wal.log")),
                             len(image))
            s.put("n", b"v")
            self.assertEqual(s.commit(), 3)

    def test_orphan_staged_image_after_publish_is_removed(self):
        # A kill between plan removal and image unlink.
        d = os.path.join(self._tmp.name, "orphan")
        os.makedirs(d)
        self.build_history(d)
        with Store(d) as s:
            s.compact()
        shutil.copy(os.path.join(d, "wal.log"),
                    os.path.join(d, "wal.cmp"))
        self.assertIn("wal.cmp", os.listdir(d))
        with Store(d) as s:
            self.assertEqual(s.stats()["seq"], 2)
        self.assertNotIn("wal.cmp", os.listdir(d))

    def test_stale_temp_files_are_removed(self):
        d = os.path.join(self._tmp.name, "temps")
        os.makedirs(d)
        self.build_history(d)
        for name in ("wal.cmp.tmp", "wal.cpr.tmp", "wal.log.tmp",
                     "wal.ckp.tmp"):
            with open(os.path.join(d, name), "wb") as f:
                f.write(b"partial")
        with Store(d) as s:
            s.compact()
        self.assertFalse(
            any(n.endswith(".tmp") for n in os.listdir(d)))


class PostCompactionTearTest(CompactionBase):
    EXPECTED = {"once": b"rewritten", "repeat": b"second",
                "empty": b""}

    def test_every_offset_tear_after_compaction_converges(self):
        from wal_store import inject_tear
        d = os.path.join(self._tmp.name, "compacted")
        os.makedirs(d)
        self.build_history(d)
        with Store(d) as s:
            s.compact()
        with open(os.path.join(d, "wal.log"), "rb") as f:
            full = f.read()
        for off in range(len(full) + 1):
            work = os.path.join(self._tmp.name, f"off{off}")
            os.makedirs(work, exist_ok=True)
            for entry in os.listdir(work):
                os.unlink(os.path.join(work, entry))
            shutil.copy(os.path.join(d, "wal.ckp"),
                        os.path.join(work, "wal.ckp"))
            inject_tear(d, os.path.join(work, "wal.log"), off)
            with Store(work) as s:
                for key, value in self.EXPECTED.items():
                    self.assertEqual(s.get(key), value, off)
                self.assertEqual(
                    s.recover(),
                    {"applied": 3, "discarded": 0, "seq": 2}, off)
                self.assertEqual(
                    os.path.getsize(os.path.join(work, "wal.log")),
                    len(full), off)
            with Store(work) as s:
                s.put("n", b"v")
                self.assertEqual(s.commit(), 3, off)


class ReaderWriterCompactionConcurrencyTest(CompactionBase):
    """Live pinned/sweeping readers while a writer compacts in a loop."""

    SEED = r"""
import sys
sys.path.insert(0, "@ROOT@")
from wal_store import Store
with Store(sys.argv[1]) as s:
    s.put("k0", b"v0"); s.put("k1", b"v0"); s.commit()
""".replace("@ROOT@", REPO_ROOT)

    PIN = r"""
import sys, json, time
sys.path.insert(0, "@ROOT@")
from wal_store import Store
r = Store(sys.argv[1], read_only=True)
def snap():
    return {k: (r.get(k).decode() if r.get(k) is not None else None)
            for k in ("k0", "k1")}
print(json.dumps((r.stats()["seq"], snap())), flush=True)
time.sleep(float(sys.argv[2]))
print(json.dumps((r.stats()["seq"], snap())), flush=True)
r.close()
""".replace("@ROOT@", REPO_ROOT)

    WRITER = r"""
import sys, time
sys.path.insert(0, "@ROOT@")
from wal_store import Store
deadline = time.time() + float(sys.argv[2]); n = 0
with Store(sys.argv[1]) as s:
    while time.time() < deadline:
        g = ("g%d" % n).encode()
        s.put("k0", g); s.put("k1", g); s.commit()
        s.compact()
        n += 1
print(n)
""".replace("@ROOT@", REPO_ROOT)

    SWEEP = r"""
import sys, json, time
sys.path.insert(0, "@ROOT@")
from wal_store import Store
deadline = time.time() + float(sys.argv[3]); seen = []
while time.time() < deadline:
    r = Store(sys.argv[1], read_only=True)
    v0, v1 = r.get("k0"), r.get("k1")
    seen.append((r.stats()["seq"],
                 None if v0 is None else v0.decode(),
                 None if v1 is None else v1.decode()))
    r.close()
print(json.dumps(seen))
""".replace("@ROOT@", REPO_ROOT)

    def test_pinned_readers_survive_continuous_compaction(self):
        d = self.dir
        subprocess.run([sys.executable, "-c", self.SEED, d], check=True)
        pins = [subprocess.Popen(
            [sys.executable, "-c", self.PIN, d, "4"],
            stdout=subprocess.PIPE, text=True) for _ in range(4)]
        time.sleep(0.4)
        p = subprocess.run(
            [sys.executable, "-c", self.WRITER, d, "2.0"],
            capture_output=True, text=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertGreater(int(p.stdout.strip()), 0)
        for proc in pins:
            out, err = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 0, err)
            lines = [json.loads(x) for x in out.splitlines()]
            self.assertEqual(len(lines), 2)
            for _seq, snap in lines:
                # Keys committed together must always carry one generation.
                self.assertEqual(snap["k0"], snap["k1"])
            # The pinned snapshot must not move across the compactions.
            self.assertEqual(lines[0][1], lines[1][1])

    def test_sweeping_readers_always_see_coherent_snapshots(self):
        d = self.dir
        subprocess.run([sys.executable, "-c", self.SEED, d], check=True)
        duration = 2.0
        writer = subprocess.Popen(
            [sys.executable, "-c", self.WRITER, d, str(duration)])
        readers = [subprocess.Popen(
            [sys.executable, "-c", self.SWEEP, d, os.devnull,
             str(duration)], stdout=subprocess.PIPE) for _ in range(4)]
        self.assertEqual(writer.wait(timeout=duration + 10), 0)
        total = 0
        for proc in readers:
            out, err = proc.communicate(timeout=15)
            self.assertEqual(proc.returncode, 0, err)
            for _seq, v0, v1 in json.loads(out):
                total += 1
                self.assertEqual(v0, v1)
        self.assertGreater(total, 0)


class OldVersionLogTest(CompactionBase):
    def test_log_without_checkpoint_opens_and_compacts(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.delete("a")
            s.commit()
        os.unlink(os.path.join(self.dir, "wal.ckp"))  # old-version directory
        with Store(self.dir) as s:
            self.assertEqual(s.get("b"), b"2")
            self.assertIsNone(s.get("a"))
            stats = s.compact()
        self.assertEqual(stats["seq"], 2)
        with Store(self.dir) as s:
            self.assertEqual(
                s.recover(),
                {"applied": 1, "discarded": 0, "seq": 2})
            s.put("c", b"3")
            self.assertEqual(s.commit(), 3)


class BaseCommitValidationTest(CompactionBase):
    def _compacted(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("a", b"2")
            s.commit()
        with Store(self.dir) as s:
            s.compact()
        with open(os.path.join(self.dir, "wal.log"), "rb") as f:
            return f.read()

    def _damage_both(self, payload):
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(payload)
        with open(os.path.join(self.dir, "wal.ckp"), "wb") as f:
            f.write(payload)

    def test_sequence_gap_after_base_is_corrupt(self):
        image = self._compacted()
        self._damage_both(image + _encode_frame(_OP_COMMIT, seq=9))
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_duplicate_sequence_after_base_is_corrupt(self):
        image = self._compacted()
        self._damage_both(image + _encode_frame(_OP_COMMIT, seq=2))
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_second_base_marker_is_corrupt(self):
        image = self._compacted()
        self._damage_both(
            image
            + _encode_frame(_OP_COMMIT, seq=3)
            + _encode_frame(_OP_COMMIT, seq=4, base=True))
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_plain_first_commit_still_must_be_sequence_one(self):
        # The new base-flag framing must not relax the ordinary rule.
        payload = (_encode_frame(_OP_PUT, b"1", key="a")
                   + _encode_frame(_OP_COMMIT, seq=2))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(payload)
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


if __name__ == "__main__":
    unittest.main()
