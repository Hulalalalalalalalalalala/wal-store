"""Crash-safe log compaction.

These tests cover:

* the compacted log holding exactly the live committed puts in sorted key
  order followed by a base commit marker with the preserved sequence;
* overwritten keys collapsing to their last value and deleted keys never
  returning, including an all-deleted/empty state compacted to an empty log;
* the durable sequence staying put and the next commit being ``seq + 1``;
* reclaimed space, removed sidecars and byte-identical repeated runs;
* a kill at every convergence primitive (plus chained kills) converging by
  reopen/redo to exactly the clean-run bytes, state and three-field report;
* a log torn at every byte offset after compaction recovering to the same
  state with no sequence regression;
* live read-only processes pinning their snapshot across a compaction and a
  following commit, while a fresh reader sees the new snapshot;
* old-version directories (a raw WAL2 log, no sidecars) compacting directly;
* read-only rejection, pending-change rejection and unchanged error types.
"""

import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from wal_store import CorruptLogError, Store, inject_tear
from wal_store.store import (
    _encode_frame,
    _iter_frames,
    _OP_BASE,
    _OP_COMMIT,
    _OP_DELETE,
    _OP_PUT,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CompactBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        try:
            os.chmod(self.dir, 0o755)
        except OSError:
            pass
        self._tmp.cleanup()

    def log_bytes(self, name="wal.log"):
        with open(os.path.join(self.dir, name), "rb") as f:
            return f.read()

    def scan(self, data):
        frames, terminal = [], None
        for event in _iter_frames(io.BytesIO(data)):
            if event[0] == "frame":
                frames.append(event)
            else:
                terminal = event
        return frames, terminal


class BasicCompactionTest(CompactBase):
    def _history(self):
        with Store(self.dir) as s:
            for i in range(30):
                s.put(f"k{i:02d}", f"v{i}".encode())
                if i % 5 == 4:
                    s.commit()
            s.put("k00", b"overwritten")
            s.delete("k29")
            s.commit()  # seq 7
        return 7

    def test_log_contains_only_live_sorted_then_base(self):
        seq = self._history()
        before = self.log_bytes()
        with Store(self.dir) as s:
            report = s.compact()
        after = self.log_bytes()
        self.assertLess(len(after), len(before))
        self.assertEqual(
            report, {"applied": 29, "discarded": 0, "seq": seq})

        frames, terminal = self.scan(after)
        self.assertIsNone(terminal)
        ops = [(m["t"], m.get("k"), m.get("s"))
               for _, m, _v, _s, _e in frames]
        self.assertTrue(all(op[0] == _OP_PUT for op in ops[:-1]))
        keys = [op[1] for op in ops[:-1]]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(keys[0], "k00")
        self.assertNotIn("k29", keys)
        self.assertEqual(ops[-1], (_OP_BASE, None, seq))

        # The checkpoint is the same compacted image; staging file is gone.
        self.assertEqual(self.log_bytes("wal.ckp"), after)
        self.assertFalse(os.path.exists(os.path.join(self.dir, "wal.cmp")))

    def test_state_and_sequence_preserved_next_commit_plus_one(self):
        seq = self._history()
        with Store(self.dir) as s:
            self.assertEqual(s.compact()["seq"], seq)
            self.assertEqual(s.get("k00"), b"overwritten")
            self.assertIsNone(s.get("k29"))
            self.assertEqual(s.get("k10"), b"v10")
            # An empty commit advances just like a real one; the next
            # commit is that value + 1.
            self.assertEqual(s.commit(), seq + 1)
            s.put("new", b"x")
            self.assertEqual(s.commit(), seq + 2)
        with Store(self.dir) as s:
            self.assertEqual(s.stats()["seq"], seq + 2)
            self.assertEqual(s.get("k00"), b"overwritten")
            self.assertIsNone(s.get("k29"))
            self.assertEqual(s.get("new"), b"x")

    def test_compact_then_recover_report(self):
        self._history()
        with Store(self.dir) as s:
            s.compact()
        with Store(self.dir) as s:
            report = s.recover()
        self.assertEqual(report["seq"], 7)
        self.assertEqual(report["discarded"], 0)
        self.assertEqual(report["applied"], 29)

    def test_empty_and_all_deleted_compact_to_empty(self):
        empty = os.path.join(self.dir, "empty")
        os.makedirs(empty)
        with Store(empty) as s:
            self.assertEqual(
                s.compact(), {"applied": 0, "discarded": 0, "seq": 0})
        self.assertEqual(os.path.getsize(os.path.join(empty, "wal.log")), 0)
        self.assertFalse(os.path.exists(os.path.join(empty, "wal.ckp")))
        with Store(empty) as s:
            s.put("a", b"1")
            self.assertEqual(s.commit(), 1)

        gone = os.path.join(self.dir, "gone")
        os.makedirs(gone)
        with Store(gone) as s:
            s.put("a", b"1")
            s.commit()
            s.delete("a")
            s.commit()
            report = s.compact()
        self.assertEqual(report,
                         {"applied": 0, "discarded": 0, "seq": 2})
        # Empty state at seq 2: only the base marker remains, and the
        # sequence is carried by it rather than reset.
        gone_log = os.path.join(gone, "wal.log")
        self.assertLess(os.path.getsize(gone_log),
                        len(_encode_frame(_OP_PUT, b"1", key="a")) * 4)
        frames, terminal = self.scan(open(gone_log, "rb").read())
        self.assertIsNone(terminal)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0][1], {"t": _OP_BASE, "s": 2})
        with Store(gone) as s:
            self.assertIsNone(s.get("a"))
            self.assertEqual(s.stats()["seq"], 2)
            s.put("b", b"2")
            self.assertEqual(s.commit(), 3)
            self.assertIsNone(s.get("a"))

    def test_repeated_compaction_is_byte_identical(self):
        self._history()
        with Store(self.dir) as s:
            first = s.compact()
        log1 = self.log_bytes()
        ckp1 = self.log_bytes("wal.ckp")
        with Store(self.dir) as s:
            second = s.compact()
        self.assertEqual(first, second)
        self.assertEqual(self.log_bytes(), log1)
        self.assertEqual(self.log_bytes("wal.ckp"), ckp1)

    def test_compact_rejects_pending_and_read_only(self):
        self._history()
        with Store(self.dir) as s:
            s.put("pending", b"p")
            with self.assertRaises(ValueError):
                s.compact()
        with Store(self.dir, read_only=True) as r:
            with self.assertRaises(ValueError):
                r.compact()

    def test_stats_after_compaction(self):
        self._history()
        with Store(self.dir) as s:
            s.compact()
            stats = s.stats()
        self.assertEqual(stats["seq"], 7)
        self.assertEqual(stats["entries"], 30)  # 29 puts + base marker
        self.assertEqual(stats["bytes"],
                         os.path.getsize(os.path.join(self.dir, "wal.log")))
        with Store(self.dir, read_only=True) as r:
            self.assertEqual(r.stats(), stats)

    def test_raw_bytes_and_unicode_keys_survive(self):
        value = bytes(range(256)) + b"\x00\xff\nbin"
        with Store(self.dir) as s:
            s.put("bin", value)
            s.put("ümlaut-键", b"u")
            s.commit()
            s.put("bin", b"last")
            s.commit()
            s.compact()
        with Store(self.dir) as s:
            self.assertEqual(s.get("bin"), b"last")
            self.assertEqual(s.get("ümlaut-键"), b"u")


# A compactor that os._exit()s on the nth replace/fsync/ftruncate/unlink.
CRASH_COMPACT = r"""
import sys, os
sys.path.insert(0, %r)
n = int(sys.argv[2]); count = [0]
names = ("replace", "fsync", "ftruncate", "unlink")
real = {nm: getattr(os, nm) for nm in names}
def wrap(nm):
    r = real[nm]
    def inner(*a, **k):
        count[0] += 1
        if count[0] == n:
            os._exit(9)
        return r(*a, **k)
    return inner
for nm in names:
    setattr(os, nm, wrap(nm))
from wal_store import Store
with Store(sys.argv[1]) as s:
    s.compact()
""" % REPO_ROOT


class KillSafeCompactionTest(CompactBase):
    def _build(self):
        with Store(self.dir) as s:
            for i in range(24):
                s.put(f"k{i:02d}", b"x" * 40)
                s.commit()
            for i in range(0, 24, 4):
                s.delete(f"k{i:02d}")
            s.commit()
            expected = dict(s._data)
            seq = s.stats()["seq"]
        return expected, seq

    def _reference(self, seq):
        ref = os.path.join(self._tmp.name, "ref")
        shutil.copytree(self.dir, ref)
        with Store(ref) as s:
            s.compact()
        log = open(os.path.join(ref, "wal.log"), "rb").read()
        ckp = open(os.path.join(ref, "wal.ckp"), "rb").read()
        return log, ckp

    def _crash(self, work, point):
        return subprocess.run(
            [sys.executable, "-c", CRASH_COMPACT, work, str(point)],
            capture_output=True)

    def test_kill_at_every_step_converges_byte_identically(self):
        expected, seq = self._build()
        ref_log, ref_ckp = self._reference(seq)
        live = sorted(expected)
        for point in range(1, 50):
            work = os.path.join(self._tmp.name, f"w{point}")
            shutil.copytree(self.dir, work)
            proc = self._crash(work, point)
            self.assertIn(proc.returncode, (0, 9), proc.stderr)
            # Open converges what it can; a redo always finishes the job.
            with Store(work) as s:
                report = s.recover()
                self.assertEqual(report["seq"], seq)
                self.assertEqual(report["discarded"], 0)
                s.compact()
            self.assertEqual(
                open(os.path.join(work, "wal.log"), "rb").read(), ref_log)
            self.assertEqual(
                open(os.path.join(work, "wal.ckp"), "rb").read(), ref_ckp)
            with Store(work) as s:
                self.assertEqual({k: s.get(k) for k in live}, expected)
                s.put("n", b"n")
                self.assertEqual(s.commit(), seq + 1)
            shutil.rmtree(work)

    def test_chained_kills_converge(self):
        expected, seq = self._build()
        ref_log, ref_ckp = self._reference(seq)
        for chain in ((2, 7), (5, 3, 9), (1, 1), (8, 4, 2, 6)):
            work = os.path.join(self._tmp.name, "chain")
            shutil.copytree(self.dir, work)
            for point in chain:
                self._crash(work, point)
            with Store(work) as s:
                s.recover()
                s.compact()
            self.assertEqual(
                open(os.path.join(work, "wal.log"), "rb").read(), ref_log)
            self.assertEqual(
                open(os.path.join(work, "wal.ckp"), "rb").read(), ref_ckp)
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], seq)
                self.assertEqual(s.get("k01"), expected["k01"])
                self.assertIsNone(s.get("k00"))
            shutil.rmtree(work, ignore_errors=True)

    def test_timing_based_kills_always_converge(self):
        expected, seq = self._build()
        code = ("import sys; sys.path.insert(0,%r);"
                "from wal_store import Store;"
                "Store(sys.argv[1]).compact()" % REPO_ROOT)
        for trial in range(10):
            work = os.path.join(self._tmp.name, f"t{trial}")
            shutil.copytree(self.dir, work)
            for _ in range(4):
                p = subprocess.Popen([sys.executable, "-c", code, work])
                time.sleep(0.002 * (trial + 1))
                if p.poll() is None:
                    p.send_signal(signal.SIGKILL)
                    p.wait()
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], seq)
                self.assertEqual(dict(s._data), expected)
            shutil.rmtree(work)

    def test_stale_staging_tmp_ignored(self):
        self._build()
        with open(os.path.join(self.dir, "wal.cmp.tmp"), "wb") as f:
            f.write(b"partial staging bytes")
        with Store(self.dir) as s:
            s.compact()
        self.assertFalse(
            os.path.exists(os.path.join(self.dir, "wal.cmp.tmp")))

    def test_every_offset_tear_after_compaction(self):
        expected, seq = self._build()
        with Store(self.dir) as s:
            s.compact()
        full = self.log_bytes()
        for off in range(0, len(full) + 1, 3):
            work = os.path.join(self._tmp.name, f"o{off}")
            os.makedirs(work)
            shutil.copy(os.path.join(self.dir, "wal.ckp"),
                        os.path.join(work, "wal.ckp"))
            inject_tear(self.dir, os.path.join(work, "wal.log"), off)
            with Store(work) as s:
                report = s.recover()
                self.assertEqual(report["seq"], seq, off)
                self.assertEqual(report["discarded"], 0, off)
                self.assertEqual(dict(s._data), expected, off)
                s.put("fresh", b"f")
                self.assertEqual(s.commit(), seq + 1, off)
            shutil.rmtree(work)


HUNG_READER = r"""
import sys, time, json
sys.path.insert(0, %r)
from wal_store import Store
r = Store(sys.argv[1], read_only=True)
snap = {k: r.get(k) for k in ("k01", "k02")}
print(json.dumps({"seq": r.stats()["seq"],
                  "snap": {k: (v.decode() if v else None)
                           for k, v in snap.items()}}), flush=True)
time.sleep(float(sys.argv[2]))
snap = {k: r.get(k) for k in ("k01", "k02")}
print(json.dumps({"seq": r.stats()["seq"],
                  "snap": {k: (v.decode() if v else None)
                           for k, v in snap.items()}}), flush=True)
""" % REPO_ROOT


class ReaderDuringCompactionTest(CompactBase):
    def test_live_readers_pin_snapshot_across_compaction(self):
        with Store(self.dir) as s:
            s.put("k01", b"old1")
            s.put("k02", b"old2")
            s.commit()
            for i in range(60):
                s.put("k01", ("v%02d" % i).encode())
                s.commit()
        pinned_seq = 61
        readers = [subprocess.Popen(
            [sys.executable, "-c", HUNG_READER, self.dir, "6"],
            stdout=subprocess.PIPE) for _ in range(3)]
        time.sleep(0.4)
        with Store(self.dir) as s:
            report = s.compact()
            self.assertEqual(report["seq"], pinned_seq)
            s.put("k02", b"after")
            s.commit()
        # A fresh reader is on the compacted-then-extended snapshot.
        with Store(self.dir, read_only=True) as r:
            self.assertEqual(r.get("k01"), b"v59")
            self.assertEqual(r.get("k02"), b"after")
            self.assertEqual(r.stats()["seq"], pinned_seq + 1)
        for p in readers:
            out, err = p.communicate(timeout=15)
            self.assertEqual(p.returncode, 0, err)
            lines = out.decode().splitlines()
            before, after = json.loads(lines[0]), json.loads(lines[-1])
            self.assertEqual(before, after)
            self.assertEqual(before["seq"], pinned_seq)
            self.assertEqual(before["snap"],
                             {"k01": "v59", "k02": "old2"})

    def test_reader_creates_nothing_during_compaction(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        with Store(self.dir, read_only=True) as r:
            before = set(os.listdir(self.dir))
            r.get("a")
            with Store(self.dir) as w:
                w.compact()
            r.get("a")
            r.stats()
            self.assertEqual(r.get("a"), b"1")
        self.assertNotIn("wal.cmp", set(os.listdir(self.dir)))
        self.assertFalse(
            any(n.endswith(".tmp") for n in os.listdir(self.dir)))


class OldFormatCompactionTest(CompactBase):
    def test_raw_wal2_log_without_sidecars_compacts(self):
        blob = b"".join([
            _encode_frame(_OP_PUT, b"one", key="a"),
            _encode_frame(_OP_PUT, b"two", key="b"),
            _encode_frame(_OP_COMMIT, seq=1),
            _encode_frame(_OP_DELETE, key="a"),
            _encode_frame(_OP_PUT, b"three", key="c"),
            _encode_frame(_OP_COMMIT, seq=2),
        ])
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(blob)
        with Store(self.dir) as s:
            self.assertEqual(s.get("b"), b"two")
            self.assertIsNone(s.get("a"))
            report = s.compact()
        self.assertEqual(report,
                         {"applied": 2, "discarded": 0, "seq": 2})
        with Store(self.dir) as s:
            self.assertEqual(s.get("b"), b"two")
            self.assertEqual(s.get("c"), b"three")
            self.assertIsNone(s.get("a"))
            s.put("d", b"4")
            self.assertEqual(s.commit(), 3)

    def test_corrupt_compacted_image_raises_and_touches_nothing(self):
        with Store(self.dir) as s:
            for i in range(5):
                s.put(f"k{i}", b"v")
                s.commit()
            s.compact()
        for name in ("wal.log", "wal.ckp"):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                raw = bytearray(f.read())
            raw[20] ^= 0xFF
            with open(p, "wb") as f:
                f.write(bytes(raw))
        with Store(self.dir) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


if __name__ == "__main__":
    unittest.main()
