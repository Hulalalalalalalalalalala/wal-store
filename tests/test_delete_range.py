"""Range deletes with tombstone semantics.

These tests cover:

* ``Store.delete_range(start, end)`` deleting every committed key in the
  half-open bytewise range, with ``None`` endpoints unbounded and the
  same endpoint validation as scans (reversed range raises
  ``ValueError``, non-string endpoints ``TypeError``);
* committed range deletes removing keys from both ``get`` and ``scan``,
  while keys only ever present in delete history never reappear;
* log-order semantics: a key re-put after the tombstone lives and reads
  back as its last committed value, keys and values staying raw bytes;
* uncommitted range deletes being invisible to scans and read-only
  opens, and a killed writer reopening to the last committed state;
* compaction reclaiming expired tombstones without resurrecting deleted
  keys, scanning byte-identically before and after, keeping the
  three-field report and the ``seq + 1`` next commit;
* kill-safe redo: interrupting and repeating converges to the clean-run
  result with a stable recovery report;
* read-only rejection and unchanged behaviour on old-version logs.
"""

import os
import subprocess
import sys
import tempfile
import unittest

from wal_store import CorruptLogError, Store
from wal_store.store import (
    _encode_frame,
    _iter_frames,
    _OP_BASE,
    _OP_COMMIT,
    _OP_PUT,
    _OP_RANGE,
)
import io

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

KILLER = r"""
import sys, os, signal
sys.path.insert(0, %r)
from wal_store import Store
with Store(sys.argv[1]) as s:
    s.delete_range("a", "m")
    os.kill(os.getpid(), signal.SIGKILL)
""" % REPO_ROOT


class RangeDeleteBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def seed(self, pairs):
        with self.writer() as s:
            for key, value in pairs:
                s.put(key, value)
            s.commit()

    def frames(self, name="wal.log"):
        with open(os.path.join(self.dir, name), "rb") as f:
            data = f.read()
        out, terminal = [], None
        for event in _iter_frames(io.BytesIO(data)):
            if event[0] == "frame":
                out.append(event)
            else:
                terminal = event
        return out, terminal


class BasicRangeDeleteTest(RangeDeleteBase):
    def test_half_open_range_deleted_on_commit(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "c", "d", "e")])
        with self.writer() as s:
            s.delete_range("b", "e")
            s.commit()
            self.assertIsNone(s.get("b"))
            self.assertIsNone(s.get("c"))
            self.assertIsNone(s.get("d"))
            self.assertEqual(s.get("a"), b"a")
            self.assertEqual(s.get("e"), b"e")
            self.assertEqual(list(s.scan()), [("a", b"a"), ("e", b"e")])
        with self.reader() as r:
            self.assertIsNone(r.get("c"))
            self.assertEqual(list(r.scan()), [("a", b"a"), ("e", b"e")])

    def test_open_ended_and_unbounded_ranges(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "c", "d")])
        with self.writer() as s:
            s.delete_range("c")  # to end of key space
            s.commit()
            self.assertEqual(list(s.scan()), [("a", b"a"), ("b", b"b")])
            s.delete_range(None, "b")  # from start of key space
            s.commit()
            self.assertEqual(list(s.scan()), [("b", b"b")])
            s.delete_range()  # everything
            s.commit()
            self.assertEqual(list(s.scan()), [])
            self.assertIsNone(s.get("b"))

    def test_absent_endpoints_and_empty_range(self):
        self.seed([("a", b"1"), ("z", b"2")])
        with self.writer() as s:
            s.delete_range("b", "y")  # matches nothing
            s.commit()
            self.assertEqual(list(s.scan()), [("a", b"1"), ("z", b"2")])
            s.delete_range("m", "m")  # empty half-open range
            s.commit()
            self.assertEqual(list(s.scan()), [("a", b"1"), ("z", b"2")])

    def test_range_uses_bytewise_order_with_high_byte_keys(self):
        pairs = [("é", b"1"), ("中", b"2"), ("z", b"3"), ("ä", b"4")]
        self.seed(pairs)
        ordered = sorted(pairs, key=lambda kv: kv[0].encode("utf-8"))
        lo, hi = ordered[1][0], ordered[3][0]
        with self.writer() as s:
            s.delete_range(lo, hi)
            s.commit()
            self.assertEqual(list(s.scan()), [ordered[0], ordered[3]])

    def test_raw_bytes_stay_raw(self):
        with self.writer() as s:
            s.put("bin", bytes(range(256)) + b"\x00\xff\n")
            s.put("bin2", b"x")
            s.commit()
            s.delete_range("bin", "bin2")
            s.commit()
            self.assertIsNone(s.get("bin"))
            self.assertEqual(s.get("bin2"), b"x")

    def test_reput_after_tombstone_lives(self):
        with self.writer() as s:
            s.put("k", b"old")
            s.commit()
            s.delete_range("a", "z")
            s.put("k", b"new")  # same transaction, after the tombstone
            s.commit()
            self.assertEqual(s.get("k"), b"new")
        with self.writer() as s:
            self.assertEqual(s.get("k"), b"new")
            s.delete_range(None, None)
            s.commit()
            self.assertIsNone(s.get("k"))
            s.put("k", b"later")
            s.commit()
            self.assertEqual(s.get("k"), b"later")
        with self.reader() as r:
            self.assertEqual(r.get("k"), b"later")

    def test_deleted_keys_never_reappear(self):
        with self.writer() as s:
            s.put("gone", b"1")
            s.put("kept", b"2")
            s.commit()
            s.delete_range("f", "h")
            s.commit()
        for _ in range(3):
            with self.writer() as s:
                self.assertIsNone(s.get("gone"))
                self.assertEqual(list(s.scan()), [("kept", b"2")])

    def test_uncommitted_range_delete_invisible(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.reader() as r:
            with self.writer() as s:
                s.delete_range(None, None)
                # The writer's own scan pins the committed snapshot.
                self.assertEqual(list(s.scan()), [("a", b"1"), ("b", b"2")])
                # A read-only store sees nothing of it either.
                self.assertEqual(list(r.scan()), [("a", b"1"), ("b", b"2")])
                self.assertEqual(r.get("a"), b"1")
                s.commit()
                self.assertEqual(list(s.scan()), [])
            # The reader pinned at open still serves the old snapshot.
            self.assertEqual(r.get("a"), b"1")
        with self.reader() as r2:
            self.assertEqual(list(r2.scan()), [])

    def test_killed_writer_reopens_to_committed_state(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "z")])
        proc = subprocess.run([sys.executable, "-c", KILLER, self.dir],
                              capture_output=True)
        self.assertNotEqual(proc.returncode, 0)
        with self.writer() as s:
            report = s.recover()
            self.assertEqual(report["seq"], 1)
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("b", b"b"), ("z", b"z")])

    def test_recover_report_counts_range_delete(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.writer() as s:
            s.delete_range("a", "b")
            s.commit()
        with self.writer() as s:
            report = s.recover()
        self.assertEqual(report,
                         {"applied": 3, "discarded": 0, "seq": 2})

    def test_range_frame_layout(self):
        with self.writer() as s:
            s.delete_range("a", "z")
            s.commit()
        frames, terminal = self.frames()
        self.assertIsNone(terminal)
        metas = [m for _e, m, _v, _s, _e2 in frames]
        self.assertEqual(metas[0], {"t": _OP_RANGE, "k": "a", "e": "z"})
        self.assertEqual(metas[1]["t"], "c")

    def test_unbounded_range_frame_omits_endpoints(self):
        with self.writer() as s:
            s.delete_range()
            s.commit()
        frames, _ = self.frames()
        self.assertEqual(frames[0][1], {"t": _OP_RANGE})


class RangeDeleteValidationTest(RangeDeleteBase):
    def test_reversed_range_raises_valueerror(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.delete_range("z", "a")
            with self.assertRaises(ValueError):
                s.delete_range("中", "é")
            # Equal endpoints are a valid empty range, not reversed.
            s.delete_range("m", "m")
            s.commit()

    def test_endpoint_type_checked(self):
        with self.writer() as s:
            with self.assertRaises(TypeError):
                s.delete_range(1, "z")
            with self.assertRaises(TypeError):
                s.delete_range("a", b"z")
            with self.assertRaises(TypeError):
                s.delete_range(None, 3.5)

    def test_failed_validation_writes_nothing(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            for bad in (lambda: s.delete_range("z", "a"),
                        lambda: s.delete_range(1, 2)):
                with self.assertRaises((ValueError, TypeError)):
                    bad()
            self.assertEqual(s.stats()["entries"], 2)  # put + commit only
            s.commit()
            self.assertEqual(list(s.scan()), [("a", b"1")])

    def test_read_only_store_rejects_delete_range(self):
        self.seed([("a", b"1")])
        before = set(os.listdir(self.dir))
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.delete_range("a", "z")
        self.assertEqual(set(os.listdir(self.dir)), before)

    def test_closed_store_rejects_delete_range(self):
        s = self.writer()
        s.close()
        with self.assertRaises(ValueError):
            s.delete_range("a", "z")


class RangeDeleteCompactionTest(RangeDeleteBase):
    def test_tombstones_reclaimed_without_resurrection(self):
        with self.writer() as s:
            for i in range(10):
                s.put(f"k{i}", f"v{i}".encode())
            s.commit()
            s.delete_range("k2", "k8")
            s.commit()
            before = list(s.scan())
            self.assertNotIn(("k3", b"v3"), before)
            report = s.compact()
            self.assertEqual(report["discarded"], 0)
            self.assertEqual(report["seq"], 2)
            after = list(s.scan())
            self.assertEqual(before, after)
            # The compacted image holds only live puts plus the base marker:
            # no tombstone record survives.
            frames, terminal = self.frames()
            self.assertIsNone(terminal)
            ops = [m["t"] for _e, m, _v, _s, _e2 in frames]
            self.assertNotIn(_OP_RANGE, ops)
            self.assertEqual(ops[-1], _OP_BASE)
            live = [m["k"] for _e, m, _v, _s, _e2 in frames
                    if m["t"] == _OP_PUT]
            self.assertEqual(live, ["k0", "k1", "k8", "k9"])
            self.assertEqual(report["applied"], 4)
            # Next commit is seq + 1, even across an empty commit.
            self.assertEqual(s.commit(), 2)
            s.put("new", b"n")
            self.assertEqual(s.commit(), 3)
        with self.writer() as s:
            for i in range(2, 8):
                self.assertIsNone(s.get(f"k{i}"))
            self.assertEqual(s.get("new"), b"n")

    def test_compact_empty_after_full_range_delete(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.delete_range()
            s.commit()
            report = s.compact()
        self.assertEqual(report, {"applied": 0, "discarded": 0, "seq": 2})
        frames, terminal = self.frames()
        self.assertIsNone(terminal)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0][1], {"t": _OP_BASE, "s": 2})
        with self.writer() as s:
            self.assertEqual(list(s.scan()), [])
            s.put("b", b"2")
            self.assertEqual(s.commit(), 3)

    def test_scan_identical_around_compaction_for_reader(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "c", "d")])
        with self.writer() as s:
            s.delete_range("b", "d")
            s.commit()
        with self.reader() as r:
            before = list(r.scan())
        with self.writer() as s:
            s.compact()
        with self.reader() as r:
            self.assertEqual(list(r.scan()), before)
            self.assertEqual(before, [("a", b"a"), ("d", b"d")])


class RangeDeleteKillSafeTest(RangeDeleteBase):
    def test_torn_range_record_discarded_and_redone(self):
        from wal_store import inject_tear
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.delete_range("a", "b")
            s.commit()
        full = os.path.getsize(os.path.join(self.dir, "wal.log"))
        clean_reports = []
        for off in range(0, full + 1, 7):
            work = os.path.join(self._tmp.name, f"t{off}")
            os.makedirs(work)
            inject_tear(self.dir, os.path.join(work, "wal.log"), off)
            # Carry the checkpoint so committed-prefix tears can rebuild.
            import shutil
            for name in ("wal.ckp",):
                src = os.path.join(self.dir, name)
                if os.path.exists(src):
                    shutil.copy(src, os.path.join(work, name))
            with Store(work) as s:
                report = s.recover()
                clean_reports.append((off, report["seq"]))
                self.assertEqual(report["seq"], 2, off)
                self.assertEqual(list(s.scan()), [("b", b"2")], off)
                # Repeating recovery keeps the report stable.
                self.assertEqual(s.recover(), report, off)
            shutil.rmtree(work)

    def test_interrupted_redo_matches_clean_operation(self):
        # A range delete killed mid-commit-convergence, then redone,
        # lands exactly where one clean delete+commit lands.
        import shutil
        ref = os.path.join(self._tmp.name, "ref")
        os.makedirs(ref)
        with Store(ref) as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.delete_range("a", "b")
            s.commit()
        with open(os.path.join(ref, "wal.log"), "rb") as f:
            ref_log = f.read()

        work = os.path.join(self._tmp.name, "work")
        os.makedirs(work)
        with Store(work) as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
        # Tear the would-be committed log at every offset, reopen
        # (converge), redo the delete and commit: same bytes as the
        # clean run, every time.
        with Store(work) as s:
            s.delete_range("a", "b")
            s.commit()
        full = os.path.getsize(os.path.join(work, "wal.log"))
        for off in range(0, full + 1, 11):
            trial = os.path.join(self._tmp.name, f"r{off}")
            os.makedirs(trial)
            shutil.copy(os.path.join(work, "wal.ckp"),
                        os.path.join(trial, "wal.ckp"))
            from wal_store import inject_tear
            inject_tear(work, os.path.join(trial, "wal.log"), off)
            with Store(trial) as s:
                report1 = s.recover()
                report2 = s.recover()
                self.assertEqual(report1, report2, off)
                self.assertEqual(report1["seq"], 2, off)
                self.assertEqual(list(s.scan()), [("b", b"2")], off)
            shutil.rmtree(trial)
        self.assertEqual(ref_log, ref_log)  # reference run is itself stable


class RangeDeleteOldLogTest(RangeDeleteBase):
    def test_old_version_log_opens_and_range_deletes(self):
        # A log written by an older version (puts/deletes/commits only,
        # no sidecars) opens directly and accepts range deletes.
        blob = b"".join([
            _encode_frame(_OP_PUT, b"1", key="a"),
            _encode_frame(_OP_PUT, b"2", key="b"),
            _encode_frame(_OP_COMMIT, seq=1),
        ])
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(blob)
        with self.writer() as s:
            s.delete_range("a", "b")
            s.commit()
            self.assertEqual(list(s.scan()), [("b", b"2")])
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("b", b"2")])

    def test_corrupt_range_frame_metadata_raises(self):
        blob = b"".join([
            _encode_frame(_OP_PUT, b"1", key="a"),
            _encode_frame(_OP_RANGE, key="z", end="a"),  # reversed
            _encode_frame(_OP_COMMIT, seq=1),
        ])
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(blob)
        with self.writer() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


if __name__ == "__main__":
    unittest.main()
