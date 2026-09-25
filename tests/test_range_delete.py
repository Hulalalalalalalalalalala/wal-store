"""Batch range deletes with byte-order tombstones.

These tests cover:

* ``Store.delete_range(start, end)`` and the ``delete(start, end)`` form
  deleting every committed key a ``scan(start, end)`` would return --
  start inclusive, end exclusive, either side unbounded, raw byte order;
* committed range tombstones removing the keys from both single-key reads
  and scans, including keys that only ever appear in delete history;
* keys written inside a previously deleted range returning their last
  committed value, with keys and values still raw bytes;
* uncommitted range deletes staying invisible to scans, the committed
  snapshot and read-only stores, and being lost entirely on a kill;
* shared endpoint validation with scans (``TypeError``/``ValueError``);
* compaction reclaiming the tombstones without resurrecting anything,
  the same snapshot scanning identically, the three-field report and the
  strict ``seq + 1`` rule (including empty commits);
* recovery counting the records, torn logs at every offset converging,
  and an interrupted range delete converging byte-identically on redo;
* corrupted range frames raising ``CorruptLogError`` and compacted
  images refusing to contain a tombstone.
"""

import io
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

from wal_store import CorruptLogError, Store, inject_tear
from wal_store.store import (
    _encode_frame,
    _iter_frames,
    _OP_BASE,
    _OP_RANGE,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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

    def seed(self, keys):
        with self.writer() as s:
            for key in keys:
                s.put(key, key.encode())
            s.commit()


class BasicRangeDeleteTest(RangeDeleteBase):
    def test_half_open_bytewise_range(self):
        self.seed(["a", "aa", "b", "c", "d"])
        with self.writer() as s:
            s.delete_range("aa", "c")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("c", b"c"), ("d", b"d")])
            self.assertIsNone(s.get("aa"))
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.get("a"), b"a")
            self.assertEqual(s.get("c"), b"c")
        with self.reader() as r:
            self.assertEqual(list(r.scan()),
                             [("a", b"a"), ("c", b"c"), ("d", b"d")])

    def test_unbounded_sides_and_full_range(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            s.delete_range(None, "b")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("b", b"b"), ("c", b"c")])
            s.delete_range("c")
            s.commit()
            self.assertEqual(list(s.scan()), [("b", b"b")])
            s.put("x", b"1")
            s.commit()
            s.delete_range()
            s.commit()
            self.assertEqual(list(s.scan()), [])
            self.assertIsNone(s.get("b"))

    def test_equal_and_absent_endpoints_delete_expected_keys(self):
        self.seed(["a", "b", "c", "d"])
        with self.writer() as s:
            # Equal endpoints: empty range, nothing deleted.
            s.delete_range("b", "b")
            s.commit()
            self.assertEqual(len(list(s.scan())), 4)
            # Absent start/end behave by byte order, exactly like scan:
            # "b" < "bb", so b survives; only c is inside [bb, cc).
            s.delete_range("bb", "cc")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("b", b"b"), ("d", b"d")])

    def test_delete_two_arg_form_matches_delete_range(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            s.delete("a", "c")
            s.commit()
            self.assertEqual(list(s.scan()), [("c", b"c")])
            s.delete("c")
            s.commit()
            self.assertEqual(list(s.scan()), [])

    def test_deletes_keys_that_only_existed_in_history(self):
        with self.writer() as s:
            s.put("gone", b"1")
            s.put("kept", b"2")
            s.commit()
            s.delete_range("g", "h")
            s.commit()
        with self.writer() as s:
            s.compact()
        with self.reader() as r:
            self.assertIsNone(r.get("gone"))
            self.assertEqual(list(r.scan()), [("kept", b"2")])

    def test_write_after_range_delete_keeps_new_value(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            s.delete_range("a", "z")
            s.put("b", b"new")
            s.commit()
            self.assertIsNone(s.get("a"))
            self.assertEqual(s.get("b"), b"new")
            self.assertIsNone(s.get("c"))
        with self.writer() as s:
            s.compact()
            s.put("a", b"rewritten")
            self.assertEqual(s.commit(), 3)
        with self.reader() as r:
            self.assertEqual(r.get("a"), b"rewritten")
            self.assertEqual(r.get("b"), b"new")
            self.assertIsNone(r.get("c"))

    def test_high_byte_order_and_empty_values(self):
        pairs = [("é", b"\xff\x00"), ("中", b""), ("z", b"a"),
                 ("ä", b"\x80\n")]
        with self.writer() as s:
            for key, value in pairs:
                s.put(key, value)
            s.commit()
            order = [k for k, _ in sorted(
                pairs, key=lambda kv: kv[0].encode("utf-8"))]
            # Byte order is z < ä < é < 中.
            s.delete_range(order[1], order[3])
            s.commit()
            got = dict(s.scan())
            self.assertEqual(set(got), {order[0], order[3]})
            self.assertEqual(got["中"], b"")


class UncommittedRangeDeleteTest(RangeDeleteBase):
    def test_invisible_to_scan_and_readers_before_commit(self):
        self.seed(["a", "b", "c"])
        with self.writer() as s:
            s.delete_range("a", "z")
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("b", b"b"), ("c", b"c")])
            with self.reader() as r:
                self.assertEqual(list(r.scan()),
                                 [("a", b"a"), ("b", b"b"), ("c", b"c")])
                self.assertEqual(r.get("a"), b"a")

    def test_mixed_pending_session_scans_committed_snapshot(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.delete_range(None, None)
            self.assertEqual(list(s.scan()), [("a", b"1")])
        with self.writer() as s:
            self.assertEqual(list(s.scan()), [("a", b"1")])

    KILLER = r"""
import sys
sys.path.insert(0, %r)
from wal_store import Store
with Store(sys.argv[1]) as s:
    for k in ("a", "b", "c"):
        s.put(k, b"v")
    s.commit()
    s.delete_range(None, None)
    __import__("os").kill(__import__("os").getpid(),
                          __import__("signal").SIGKILL)
""" % REPO_ROOT

    def test_killed_writer_reopens_at_last_committed_state(self):
        proc = subprocess.run(
            [sys.executable, "-c", self.KILLER, self.dir],
            capture_output=True)
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr)
        with self.writer() as s:
            self.assertEqual(sorted(k for k, _ in s.scan()),
                             ["a", "b", "c"])
            self.assertEqual(s.stats()["seq"], 1)
            self.assertEqual(
                s.recover(), {"applied": 3, "discarded": 0, "seq": 1})


class EndpointValidationTest(RangeDeleteBase):
    def test_reversed_endpoints_raise_valueerror(self):
        self.seed(["a", "z"])
        with self.writer() as s:
            before = s.stats()
            with self.assertRaises(ValueError):
                s.delete_range("z", "a")
            with self.assertRaises(ValueError):
                s.delete("中", "é")
            # Nothing was written or staged by the rejected calls.
            self.assertEqual(s.stats(), before)
            self.assertEqual(list(s.scan()),
                             [("a", b"a"), ("z", b"z")])

    def test_endpoint_types_checked(self):
        self.seed(["a"])
        with self.writer() as s:
            for bad in (1, b"a", 1.5, ["a"]):
                with self.assertRaises(TypeError):
                    s.delete_range(bad, "z")
                with self.assertRaises(TypeError):
                    s.delete_range("a", bad)

    def test_read_only_and_closed_rejection(self):
        self.seed(["a"])
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.delete_range("a", "b")
        s = self.writer()
        s.close()
        with self.assertRaises(ValueError):
            s.delete_range("a", "b")


class RecoveryRangeDeleteTest(RangeDeleteBase):
    def _build(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.put("c", b"3")
            s.commit()                        # seq 1
            s.delete_range("a", "c")
            s.commit()                        # seq 2
            s.put("d", b"4")
            s.delete_range(None, "b")
            s.commit()                        # seq 3
        return (open(os.path.join(self.dir, "wal.log"), "rb").read(),
                open(os.path.join(self.dir, "wal.ckp"), "rb").read())

    def test_recover_report_counts_range_records(self):
        self._build()
        with self.writer() as s:
            report = s.recover()
        # 4 puts + 2 range tombstones = 6 applied records.
        self.assertEqual(report,
                         {"applied": 6, "discarded": 0, "seq": 3})

    def test_torn_log_at_every_offset_converges(self):
        log, ckp = self._build()
        expected = {"c": b"3", "d": b"4"}
        for off in range(0, len(log) + 1, 17):
            work = os.path.join(self._tmp.name, f"o{off}")
            os.makedirs(work)
            with open(os.path.join(work, "wal.ckp"), "wb") as f:
                f.write(ckp)
            inject_tear(self.dir, os.path.join(work, "wal.log"), off)
            with Store(work) as s:
                state = {k: s.get(k) for k in ("a", "b", "c", "d")}
                state = {k: v for k, v in state.items() if v is not None}
                self.assertEqual(state, expected, off)
                self.assertEqual(s.stats()["seq"], 3, off)
                s.put("fresh", b"f")
                self.assertEqual(s.commit(), 4, off)
                self.assertIsNone(s.get("a"))
            shutil.rmtree(work)

    def test_uncommitted_torn_range_frame_dropped(self):
        self.seed(["a", "b"])
        raw = open(os.path.join(self.dir, "wal.log"), "rb").read()
        torn = _encode_frame(_OP_RANGE, start=None, end=None)
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(torn[:11])
        with self.writer() as s:
            self.assertEqual(s.get("a"), b"a")
            report = s.recover()
        self.assertEqual(report["discarded"], 1)
        self.assertEqual(
            open(os.path.join(self.dir, "wal.log"), "rb").read(), raw)


class CompactionRangeDeleteTest(RangeDeleteBase):
    def test_compaction_reclaims_tombstones_not_keys(self):
        with self.writer() as s:
            for i in range(20):
                s.put(f"k{i:02d}", b"x")
            s.commit()
            s.delete_range("k02", "k10")
            s.commit()
            before = list(s.scan())
            report = s.compact()
            after = list(s.scan())
        self.assertEqual(before, after)
        self.assertEqual(report["discarded"], 0)
        self.assertEqual(report["seq"], 2)
        self.assertEqual(report["applied"], len(before))
        raw = open(os.path.join(self.dir, "wal.log"), "rb").read()
        self.assertNotIn(b'"t":"r"', raw)
        # Only put frames then a single base marker.
        frames, terminal = self._scan(raw)
        self.assertIsNone(terminal)
        self.assertTrue(all(meta["t"] == "p" for meta, _v in frames[:-1]))
        self.assertEqual(frames[-1][0], {"t": _OP_BASE, "s": 2})

    @staticmethod
    def _scan(data):
        frames, terminal = [], None
        for event in _iter_frames(io.BytesIO(data)):
            if event[0] == "frame":
                _, meta, value, _s, _e = event
                frames.append((meta, value))
            else:
                terminal = event
        return frames, terminal

    def test_repeated_and_interrupted_compaction_byte_identical(self):
        with self.writer() as s:
            for i in range(24):
                s.put(f"k{i:02d}", b"x" * 40)
                s.commit()
            s.delete_range("k02", "k20")
            s.commit()
            expected = dict(s.scan())
            seq = s.stats()["seq"]
        ref = os.path.join(self._tmp.name, "ref")
        shutil.copytree(self.dir, ref)
        with Store(ref) as s:
            s.compact()
        ref_log = open(os.path.join(ref, "wal.log"), "rb").read()
        ref_ckp = open(os.path.join(ref, "wal.ckp"), "rb").read()

        with self.writer() as s:
            first = s.compact()
            second = s.compact()
        self.assertEqual(first, second)
        self.assertEqual(
            open(os.path.join(self.dir, "wal.log"), "rb").read(), ref_log)
        self.assertEqual(
            open(os.path.join(self.dir, "wal.ckp"), "rb").read(), ref_ckp)
        with self.writer() as s:
            self.assertEqual(dict(s.scan()), expected)
            self.assertEqual(s.commit(), seq + 1)      # empty commit advances
            s.put("n", b"n")
            self.assertEqual(s.commit(), seq + 2)

    def test_compact_candidate_with_range_frame_is_corrupt(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.delete_range(None, None)
            s.commit()
            image = (_encode_frame(_OP_RANGE, start=None, end=None)
                     + _encode_frame(_OP_BASE, seq=2))
            with open(os.path.join(self.dir, "wal.cmp"), "wb") as f:
                f.write(image)
            with self.assertRaises(CorruptLogError):
                s.recover()


# A range-delete committer that os._exit()s on the nth replace/fsync/etc.
CRASH_RANGE = r"""
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
    s.delete_range("b", "d")
    s.commit()
""" % REPO_ROOT


class InterruptedRangeDeleteTest(RangeDeleteBase):
    def test_redo_after_kill_converges_byte_identically(self):
        pre = os.path.join(self._tmp.name, "pre")
        os.makedirs(pre)
        with Store(pre) as s:
            for key in ("a", "b", "c", "d"):
                s.put(key, b"v")
            s.commit()

        clean = os.path.join(self._tmp.name, "clean")
        shutil.copytree(pre, clean)
        with Store(clean) as s:
            s.delete_range("b", "d")
            s.commit()
            expected = dict(s.scan())
        ref_log = open(os.path.join(clean, "wal.log"), "rb").read()
        ref_ckp = open(os.path.join(clean, "wal.ckp"), "rb").read()

        for point in range(1, 24):
            work = os.path.join(self._tmp.name, f"w{point}")
            shutil.rmtree(work, ignore_errors=True)
            shutil.copytree(pre, work)
            proc = subprocess.run(
                [sys.executable, "-c", CRASH_RANGE, work, str(point)],
                capture_output=True)
            self.assertIn(proc.returncode, (0, 9), proc.stderr)
            with Store(work) as s:
                report = s.recover()
                if report["seq"] == 1:
                    s.delete_range("b", "d")
                    s.commit()
                else:
                    self.assertEqual(report["seq"], 2)
            self.assertEqual(
                open(os.path.join(work, "wal.log"), "rb").read(), ref_log)
            self.assertEqual(
                open(os.path.join(work, "wal.ckp"), "rb").read(), ref_ckp)
            self.assertFalse(
                os.path.exists(os.path.join(work, "wal.rec")))
            with Store(work) as s:
                self.assertEqual(dict(s.scan()), expected)
                self.assertEqual(
                    s.recover(),
                    {"applied": 5, "discarded": 0, "seq": 2})
            shutil.rmtree(work)

    def test_chained_kills_then_redo_converges(self):
        pre = os.path.join(self._tmp.name, "pre2")
        os.makedirs(pre)
        with Store(pre) as s:
            for key in ("a", "b", "c", "d"):
                s.put(key, b"v")
            s.commit()

        clean = os.path.join(self._tmp.name, "clean2")
        shutil.copytree(pre, clean)
        with Store(clean) as s:
            s.delete_range("b", "d")
            s.commit()
            expected = dict(s.scan())
        ref_log = open(os.path.join(clean, "wal.log"), "rb").read()

        for chain in ((1, 4), (2, 2), (3, 1, 5), (7, 2, 4)):
            work = os.path.join(self._tmp.name, "chain")
            shutil.rmtree(work, ignore_errors=True)
            shutil.copytree(pre, work)
            for point in chain:
                with Store(work) as s:
                    if s.recover()["seq"] == 2:
                        break  # the commit already survived; nothing to redo
                subprocess.run(
                    [sys.executable, "-c", CRASH_RANGE, work,
                     str(point)], capture_output=True)
                # Reopen converges in between, exactly like a real retry.
                with Store(work) as s:
                    s.recover()
            with Store(work) as s:
                report = s.recover()
                if report["seq"] == 1:
                    s.delete_range("b", "d")
                    s.commit()
            self.assertEqual(
                open(os.path.join(work, "wal.log"), "rb").read(),
                ref_log, chain)
            with Store(work) as s:
                self.assertEqual(dict(s.scan()), expected)
            shutil.rmtree(work, ignore_errors=True)


class CorruptRangeFrameTest(RangeDeleteBase):
    def _committed(self, frames):
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(b"".join(frames))

    def test_range_frame_missing_endpoint_is_corrupt(self):
        import zlib
        for payload in (b'{"t":"r","s":"a"}\n',
                        b'{"t":"r","e":"b"}\n',
                        b'{"t":"r"}\n'):
            prefix = b"WAL2" + len(payload).to_bytes(8, "big")
            frame = (prefix + zlib.crc32(prefix).to_bytes(4, "big")
                     + payload + zlib.crc32(payload).to_bytes(4, "big"))
            d = os.path.join(self._tmp.name,
                             "c" + str(len(payload)))
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "wal.log"), "wb") as f:
                f.write(frame)
            with Store(d) as s:
                with self.assertRaises(CorruptLogError):
                    s.recover()

    def test_range_frame_bad_endpoint_type_is_corrupt(self):
        import zlib
        payload = b'{"t":"r","s":3,"e":null}\n'
        prefix = b"WAL2" + len(payload).to_bytes(8, "big")
        frame = (prefix + zlib.crc32(prefix).to_bytes(4, "big")
                 + payload + zlib.crc32(payload).to_bytes(4, "big"))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(frame)
        with self.writer() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


if __name__ == "__main__":
    unittest.main()
