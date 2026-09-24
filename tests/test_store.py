"""Tests for store.py."""

import os
import signal
import subprocess
import sys
import tempfile
import unittest

from wal_store import CorruptLogError
from wal_store.store import Store, _encode_frame, _OP_COMMIT, _OP_DELETE, _OP_PUT

KILLER = r"""
import sys
sys.path.insert(0, %r)
from wal_store.store import Store

path = sys.argv[1]
with Store(path) as s:
    s.put("committed", b"yes")
    s.commit()
    s.put("uncommitted", b"no")
    s.delete("committed")
    os_kill = __import__("os").kill
    os_kill(__import__("os").getpid(), __import__("signal").SIGKILL)
"""


class StoreBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def log_bytes(self):
        with open(os.path.join(self.dir, "wal.log"), "rb") as f:
            return f.read()

    def reopen(self):
        return Store(self.dir)


class BasicOperationsTest(StoreBase):
    def test_open_missing_directory_raises_filenotfounderror(self):
        missing = os.path.join(self.dir, "deep", "store")
        with self.assertRaises(FileNotFoundError):
            Store(missing)
        # Nothing was created as a side effect.
        self.assertFalse(os.path.exists(os.path.join(self.dir, "deep")))

    def test_open_existing_directory_creates_log(self):
        path = os.path.join(self.dir, "store")
        os.makedirs(path)
        with Store(path) as s:
            self.assertEqual(s.get("x"), None)
        self.assertTrue(os.path.isfile(os.path.join(path, "wal.log")))

    def test_open_regular_file_raises_oserror(self):
        path = os.path.join(self.dir, "afile")
        with open(path, "wb") as f:
            f.write(b"junk")
        with self.assertRaises(OSError):
            Store(path)

    def test_put_get_commit_roundtrip(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.put("b", b"two")
            self.assertEqual(s.get("a"), b"1")
            self.assertEqual(s.commit(), 1)
            self.assertEqual(s.get("b"), b"two")

        with self.reopen() as s:
            self.assertEqual(s.get("a"), b"1")
            self.assertEqual(s.get("b"), b"two")
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 1})

    def test_raw_value_bytes_preserved(self):
        value = bytes(range(256)) + b"\x00\xff\nWAL1garbage\n\x00"
        with Store(self.dir) as s:
            s.put("bin", value)
            s.commit()
        with self.reopen() as s:
            self.assertEqual(s.get("bin"), value)

    def test_delete_missing_is_logged_without_error(self):
        with Store(self.dir) as s:
            s.put("k", b"v")
            s.delete("absent")
            seq = s.commit()
        self.assertEqual(seq, 1)
        with self.reopen() as s:
            self.assertEqual(s.get("k"), b"v")
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 1})

    def test_delete_committed_key(self):
        with Store(self.dir) as s:
            s.put("k", b"v")
            s.commit()
            s.delete("k")
            s.commit()
        with self.reopen() as s:
            self.assertIsNone(s.get("k"))
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 2})

    def test_get_missing_returns_none(self):
        with Store(self.dir) as s:
            self.assertEqual(s.get("nope"), None)

    def test_commit_without_pending_returns_seq(self):
        with Store(self.dir) as s:
            self.assertEqual(s.commit(), 0)
            s.put("k", b"v")
            s.commit()
            self.assertEqual(s.commit(), 1)
            self.assertEqual(s.commit(), 1)
        with self.reopen() as s:
            self.assertEqual(s.commit(), 1)

    def test_stats(self):
        with Store(self.dir) as s:
            self.assertEqual(s.stats(), {"seq": 0, "entries": 0,
                                         "bytes": 0})
            s.put("k", b"value")
            self.assertGreater(s.stats()["bytes"], 0)
            self.assertEqual(s.stats()["entries"], 1)
            s.commit()
            stats = s.stats()
        self.assertEqual(stats["seq"], 1)
        self.assertEqual(stats["entries"], 2)
        with self.reopen() as s:
            self.assertEqual(s.stats()["bytes"], stats["bytes"])
            self.assertEqual(s.stats()["entries"], 2)


class ValidationTest(StoreBase):
    def test_non_string_key_typeerror(self):
        with Store(self.dir) as s:
            for bad in (1, 1.5, b"k", None, ["k"], object()):
                with self.subTest(bad=bad):
                    with self.assertRaises(TypeError):
                        s.put(bad, b"v")
                    with self.assertRaises(TypeError):
                        s.get(bad)
                    with self.assertRaises(TypeError):
                        s.delete(bad)

    def test_empty_key_valueerror(self):
        with Store(self.dir) as s:
            with self.assertRaises(ValueError):
                s.put("", b"v")
            with self.assertRaises(ValueError):
                s.get("")
            with self.assertRaises(ValueError):
                s.delete("")

    def test_non_bytes_value_typeerror(self):
        with Store(self.dir) as s:
            for bad in ("v", 1, None, bytearray(b"v"), [1]):
                with self.subTest(bad=bad):
                    with self.assertRaises(TypeError):
                        s.put("k", bad)

    def test_validation_failure_applies_nothing(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        # Corrupt the log, then make sure a failed recover changes nothing.
        raw = self.log_bytes()
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(raw + b"WAL1\x00")  # torn header of a new frame
        with self.reopen() as s:
            # Torn tail is silently dropped at open, already clean now.
            self.assertEqual(s.get("a"), b"1")
            result = s.recover()
        self.assertEqual(result, {"applied": 1, "discarded": 1, "seq": 1})

    def test_recover_with_pending_raises(self):
        with Store(self.dir) as s:
            s.put("k", b"v")
            with self.assertRaises(ValueError):
                s.recover()
            s.delete("k")
            with self.assertRaises(ValueError):
                s.recover()


class RecoveryTest(StoreBase):
    def test_uncommitted_changes_dropped_on_reopen(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.delete("a")
        with self.reopen() as s:
            self.assertEqual(s.get("a"), b"1")
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 2, "seq": 1})

    def test_hard_kill_loses_only_uncommitted(self):
        code = KILLER % os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))
        proc = subprocess.run(
            [sys.executable, "-c", code, self.dir],
            capture_output=True)
        self.assertEqual(proc.returncode, -signal.SIGKILL,
                         proc.stderr)
        with Store(self.dir) as s:
            self.assertEqual(s.get("committed"), b"yes")
            self.assertIsNone(s.get("uncommitted"))
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 2, "seq": 1})

    def test_recover_is_idempotent(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.delete("a")
            s.put("a", b"2")
            s.commit()
        with self.reopen() as s:
            first = s.recover()
            second = s.recover()
            third = s.recover()
        self.assertEqual(first, {"applied": 3, "discarded": 0, "seq": 1})
        self.assertEqual(second, first)
        self.assertEqual(third, first)

    def test_recover_report_key_order(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        with self.reopen() as s:
            report = s.recover()
        self.assertEqual(list(report), ["applied", "discarded", "seq"])
        for value in report.values():
            self.assertIs(type(value), int)

    def test_multiple_reopens_identical(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.commit()
        expected = {"a": b"1", "b": b"2"}
        for _ in range(3):
            with Store(self.dir) as s:
                self.assertEqual(s.get("a"), b"1")
                self.assertEqual(s.get("b"), b"2")
                got = {k: s.get(k) for k in expected}
            self.assertEqual(got, expected)

    def test_torn_frame_tail_dropped(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        # Complete frame bytes without commit marker, then chop it mid-frame.
        frame = _encode_frame(_OP_PUT, b"torn-tail", key="b")
        for cut in (1, 4, 8, 12, len(frame) - 1):
            with open(os.path.join(self.dir, "wal.log"), "wb") as f:
                f.write(raw + frame[:cut])
            with self.reopen() as s:
                self.assertEqual(s.get("a"), b"1")
                self.assertIsNone(s.get("b"))
                self.assertEqual(s.recover(),
                                 {"applied": 1, "discarded": 1, "seq": 1})
            # The torn bytes were truncated.
            self.assertEqual(self.log_bytes(), raw)

    def test_fully_written_uncommitted_frame_dropped(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"x", key="b"))
        with self.reopen() as s:
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 1, "seq": 1})
        self.assertEqual(self.log_bytes(), raw)

    def test_log_with_no_commits_recovers_empty(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.put("b", b"2")
            # Never committed: everything is tail.
        with self.reopen() as s:
            self.assertIsNone(s.get("a"))
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.recover(),
                             {"applied": 0, "discarded": 2, "seq": 0})
            self.assertEqual(s.stats(), {"seq": 0, "entries": 0, "bytes": 0})

    def test_empty_log_recovers_empty_state(self):
        with Store(self.dir) as s:
            self.assertEqual(s.recover(),
                             {"applied": 0, "discarded": 0, "seq": 0})
            self.assertIsNone(s.get("anything"))

    def test_seq_continues_from_committed_after_recovery(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")  # uncommitted tail, dropped at next open
        with self.reopen() as s:
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 1, "seq": 1})
            s.put("c", b"3")
            self.assertEqual(s.commit(), 2)
        # Repeated restarts keep advancing; the sequence never regresses.
        for expected in (2, 3, 4):
            with self.reopen() as s:
                self.assertEqual(s.recover()["seq"], expected)
                s.put("x", b"y")
                self.assertEqual(s.commit(), expected + 1)

    def test_stats_ignore_tail_and_are_stable_across_restarts(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")  # uncommitted tail
        with self.reopen() as s:
            first = s.stats()
        self.assertEqual(first["seq"], 1)
        self.assertEqual(first["entries"], 2)
        for _ in range(3):
            with self.reopen() as s:
                self.assertEqual(s.stats(), first)

    def test_corrupt_committed_frame_raises(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        # Corrupt the CRC trailer of the committed frame.
        damaged = bytearray(raw)
        damaged[-1] ^= 0xFF
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()
            with self.assertRaises(CorruptLogError):
                s.recover()
        # Nothing was truncated past the error.
        self.assertEqual(self.log_bytes(), bytes(damaged))

    def test_corruptlogerror_is_a_valueerror(self):
        self.assertTrue(issubclass(CorruptLogError, ValueError))

    def test_damage_in_middle_is_corrupt_not_torn(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.commit()
        damaged = bytearray(self.log_bytes())
        # Flip a byte inside the first frame's payload; later frames are
        # intact, but damage before the tail is corruption, not a torn tail.
        damaged[13] ^= 0xFF
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()
        self.assertEqual(self.log_bytes(), bytes(damaged))

    def test_oversized_length_is_corrupt_not_torn(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        # A length field beyond the payload limit, even at the very end of
        # the log, is corruption rather than a torn record.
        bogus = b"WAL1" + ((1 << 40) + 1).to_bytes(8, "big")
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(raw + bogus)
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()
        self.assertEqual(self.log_bytes(), raw + bogus)

    def test_writes_blocked_while_corrupt(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        damaged = bytearray(raw)
        damaged[-1] ^= 0xFF
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with self.reopen() as s:
            with self.assertRaises(ValueError):
                s.put("b", b"2")
            with self.assertRaises(ValueError):
                s.delete("a")
            with self.assertRaises(ValueError):
                s.commit()
            # get still works against the (empty) in-memory view.
            self.assertIsNone(s.get("a"))
        self.assertEqual(self.log_bytes(), bytes(damaged))

    def test_sequence_gap_raises(self):
        frames = (
            _encode_frame(_OP_PUT, b"1", key="a")
            + _encode_frame(_OP_COMMIT, seq=1)
            + _encode_frame(_OP_PUT, b"2", key="b")
            + _encode_frame(_OP_COMMIT, seq=3))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(frames)
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_sequence_duplicate_raises(self):
        frames = (
            _encode_frame(_OP_PUT, b"1", key="a")
            + _encode_frame(_OP_COMMIT, seq=1)
            + _encode_frame(_OP_PUT, b"2", key="b")
            + _encode_frame(_OP_COMMIT, seq=1))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(frames)
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_sequence_does_not_start_at_one_raises(self):
        frames = (_encode_frame(_OP_PUT, b"1", key="a")
                  + _encode_frame(_OP_COMMIT, seq=2))
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(frames)
        with self.reopen() as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_recover_after_failure_is_unchanged(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        damaged = bytearray(raw)
        damaged[-1] ^= 0xFF
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(bytes(damaged))
        with self.reopen() as s:
            for _ in range(3):
                with self.assertRaises(CorruptLogError):
                    s.recover()
        self.assertEqual(self.log_bytes(), bytes(damaged))

    def test_seq_advances_strictly_across_commits(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            self.assertEqual(s.commit(), 1)
            s.put("b", b"2")
            self.assertEqual(s.commit(), 2)
            s.delete("a")
            self.assertEqual(s.commit(), 3)
        with self.reopen() as s:
            self.assertEqual(s.recover(),
                             {"applied": 3, "discarded": 0, "seq": 3})

    def test_recover_reflects_commits_in_same_session(self):
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 0, "seq": 1})
            s.put("b", b"2")
            s.commit()
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 2})

    def test_empty_then_dirty_tail_after_commit(self):
        # Commit, then dirty mutations, then torn bytes at the very end.
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        raw = self.log_bytes()
        dirty = _encode_frame(_OP_PUT, b"2", key="b")
        torn = _encode_frame(_OP_DELETE, key="a")
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(raw + dirty + torn[:7])
        with self.reopen() as s:
            self.assertEqual(s.get("a"), b"1")
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.recover(),
                             {"applied": 1, "discarded": 2, "seq": 1})
        self.assertEqual(self.log_bytes(), raw)


if __name__ == "__main__":
    unittest.main()
