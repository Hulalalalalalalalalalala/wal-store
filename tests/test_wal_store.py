import json
import os
import struct
import subprocess
import sys
import tempfile
import unittest
import zlib
from pathlib import Path

from wal_store import Store
from wal_store.store import LOG_NAME

HEADER = struct.Struct(">BII")


def record(rtype, payload):
    crc = zlib.crc32(bytes([rtype]) + payload) & 0xFFFFFFFF
    return HEADER.pack(rtype, len(payload), crc) + payload


def put_record(key, value):
    k = key.encode("utf-8")
    return record(1, struct.pack(">I", len(k)) + k + value)


def commit_record(seq):
    return record(3, struct.pack(">Q", seq))


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name) / "store"
        self.dir.mkdir()

    def open(self):
        return Store(self.dir)

    def test_missing_directory_raises(self):
        with self.assertRaises(FileNotFoundError):
            Store(Path(self.tmp.name) / "nope")

    def test_put_get_commit_sequence(self):
        with self.open() as s:
            s.put("a", b"1")
            self.assertEqual(s.get("a"), b"1")
            self.assertEqual(s.commit(), 1)
            s.put("b", b"2")
            self.assertEqual(s.commit(), 2)
            self.assertEqual(s.stats()["sequence"], 2)

    def test_get_missing_and_deleted_return_none(self):
        with self.open() as s:
            self.assertIsNone(s.get("never"))
            s.put("x", b"v")
            s.commit()
            s.delete("x")
            self.assertIsNone(s.get("x"))
            s.commit()

    def test_overwrite_keeps_key_count(self):
        with self.open() as s:
            s.put("k", b"1")
            s.put("k", b"2")
            s.put("k", b"3")
            s.commit()
            self.assertEqual(s.get("k"), b"3")
            self.assertEqual(s.stats()["keys"], 1)

    def test_type_and_value_validation(self):
        with self.open() as s:
            for bad in (1, None, b"bytes", 1.5):
                with self.assertRaises(TypeError):
                    s.put(bad, b"v")
                with self.assertRaises(TypeError):
                    s.get(bad)
                with self.assertRaises(TypeError):
                    s.delete(bad)
            with self.assertRaises(ValueError):
                s.put("", b"v")
            with self.assertRaises(ValueError):
                s.get("")
            with self.assertRaises(ValueError):
                s.delete("")
            for bad in ("str", 1, None, bytearray(b"x")):
                with self.assertRaises(TypeError):
                    s.put("k", bad)

    def test_recovery_after_reopen(self):
        with self.open() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.put("c", b"3")
            s.commit()
        with self.open() as s:
            report = s.recover()
            self.assertEqual(report, {"applied": 3, "sequence": 2})
            self.assertEqual(s.get("a"), b"1")
            self.assertEqual(s.get("b"), b"2")
            self.assertEqual(s.get("c"), b"3")

    def test_uncommitted_changes_not_recovered(self):
        with self.open() as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")
            s.delete("a")
        with self.open() as s:
            report = s.recover()
            self.assertEqual(report, {"applied": 1, "sequence": 1})
            self.assertEqual(s.get("a"), b"1")
            self.assertIsNone(s.get("b"))

    def test_sequence_continues_after_restart(self):
        with self.open() as s:
            s.put("a", b"1")
            self.assertEqual(s.commit(), 1)
        with self.open() as s:
            s.recover()
            s.put("b", b"2")
            self.assertEqual(s.commit(), 2)

    def test_uncommitted_tail_does_not_resurface(self):
        with self.open() as s:
            s.put("a", b"1")
            s.commit()
            s.put("ghost", b"g")
        with self.open() as s:
            s.recover()
            s.put("b", b"2")
            s.commit()
        with self.open() as s:
            s.recover()
            self.assertIsNone(s.get("ghost"))
            self.assertEqual(s.get("b"), b"2")

    def test_empty_log_recovers_cleanly(self):
        with self.open() as s:
            report = s.recover()
            self.assertEqual(report, {"applied": 0, "sequence": 0})
            self.assertEqual(s.stats()["keys"], 0)

    def test_truncated_tail_silently_dropped(self):
        log = self.dir / LOG_NAME
        log.write_bytes(put_record("a", b"1") + commit_record(1))
        with log.open("ab") as fh:
            fh.write(put_record("b", b"2")[:7])  # torn write
        with self.open() as s:
            report = s.recover()
            self.assertEqual(report, {"applied": 1, "sequence": 1})
            self.assertEqual(s.get("a"), b"1")

    def test_corrupt_record_raises_and_applies_nothing(self):
        log = self.dir / LOG_NAME
        good = put_record("a", b"1") + commit_record(1)
        bad = bytearray(put_record("b", b"2"))
        bad[-1] ^= 0xFF  # break the checksum
        log.write_bytes(good + bytes(bad) + commit_record(2))
        with self.open() as s:
            with self.assertRaises(ValueError):
                s.recover()
            self.assertIsNone(s.get("a"))
            self.assertEqual(s.stats()["sequence"], 0)

    def test_stats_bytes_matches_log_size(self):
        with self.open() as s:
            s.put("a", b"1")
            s.commit()
            self.assertEqual(
                s.stats()["bytes"], (self.dir / LOG_NAME).stat().st_size
            )


class CliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name) / "store"
        self.dir.mkdir()

    def run_cli(self, *argv):
        return subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", str(self.dir), *argv],
            capture_output=True,
            cwd=Path(__file__).resolve().parent.parent,
        )

    def test_put_get_roundtrip(self):
        value_file = Path(self.tmp.name) / "value.bin"
        value_file.write_bytes(b"hello \x00 world")
        r = self.run_cli("put", "k", "--value-file", str(value_file))
        self.assertEqual(r.returncode, 0, r.stderr)
        r = self.run_cli("get", "k")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, b"hello \x00 world")

    def test_get_missing_key_empty_output_zero_exit(self):
        r = self.run_cli("get", "absent")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, b"")

    def test_put_missing_value_file(self):
        r = self.run_cli("put", "k", "--value-file",
                         str(Path(self.tmp.name) / "nope"))
        self.assertEqual(r.returncode, 1)
        self.assertTrue(r.stderr)

    def test_unknown_command_exit_2(self):
        r = self.run_cli("frobnicate")
        self.assertEqual(r.returncode, 2)
        self.assertIn(b"recover", r.stderr)
        self.assertIn(b"put", r.stderr)
        self.assertIn(b"get", r.stderr)

    def test_recover_reports_json(self):
        value_file = Path(self.tmp.name) / "v"
        value_file.write_bytes(b"x")
        self.run_cli("put", "k", "--value-file", str(value_file))
        r = self.run_cli("recover")
        self.assertEqual(r.returncode, 0)
        report = json.loads(r.stdout)
        self.assertEqual(report, {"applied": 1, "sequence": 1})


if __name__ == "__main__":
    unittest.main()
