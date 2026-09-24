import os
import struct
import subprocess
import sys
import tempfile
import unittest

from wal_store import Store

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def log_path(self):
        return os.path.join(self.dir, "wal.log")

    def log_bytes(self):
        with open(self.log_path(), "rb") as fh:
            return fh.read()


class TestBasicOps(StoreTestCase):
    def test_put_get_roundtrip(self):
        store = Store(self.dir)
        store.put("a", b"1")
        self.assertEqual(store.get("a"), b"1")

    def test_get_missing_returns_none(self):
        store = Store(self.dir)
        self.assertIsNone(store.get("nope"))

    def test_delete_removes_key(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.delete("a")
        self.assertIsNone(store.get("a"))

    def test_delete_missing_key_is_noop(self):
        store = Store(self.dir)
        store.delete("nope")
        self.assertIsNone(store.get("nope"))

    def test_overwrite_keeps_last_value(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.put("a", b"2")
        self.assertEqual(store.get("a"), b"2")

    def test_overwrite_does_not_grow_key_count(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.put("a", b"2")
        store.commit()
        self.assertEqual(store.stats()["keys"], 1)

    def test_values_are_binary_safe(self):
        store = Store(self.dir)
        blob = bytes(range(256)) * 4
        store.put("blob", blob)
        store.commit()
        self.assertEqual(store.get("blob"), blob)

    def test_non_str_key_raises_type_error(self):
        store = Store(self.dir)
        for bad in (1, None, b"bytes", 3.14):
            with self.assertRaises(TypeError):
                store.put(bad, b"v")
            with self.assertRaises(TypeError):
                store.get(bad)
            with self.assertRaises(TypeError):
                store.delete(bad)

    def test_non_bytes_value_raises_type_error(self):
        store = Store(self.dir)
        for bad in ("text", 1, None, ["x"]):
            with self.assertRaises(TypeError):
                store.put("k", bad)

    def test_empty_key_raises_value_error(self):
        store = Store(self.dir)
        with self.assertRaises(ValueError):
            store.put("", b"v")
        with self.assertRaises(ValueError):
            store.get("")
        with self.assertRaises(ValueError):
            store.delete("")

    def test_missing_directory_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            Store(os.path.join(self.dir, "does-not-exist"))


class TestCommitSequence(StoreTestCase):
    def test_sequence_starts_at_one_and_increments_by_one(self):
        store = Store(self.dir)
        self.assertEqual(store.commit(), 1)
        self.assertEqual(store.commit(), 2)
        self.assertEqual(store.commit(), 3)

    def test_only_commit_advances_sequence(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.delete("a")
        store.get("a")
        self.assertEqual(store.stats()["sequence"], 0)
        self.assertEqual(store.commit(), 1)

    def test_sequence_survives_restart_without_gaps(self):
        store = Store(self.dir)
        store.put("a", b"1")
        self.assertEqual(store.commit(), 1)
        store.put("b", b"2")
        self.assertEqual(store.commit(), 2)

        reopened = Store(self.dir)
        reopened.recover()
        self.assertEqual(reopened.commit(), 3)


class TestRecovery(StoreTestCase):
    def test_recover_replays_committed_state(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.put("b", b"2")
        store.commit()
        store.delete("a")
        store.commit()

        reopened = Store(self.dir)
        report = reopened.recover()
        self.assertEqual(report["applied"], 3)
        self.assertEqual(report["sequence"], 2)
        self.assertIsNone(reopened.get("a"))
        self.assertEqual(reopened.get("b"), b"2")

    def test_uncommitted_changes_are_not_recovered(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        store.put("b", b"2")  # never committed
        store.delete("a")     # never committed

        reopened = Store(self.dir)
        report = reopened.recover()
        self.assertEqual(report["applied"], 1)
        self.assertEqual(report["sequence"], 1)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertIsNone(reopened.get("b"))

    def test_uncommitted_records_are_dropped_from_log(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        store.put("b", b"2")

        reopened = Store(self.dir)
        reopened.recover()
        # A later commit must not resurrect the dropped uncommitted put.
        reopened.put("c", b"3")
        reopened.commit()

        third = Store(self.dir)
        third.recover()
        self.assertIsNone(third.get("b"))
        self.assertEqual(third.get("a"), b"1")
        self.assertEqual(third.get("c"), b"3")

    def test_recover_empty_log(self):
        store = Store(self.dir)
        report = store.recover()
        self.assertEqual(report, {"applied": 0, "sequence": 0})
        self.assertEqual(store.stats()["keys"], 0)

    def test_recover_fresh_directory(self):
        store = Store(self.dir)
        self.assertEqual(store.recover()["applied"], 0)

    def test_truncated_tail_record_is_discarded(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        good_size = os.path.getsize(self.log_path())

        # Simulate a crash mid-write: partial header and partial payload.
        with open(self.log_path(), "ab") as fh:
            fh.write(b"\x00\x00\x00")
        reopened = Store(self.dir)
        report = reopened.recover()
        self.assertEqual(report["applied"], 1)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertEqual(os.path.getsize(self.log_path()), good_size)

    def test_truncated_tail_full_header_partial_payload(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        with open(self.log_path(), "ab") as fh:
            fh.write(struct.pack(">II", 12345, 100))
            fh.write(b"\x01\x00")
        reopened = Store(self.dir)
        self.assertEqual(reopened.recover()["applied"], 1)
        self.assertEqual(reopened.get("a"), b"1")

    def test_corrupt_middle_record_raises_value_error(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        store.put("b", b"2")
        store.commit()

        data = bytearray(self.log_bytes())
        data[10] ^= 0xFF  # flip a byte inside the first record's payload
        with open(self.log_path(), "wb") as fh:
            fh.write(data)

        reopened = Store(self.dir)
        with self.assertRaises(ValueError):
            reopened.recover()

    def test_crash_between_writes_recovers_last_commit(self):
        store = Store(self.dir)
        expected = {}
        for i in range(5):
            store.put(f"k{i}", f"v{i}".encode())
            expected[f"k{i}"] = f"v{i}".encode()
            store.commit()
        store.put("pending", b"x")  # killed before commit

        reopened = Store(self.dir)
        reopened.recover()
        for key, value in expected.items():
            self.assertEqual(reopened.get(key), value)
        self.assertIsNone(reopened.get("pending"))
        self.assertEqual(reopened.stats()["sequence"], 5)


class TestStats(StoreTestCase):
    def test_stats_reports_sequence_keys_and_bytes(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.put("b", b"2")
        store.commit()
        stats = store.stats()
        self.assertEqual(stats["sequence"], 1)
        self.assertEqual(stats["keys"], 2)
        self.assertEqual(stats["bytes"], os.path.getsize(self.log_path()))
        self.assertGreater(stats["bytes"], 0)

    def test_stats_after_delete(self):
        store = Store(self.dir)
        store.put("a", b"1")
        store.commit()
        store.delete("a")
        store.commit()
        self.assertEqual(store.stats()["keys"], 0)


class CliTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def run_cli(self, *argv):
        return subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", self.dir, *argv],
            capture_output=True,
            cwd=REPO_ROOT,
        )

    def test_put_get_roundtrip(self):
        value_file = os.path.join(self.dir, "value.bin")
        with open(value_file, "wb") as fh:
            fh.write(b"hello\x00world")
        result = self.run_cli("put", "k", "--value-file", value_file)
        self.assertEqual(result.returncode, 0, result.stderr)

        result = self.run_cli("get", "k")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"hello\x00world")

    def test_get_missing_key_exit_zero_empty_output(self):
        result = self.run_cli("get", "missing")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"")

    def test_put_missing_value_file_exit_one(self):
        result = self.run_cli("put", "k", "--value-file", os.path.join(self.dir, "nope"))
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, b"")

    def test_unknown_subcommand_exit_two(self):
        result = subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", self.dir, "frobnicate"],
            capture_output=True,
            cwd=REPO_ROOT,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"recover", result.stderr)
        self.assertIn(b"put", result.stderr)
        self.assertIn(b"get", result.stderr)

    def test_recover_and_stats_commands(self):
        result = self.run_cli("recover")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.run_cli("stats")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"sequence=0", result.stdout)

    def test_cli_sequence_continues_across_invocations(self):
        value_file = os.path.join(self.dir, "v")
        with open(value_file, "wb") as fh:
            fh.write(b"x")
        first = self.run_cli("put", "a", "--value-file", value_file)
        second = self.run_cli("put", "b", "--value-file", value_file)
        self.assertEqual(first.stdout.strip(), b"1")
        self.assertEqual(second.stdout.strip(), b"2")


if __name__ == "__main__":
    unittest.main()
