"""Tests for the write-ahead log store semantics."""

import os
import struct
import unittest
import zlib

from wal_store import Store
from wal_store import store as store_mod

HEADER = store_mod._HEADER
CRC = store_mod._CRC


def raw_record(kind, seq, key=b"", value=b"", corrupt_crc=False):
    header = HEADER.pack(store_mod._MAGIC, kind, seq, len(key), len(value))
    body = key + value
    checksum = zlib.crc32(header + body) & 0xFFFFFFFF
    if corrupt_crc:
        checksum ^= 0xFFFFFFFF
    return header + body + CRC.pack(checksum)


class StoreTest(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.dir = tempfile.mkdtemp(prefix="wal-test-")
        self.path = os.path.join(self.dir, "store")

    def tearDown(self):
        import shutil

        shutil.rmtree(self.dir, ignore_errors=True)

    def test_open_creates_directory(self):
        self.assertFalse(os.path.exists(self.path))
        store = Store(self.path)
        store.close()
        self.assertTrue(os.path.isdir(self.path))
        self.assertTrue(os.path.isfile(os.path.join(self.path, "wal.log")))

    def test_open_path_is_file_raises_oserror(self):
        file_path = os.path.join(self.dir, "plain-file")
        with open(file_path, "wb") as fh:
            fh.write(b"x")
        with self.assertRaises(OSError):
            Store(file_path)

    def test_put_get_commit_and_reopen(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.put("b", b"two")
        self.assertEqual(store.commit(), 1)
        self.assertEqual(store.get("a"), b"1")
        self.assertEqual(store.get("b"), b"two")
        store.close()

        reopened = Store(self.path)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertEqual(recovered := reopened.recover(), {"applied": 2, "seq": 1})
        reopened.close()

    def test_get_missing_returns_none(self):
        with Store(self.path) as store:
            self.assertIsNone(store.get("nope"))

    def test_validation(self):
        store = Store(self.path)
        self.addCleanup(store.close)

        for call in (
            lambda: store.put(1, b"v"),
            lambda: store.get(1),
            lambda: store.delete(1),
            lambda: store.put(None, b"v"),
        ):
            with self.assertRaises(TypeError):
                call()

        with self.assertRaises(TypeError):
            store.put("k", "not bytes")
        with self.assertRaises(TypeError):
            store.put("k", bytearray(b"v"))

        with self.assertRaises(ValueError):
            store.put("", b"v")
        with self.assertRaises(ValueError):
            store.get("")
        with self.assertRaises(ValueError):
            store.delete("")

    def test_empty_commit_is_noop(self):
        store = Store(self.path)
        self.addCleanup(store.close)
        self.assertEqual(store.commit(), 0)
        self.assertEqual(store.stats(), {"seq": 0, "entries": 0, "bytes": 0})
        store.put("k", b"v")
        self.assertEqual(store.commit(), 1)
        self.assertEqual(store.commit(), 1)  # no new seq, no error

    def test_sequence_strictly_increasing_no_gaps(self):
        store = Store(self.path)
        self.addCleanup(store.close)
        seqs = []
        for i in range(3):
            store.put(f"k{i}", str(i).encode())
            seqs.append(store.commit())
        self.assertEqual(seqs, [1, 2, 3])
        self.assertEqual(store.stats()["seq"], 3)

    def test_uncommitted_changes_vanish_after_reopen(self):
        store = Store(self.path)
        store.put("committed", b"yes")
        store.commit()
        store.put("dirty", b"no")
        store.delete("committed")
        # Process killed here: records for the dirty batch are on disk but
        # no commit record follows.
        store.close()

        reopened = Store(self.path)
        self.assertEqual(reopened.get("committed"), b"yes")
        self.assertIsNone(reopened.get("dirty"))
        report = reopened.recover()
        self.assertEqual(report, {"applied": 1, "seq": 1})
        # A fresh batch after recovery continues the sequence contiguously.
        reopened.put("dirty", b"now")
        self.assertEqual(reopened.commit(), 2)
        reopened.close()

        third = Store(self.path)
        self.assertEqual(third.get("committed"), b"yes")
        self.assertEqual(third.get("dirty"), b"now")
        third.close()

    def test_delete_missing_is_logged_and_committed(self):
        store = Store(self.path)
        store.delete("ghost")
        store.put("keep", b"x")
        self.assertEqual(store.commit(), 1)
        store.close()

        reopened = Store(self.path)
        self.assertIsNone(reopened.get("ghost"))
        self.assertEqual(reopened.get("keep"), b"x")
        self.assertEqual(reopened.recover()["seq"], 1)
        reopened.close()

    def test_delete_committed_key(self):
        store = Store(self.path)
        store.put("k", b"v")
        store.commit()
        store.delete("k")
        self.assertIsNone(store.get("k"))  # visible inside the session
        store.commit()
        store.close()

        reopened = Store(self.path)
        self.assertIsNone(recovered_get := reopened.get("k"))
        reopened.close()

    def test_staged_get_reflects_uncommitted_batch(self):
        store = Store(self.path)
        self.addCleanup(store.close)
        store.put("k", b"staged")
        self.assertEqual(store.get("k"), b"staged")
        store.delete("k")
        self.assertIsNone(store.get("k"))

    def test_recover_dirty_session_raises(self):
        store = Store(self.path)
        self.addCleanup(store.close)
        store.put("k", b"v")
        with self.assertRaises(ValueError):
            store.recover()
        # Dirty changes are still pending after the failed recover call.
        self.assertEqual(store.get("k"), b"v")
        self.assertEqual(store.commit(), 1)

    def test_repeated_recovery_is_identical(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.put("b", b"2")
        store.commit()
        store.close()

        reopened = Store(self.path)
        first = reopened.recover()
        second = reopened.recover()
        third = reopened.recover()
        self.assertEqual(first, second)
        self.assertEqual(second, third)
        self.assertEqual(first, {"applied": 2, "seq": 1})
        reopened.close()

    def test_torn_trailing_record_is_dropped(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.commit()  # seq 1, fully durable
        store.put("b", b"2")
        store.commit()  # seq 2
        store.close()

        log = os.path.join(self.path, "wal.log")
        size = os.path.getsize(log)
        with open(log, "r+b") as fh:
            fh.truncate(size - 7)  # chop the last record mid-body

        reopened = Store(self.path)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertIsNone(reopened.get("b"))
        self.assertEqual(reopened.recover(), {"applied": 1, "seq": 1})
        reopened.close()

    def test_stats_report(self):
        store = Store(self.path)
        store.put("key", b"value")
        store.commit()
        stats = store.stats()
        self.assertEqual(stats["seq"], 1)
        self.assertEqual(stats["entries"], 2)  # one mutation + one commit
        self.assertEqual(
            stats["bytes"], os.path.getsize(os.path.join(self.path, "wal.log"))
        )
        store.close()

    def _corrupt_log_then_reopen(self, crafted_tail):
        """Build a clean seq-1 log, append crafted bytes, reopen."""
        store = Store(self.path)
        store.put("a", b"1")
        store.commit()
        store.close()
        with open(os.path.join(self.path, "wal.log"), "ab") as fh:
            fh.write(crafted_tail)

    def test_seq_gap_raises_and_applies_nothing(self):
        # mutation claiming seq 3 right after durable seq 1 -> gap
        tail = raw_record(store_mod._KIND_PUT, 3, b"x", b"y")
        tail += raw_record(store_mod._KIND_COMMIT, 3)
        self._corrupt_log_then_reopen(tail)

        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.recover()
        # State and durable seq untouched by the failed replay.
        self.assertEqual(store.stats()["seq"], 1)
        self.assertEqual(store.get("a"), b"1")
        self.assertIsNone(store.get("x"))
        store.close()

    def test_duplicate_seq_raises(self):
        tail = raw_record(store_mod._KIND_PUT, 1, b"x", b"y")
        self._corrupt_log_then_reopen(tail)
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.recover()
        self.assertEqual(store.stats()["seq"], 1)
        store.close()

    def test_bad_crc_raises(self):
        tail = raw_record(
            store_mod._KIND_PUT, 2, b"x", b"y", corrupt_crc=True
        )
        tail += raw_record(store_mod._KIND_COMMIT, 2)
        self._corrupt_log_then_reopen(tail)
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.recover()
        self.assertEqual(store.get("a"), b"1")
        self.assertIsNone(store.get("x"))
        store.close()

    def test_bad_magic_raises(self):
        bad = bytearray(raw_record(store_mod._KIND_PUT, 2, b"x", b"y"))
        bad[0:2] = b"XX"
        self._corrupt_log_then_reopen(bytes(bad))
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.recover()
        store.close()

    def test_commit_without_batch_raises(self):
        tail = raw_record(store_mod._KIND_COMMIT, 2)
        self._corrupt_log_then_reopen(tail)
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.recover()
        self.assertEqual(store.stats()["seq"], 1)
        store.close()

    def test_binary_keys_and_values(self):
        store = Store(self.path)
        self.addCleanup(store.close)
        store.put("ünïcode-key", b"\x00\x01\xff\nraw")
        store.commit()
        store.close()

        reopened = Store(self.path)
        self.assertEqual(reopened.get("ünïcode-key"), b"\x00\x01\xff\nraw")
        reopened.close()

    def test_overwrite_and_delete_then_reopen(self):
        store = Store(self.path)
        store.put("k", b"v1")
        store.commit()
        store.put("k", b"v2")
        store.commit()
        store.delete("k")
        store.commit()
        store.close()

        reopened = Store(self.path)
        self.assertIsNone(reopened.get("k"))
        self.assertEqual(reopened.recover()["seq"], 3)
        reopened.close()

    def test_torn_header_is_dropped(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.commit()
        store.close()

        log = os.path.join(self.path, "wal.log")
        with open(log, "ab") as fh:
            fh.write(b"WL\x01\x00\x00\x00")  # fewer bytes than a header
        reopened = Store(self.path)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertEqual(reopened.recover(), {"applied": 1, "seq": 1})
        reopened.close()

    def test_header_declaring_more_bytes_than_file_holds_is_dropped(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.commit()
        store.close()

        # Valid magic/kind, seq 2, but klen=5000 while no body follows.
        bogus = HEADER.pack(store_mod._MAGIC, store_mod._KIND_PUT, 2, 5000, 0)
        log = os.path.join(self.path, "wal.log")
        with open(log, "ab") as fh:
            fh.write(bogus)
        reopened = Store(self.path)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertEqual(reopened.recover(), {"applied": 1, "seq": 1})
        reopened.close()

    def test_torn_commit_record_drops_whole_batch(self):
        store = Store(self.path)
        store.put("a", b"1")
        store.commit()
        store.close()

        # A complete uncommitted batch followed by a torn commit record.
        batch = raw_record(store_mod._KIND_PUT, 2, b"b", b"2")
        batch += raw_record(store_mod._KIND_DELETE, 2, b"a")
        commit = raw_record(store_mod._KIND_COMMIT, 2)
        log = os.path.join(self.path, "wal.log")
        with open(log, "ab") as fh:
            fh.write(batch + commit[:10])

        reopened = Store(self.path)
        self.assertEqual(reopened.get("a"), b"1")
        self.assertIsNone(reopened.get("b"))
        self.assertEqual(reopened.recover(), {"applied": 1, "seq": 1})
        reopened.close()

    def test_writes_refused_while_corrupt_tail_present(self):
        bad = bytearray(raw_record(store_mod._KIND_PUT, 2, b"x", b"y"))
        bad[0:2] = b"XX"
        self._corrupt_log_then_reopen(bytes(bad))
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.put("new", b"v")
        with self.assertRaises(ValueError):
            store.delete("a")
        # Reads of committed state keep working.
        self.assertEqual(store.get("a"), b"1")
        store.close()


if __name__ == "__main__":
    unittest.main()
