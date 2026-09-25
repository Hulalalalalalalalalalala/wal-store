"""Range scans with an ordered, snapshot-pinned cursor.

These tests cover:

* ``Store.scan(start, end)`` yielding ``(key, value)`` pairs in bytewise
  key order, start inclusive, end exclusive, either endpoint omittable;
* snapshot semantics: deleted keys never appear, overwritten keys yield
  only the last committed value, empty values scan normally, uncommitted
  writer changes are invisible;
* keys and values with high bytes ordered and returned as raw bytes with
  no encoding conversion;
* the cursor pinning its snapshot at open: later commits, compaction,
  recovery and log-space reclamation never change what it yields;
* identical scan sequences before and after compaction of the same
  snapshot, for both the writer and read-only opens;
* the error surface: reversed endpoints and reads from a closed cursor
  raise ``ValueError``;
* read-only scans creating, modifying and deleting nothing on disk,
  including directories written by older versions (log only, no sidecars).
"""

import os
import tempfile
import unittest

from wal_store import ScanCursor, Store


class ScanBase(unittest.TestCase):
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

    def listing(self):
        out = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                out[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        return out


class RangeTest(ScanBase):
    def test_full_scan_is_bytewise_ordered(self):
        self.seed([("b", b"2"), ("a", b"1"), ("aa", b"11"), ("c", b"3")])
        with self.reader() as r:
            self.assertEqual(
                list(r.scan()),
                [("a", b"1"), ("aa", b"11"), ("b", b"2"), ("c", b"3")])

    def test_start_inclusive_end_exclusive(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "c", "d")])
        with self.reader() as r:
            self.assertEqual(list(r.scan("b", "d")),
                             [("b", b"b"), ("c", b"c")])
            # A start key that is absent begins at the next key.
            self.assertEqual(list(r.scan("bb", "d")), [("c", b"c")])
            # An end key that is absent stops before the next key.
            self.assertEqual(list(r.scan("a", "cc")),
                             [("a", b"a"), ("b", b"b"), ("c", b"c")])

    def test_open_ended_ranges(self):
        self.seed([(k, k.encode()) for k in ("a", "b", "c")])
        with self.reader() as r:
            self.assertEqual(list(r.scan("b")), [("b", b"b"), ("c", b"c")])
            self.assertEqual(list(r.scan(None, "b")), [("a", b"a")])
            self.assertEqual(list(r.scan(None, None)),
                             [("a", b"a"), ("b", b"b"), ("c", b"c")])

    def test_equal_endpoints_yield_nothing(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            self.assertEqual(list(r.scan("a", "a")), [])

    def test_empty_store_scans_empty(self):
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [])
        with self.writer() as s:
            self.assertEqual(list(s.scan()), [])

    def test_empty_values_appear(self):
        self.seed([("empty", b""), ("full", b"x")])
        with self.reader() as r:
            self.assertEqual(list(r.scan()),
                             [("empty", b""), ("full", b"x")])

    def test_deleted_keys_never_appear(self):
        with self.writer() as s:
            s.put("gone", b"1")
            s.put("kept", b"2")
            s.commit()
            s.delete("gone")
            s.commit()
            # A key that exists only in put+delete history is absent.
            self.assertEqual(list(s.scan()), [("kept", b"2")])
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("kept", b"2")])

    def test_key_deleted_in_every_generation_never_appears(self):
        with self.writer() as s:
            s.put("x", b"1")
            s.commit()
            s.delete("x")
            s.commit()
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [])

    def test_overwritten_key_yields_last_committed_value(self):
        with self.writer() as s:
            for i in range(5):
                s.put("k", f"v{i}".encode())
                s.commit()
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("k", b"v4")])

    def test_high_byte_keys_and_values_as_raw_bytes(self):
        pairs = [("é", b"\xff\x00"), ("中", b"\x80\n"), ("z", b"a"),
                 ("ä", b"")]
        self.seed(pairs)
        expect = sorted(pairs, key=lambda kv: kv[0].encode("utf-8"))
        with self.reader() as r:
            self.assertEqual(list(r.scan()), expect)
            # Ranges compare by raw UTF-8 bytes too.
            lo, hi = expect[1][0], expect[3][0]
            self.assertEqual(list(r.scan(lo, hi)), expect[1:3])


class SnapshotPinTest(ScanBase):
    def test_writer_cursor_hides_uncommitted_changes(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.put("c", b"3")      # uncommitted new key
            s.put("a", b"1x")     # uncommitted overwrite
            s.delete("b")         # uncommitted delete
            self.assertEqual(list(s.scan()), [("a", b"1"), ("b", b"2")])
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("a", b"1x"), ("c", b"3")])

    def test_cursor_pins_snapshot_across_later_commits(self):
        with self.writer() as s:
            s.put("k", b"v1")
            s.commit()
            cursor = s.scan()
            s.put("k", b"v2")
            s.put("new", b"n")
            s.commit()
            self.assertEqual(list(cursor), [("k", b"v1")])
        # A fresh cursor sees the new committed state.
        with self.writer() as s:
            self.assertEqual(list(s.scan()),
                             [("k", b"v2"), ("new", b"n")])

    def test_cursor_survives_store_close(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            cursor = s.scan()
        self.assertEqual(list(cursor), [("a", b"1")])

    def test_reader_cursor_unaffected_by_writer_commits(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            cursor = r.scan()
            with self.writer() as s:
                s.put("b", b"2")
                s.commit()
            self.assertEqual(list(cursor), [("a", b"1")])
            self.assertEqual(list(r.scan()), [("a", b"1")])


class CompactionScanTest(ScanBase):
    def test_same_sequence_before_and_after_compaction(self):
        with self.writer() as s:
            for i in range(10):
                s.put(f"k{i}", f"v{i}".encode())
            s.commit()
            s.delete("k3")
            s.put("k4", b"overwritten")
            s.commit()
            before = list(s.scan())
            report = s.compact()
            self.assertEqual(report["seq"], 2)
            after = list(s.scan())
            self.assertEqual(before, after)
            self.assertNotIn(("k3", b"v3"), after)
            self.assertIn(("k4", b"overwritten"), after)
        with self.reader() as r:
            self.assertEqual(list(r.scan()), before)

    def test_cursor_opened_before_compaction_keeps_its_snapshot(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            cursor = s.scan()
            s.put("c", b"3")
            s.commit()
            s.compact()  # reclaims the old log space the cursor predates
            self.assertEqual(list(cursor), [("a", b"1"), ("b", b"2")])

    def test_reader_cursor_survives_compaction_and_reclaim(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.reader() as r:
            cursor = r.scan()
            with self.writer() as s:
                s.put("c", b"3")
                s.commit()
                s.compact()
            # Old space was reclaimed; the pinned cursor still reads fine.
            self.assertEqual(list(cursor), [("a", b"1"), ("b", b"2")])
        with self.reader() as r2:
            self.assertEqual(list(r2.scan()),
                             [("a", b"1"), ("b", b"2"), ("c", b"3")])

    def test_scan_after_recovery_matches_committed_state(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            s.put("b", b"2")  # never committed: a crash remnant
        with self.writer() as s:
            report = s.recover()
            self.assertEqual(report["seq"], 1)
            self.assertEqual(list(s.scan()), [("a", b"1")])


class ErrorTest(ScanBase):
    def test_reversed_endpoints_raise_valueerror(self):
        self.seed([("a", b"1"), ("z", b"2")])
        with self.reader() as r:
            with self.assertRaises(ValueError):
                r.scan("z", "a")
            # Reversed by raw byte order even for high-byte keys.
            with self.assertRaises(ValueError):
                r.scan("中", "é")
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan("b", "a")

    def test_endpoint_type_checked(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            with self.assertRaises(TypeError):
                r.scan(1)
            with self.assertRaises(TypeError):
                r.scan(None, b"a")

    def test_reading_closed_cursor_raises_valueerror(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            cursor = r.scan()
            cursor.close()
            with self.assertRaises(ValueError):
                next(cursor)
            with self.assertRaises(ValueError):
                list(cursor)
            # Closing twice is harmless.
            cursor.close()
            self.assertTrue(cursor.closed)

    def test_cursor_closed_by_context_manager(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            with r.scan() as cursor:
                self.assertIsInstance(cursor, ScanCursor)
            with self.assertRaises(ValueError):
                next(cursor)

    def test_scan_on_closed_store_raises_valueerror(self):
        self.seed([("a", b"1")])
        r = self.reader()
        r.close()
        with self.assertRaises(ValueError):
            r.scan()
        s = self.writer()
        s.close()
        with self.assertRaises(ValueError):
            s.scan()

    def test_exhaustion_is_stopiteration_not_an_error(self):
        self.seed([("a", b"1")])
        with self.reader() as r:
            cursor = r.scan()
            self.assertEqual(next(cursor), ("a", b"1"))
            with self.assertRaises(StopIteration):
                next(cursor)


class ReadOnlyScanTest(ScanBase):
    def test_reader_scan_changes_nothing_on_disk(self):
        self.seed([("a", b"1"), ("b", b"2")])
        before = self.listing()
        with self.reader() as r:
            self.assertEqual(len(list(r.scan())), 2)
            with r.scan() as cursor:
                next(cursor)
        self.assertEqual(self.listing(), before)

    def test_old_version_directory_scans_without_conversion(self):
        # A directory written by an older version: only wal.log, none of
        # the sidecar files. It opens and scans directly, and a read-only
        # scan introduces no new files.
        self.seed([("a", b"1"), ("b", b"2")])
        for name in ("wal.ckp", "wal.rec", "wal.cmp"):
            try:
                os.unlink(os.path.join(self.dir, name))
            except FileNotFoundError:
                pass
        self.assertEqual(set(os.listdir(self.dir)), {"wal.log"})
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("a", b"1"), ("b", b"2")])
        self.assertEqual(set(os.listdir(self.dir)), {"wal.log"})


class BinaryLogTest(ScanBase):
    def test_log_bytes_hold_raw_newlines_unexpanded(self):
        # Values with newlines must land on disk byte-for-byte: no LF to
        # CRLF expansion anywhere in the log, so the same history occupies
        # the same bytes on every platform.
        value = b"line1\nline2\n"
        with self.writer() as s:
            s.put("k", value)
            s.commit()
        with open(os.path.join(self.dir, "wal.log"), "rb") as f:
            raw = f.read()
        self.assertIn(value, raw)
        self.assertNotIn(b"\r\n", raw)
        with open(os.path.join(self.dir, "wal.ckp"), "rb") as f:
            self.assertNotIn(b"\r\n", f.read())
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("k", value)])


if __name__ == "__main__":
    unittest.main()
