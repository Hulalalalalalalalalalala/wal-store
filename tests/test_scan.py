"""Range scans with an ordered, snapshot-pinned cursor.

These tests cover:

* half-open ``[start, end)`` ranges in raw key-byte order, with ``None``
  endpoints leaving a side unbounded;
* the cursor pinning the committed snapshot at open -- later commits,
  compaction and recovery never change what it yields;
* deleted keys never appearing, overwritten keys yielding only the last
  committed value, and empty values showing up like any other;
* keys with high bytes ordered by raw bytes and returned unconverted;
* identical scan results before and after compaction, and a cursor that
  keeps reading after the old log space is reclaimed;
* read-only stores scanning without creating or modifying any file;
* the error conditions: inverted endpoints and reading a closed cursor
  both raise ``ValueError``, non-string endpoints raise ``TypeError``.
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


class RangeTest(ScanBase):
    def test_full_scan_in_byte_order(self):
        self.seed([("b", b"2"), ("a", b"1"), ("d", b"4"), ("c", b"3")])
        with self.writer() as s:
            self.assertEqual([k for k, _v in s.scan()], ["a", "b", "c", "d"])

    def test_half_open_range(self):
        self.seed([(k, k.encode()) for k in "abcdef"])
        with self.writer() as s:
            self.assertEqual([k for k, _v in s.scan("b", "e")],
                             ["b", "c", "d"])
            # The end key itself is excluded; the start key is included.
            self.assertEqual([k for k, _v in s.scan("b", "f")],
                             ["b", "c", "d", "e"])

    def test_open_ended_ranges(self):
        self.seed([(k, k.encode()) for k in "abcdef"])
        with self.writer() as s:
            self.assertEqual([k for k, _v in s.scan(start="d")],
                             ["d", "e", "f"])
            self.assertEqual([k for k, _v in s.scan(end="c")], ["a", "b"])
            self.assertEqual(len(list(s.scan(None, None))), 6)

    def test_range_between_absent_keys(self):
        self.seed([("a", b"1"), ("m", b"2"), ("z", b"3")])
        with self.writer() as s:
            self.assertEqual([k for k, _v in s.scan("b", "y")], ["m"])

    def test_equal_endpoints_yield_empty_range(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.writer() as s:
            self.assertEqual(list(s.scan("a", "a")), [])

    def test_empty_store_scans_empty(self):
        with self.writer() as s:
            self.assertEqual(list(s.scan()), [])
        r = self.reader()
        self.assertEqual(list(r.scan()), [])
        r.close()

    def test_inverted_endpoints_raise_valueerror(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            with self.assertRaises(ValueError):
                s.scan("z", "a")
            with self.assertRaises(ValueError):
                s.scan(start="b", end="a")
        r = self.reader()
        with self.assertRaises(ValueError):
            r.scan("z", "a")
        r.close()

    def test_non_string_endpoint_raises_typeerror(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            for bad in (1, b"a", 1.5, ["a"], object()):
                with self.subTest(bad=bad):
                    with self.assertRaises(TypeError):
                        s.scan(bad)
                    with self.assertRaises(TypeError):
                        s.scan(end=bad)

    def test_scan_on_closed_store_raises_valueerror(self):
        self.seed([("a", b"1")])
        s = self.writer()
        s.close()
        with self.assertRaises(ValueError):
            s.scan()


class ContentTest(ScanBase):
    def test_deleted_keys_never_appear(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.delete("a")
            s.commit()
            self.assertEqual([k for k, _v in s.scan()], ["b"])

    def test_delete_only_history_never_appears(self):
        with self.writer() as s:
            s.delete("ghost")
            s.commit()
            self.assertEqual(list(s.scan()), [])

    def test_overwritten_key_yields_last_committed_value(self):
        with self.writer() as s:
            for i in range(5):
                s.put("k", f"v{i}".encode())
                s.commit()
            self.assertEqual(list(s.scan()), [("k", b"v4")])

    def test_empty_values_appear(self):
        with self.writer() as s:
            s.put("empty", b"")
            s.put("full", b"x")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("empty", b""), ("full", b"x")])

    def test_uncommitted_changes_are_not_scanned(self):
        with self.writer() as s:
            s.put("a", b"committed")
            s.put("gone", b"here")
            s.commit()
            s.put("a", b"uncommitted")
            s.put("new", b"uncommitted")
            s.delete("gone")
            self.assertEqual(list(s.scan()),
                             [("a", b"committed"), ("gone", b"here")])
            # get still reflects the pending view; only scan pins committed.
            self.assertEqual(s.get("a"), b"uncommitted")
            s.commit()
            self.assertEqual(list(s.scan()),
                             [("a", b"uncommitted"), ("new", b"uncommitted")])

    def test_high_byte_keys_sorted_by_raw_bytes(self):
        keys = ["z", "é", "ü", "键", "a", "ÿ", "Ā"]
        self.seed([(k, k.encode("utf-8")) for k in keys])
        expect = sorted(keys, key=lambda k: k.encode("utf-8"))
        r = self.reader()
        got = [k for k, _v in r.scan()]
        r.close()
        self.assertEqual(got, expect)

    def test_raw_bytes_round_trip_unconverted(self):
        value = bytes(range(256)) + b"\x00\xff\nbinary"
        self.seed([("bin", value)])
        r = self.reader()
        self.assertEqual(list(r.scan()), [("bin", value)])
        r.close()

    def test_values_are_the_stored_bytes(self):
        self.seed([("k", b"v")])
        with self.writer() as s:
            for _key, value in s.scan():
                self.assertIsInstance(value, bytes)


class SnapshotPinningTest(ScanBase):
    def test_cursor_immune_to_later_commits(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.writer() as s:
            cursor = s.scan()
            s.put("c", b"3")
            s.delete("a")
            s.commit()
            self.assertEqual(list(cursor), [("a", b"1"), ("b", b"2")])
            # A fresh cursor sees the new committed state.
            self.assertEqual(list(s.scan()), [("b", b"2"), ("c", b"3")])

    def test_cursor_survives_compaction_and_reclaim(self):
        with self.writer() as s:
            for i in range(10):
                s.put(f"k{i}", f"v{i}".encode())
                s.commit()
            cursor = s.scan("k2", "k5")
            before = [("k2", b"v2"), ("k3", b"v3"), ("k4", b"v4")]
            s.compact()
            # Old log space is reclaimed; the pinned cursor reads on.
            self.assertEqual(list(cursor), before)
            # A fresh scan of the same committed state is byte-identical.
            self.assertEqual(list(s.scan("k2", "k5")), before)
            self.assertEqual(
                list(s.scan()),
                [(f"k{i}", f"v{i}".encode()) for i in range(10)])

    def test_same_state_scans_identically_around_compaction(self):
        with self.writer() as s:
            s.put("x", b"1")
            s.put("y", b"2")
            s.commit()
            s.delete("x")
            s.put("y", b"2 rewritten")
            s.put("z", b"3")
            s.commit()
            before = list(s.scan())
            s.compact()
            self.assertEqual(list(s.scan()), before)
        with self.writer() as s:
            self.assertEqual(list(s.scan()), before)

    def test_reader_cursor_pinned_across_writer_commits(self):
        self.seed([("k", b"v1")])
        r = self.reader()
        cursor = r.scan()
        with self.writer() as s:
            s.put("k", b"v2")
            s.put("k2", b"new")
            s.commit()
        self.assertEqual(list(cursor), [("k", b"v1")])
        self.assertEqual(list(r.scan()), [("k", b"v1")])
        r.close()
        r2 = self.reader()
        self.assertEqual(list(r2.scan()), [("k", b"v2"), ("k2", b"new")])
        r2.close()

    def test_reader_scan_survives_compaction(self):
        self.seed([("a", b"1"), ("b", b"2"), ("c", b"3")])
        r = self.reader()
        cursor = r.scan("a", "c")
        with self.writer() as s:
            s.compact()
        self.assertEqual(list(cursor), [("a", b"1"), ("b", b"2")])
        r.close()

    def test_reader_scan_creates_and_modifies_nothing(self):
        self.seed([("a", b"1")])
        before = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                before[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        r = self.reader()
        self.assertEqual(list(r.scan()), [("a", b"1")])
        r.close()
        after = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                after[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        self.assertEqual(after, before)


class CursorProtocolTest(ScanBase):
    def test_reading_closed_cursor_raises_valueerror(self):
        self.seed([("a", b"1"), ("b", b"2")])
        with self.writer() as s:
            cursor = s.scan()
            self.assertEqual(next(cursor), ("a", b"1"))
            cursor.close()
            with self.assertRaises(ValueError):
                next(cursor)
            with self.assertRaises(ValueError):
                list(cursor)
            # Closing twice is fine.
            cursor.close()

    def test_exhausted_cursor_stops_normally(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            cursor = s.scan()
            next(cursor)
            with self.assertRaises(StopIteration):
                next(cursor)

    def test_cursor_is_context_manager(self):
        self.seed([("a", b"1")])
        with self.writer() as s:
            with s.scan() as cursor:
                self.assertIsInstance(cursor, ScanCursor)
                self.assertEqual(list(cursor), [("a", b"1")])
            with self.assertRaises(ValueError):
                next(cursor)

    def test_cursor_independent_of_store_close(self):
        self.seed([("a", b"1")])
        s = self.writer()
        cursor = s.scan()
        s.close()
        self.assertEqual(list(cursor), [("a", b"1")])


class ScanHistoryTest(ScanBase):
    def test_old_version_directory_scans_directly(self):
        # A directory written before sidecars existed: a raw WAL2 log only.
        from wal_store.store import _encode_frame, _OP_COMMIT, _OP_PUT
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(_encode_frame(_OP_PUT, b"1", key="a"))
            f.write(_encode_frame(_OP_PUT, b"2", key="b"))
            f.write(_encode_frame(_OP_COMMIT, seq=1))
        with self.writer() as s:
            self.assertEqual(list(s.scan()), [("a", b"1"), ("b", b"2")])
        r = self.reader()
        self.assertEqual(list(r.scan("b")), [("b", b"2")])
        r.close()

    def test_scan_reflects_recovered_state_after_torn_tail(self):
        from wal_store.store import _encode_frame, _OP_PUT
        self.seed([("a", b"1")])
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"torn", key="b")[:9])
        with self.writer() as s:
            # The torn record is discarded; it never reaches a scan.
            self.assertEqual(list(s.scan()), [("a", b"1")])


if __name__ == "__main__":
    unittest.main()
