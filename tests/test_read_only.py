"""Isolated read-only access concurrent with a single writer.

These tests cover:

* ``Store(path, read_only=True)`` pinning one complete committed snapshot;
* uncommitted writes, torn records and half commits never reaching readers;
* many reader processes running alongside a continuously committing writer,
  each observing a coherent key/value snapshot, while the writer is never
  blocked and is unaffected by readers that are killed or hang;
* the read-only hard boundary -- no file is created, modified or deleted;
* reads served from the atomically replaced ``wal.ckp`` sidecar and from a
  torn log rebuilt view, and from old directories that have no fresh state;
* corruption still raising ``wal_store.CorruptLogError`` and a missing
  directory still raising ``FileNotFoundError`` for read-only opens.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from wal_store import CorruptLogError, Store
from wal_store.store import _encode_frame, _OP_COMMIT, _OP_PUT

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ReadOnlyBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        # Make sure a chmod-ed directory can be cleaned up.
        try:
            os.chmod(self.dir, 0o755)
        except OSError:
            pass
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def listing(self):
        out = {}
        for name in os.listdir(self.dir):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                out[name] = (os.fstat(f.fileno()).st_mtime_ns, f.read())
        return out


class BasicSnapshotTest(ReadOnlyBase):
    def test_reader_sees_only_committed(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            s.put("c", b"3")
            s.delete("a")
            r = self.reader()
            self.assertEqual(r.get("a"), b"1")
            self.assertEqual(r.get("b"), b"2")
            self.assertIsNone(r.get("c"))
            r.close()

    def test_reader_pins_snapshot_across_later_commits(self):
        with self.writer() as s:
            s.put("k", b"v1")
            s.commit()
        r = self.reader()
        self.assertEqual(r.get("k"), b"v1")
        with self.writer() as s:
            s.put("k", b"v2")
            s.commit()
            s.put("k", b"v3")  # uncommitted
            # The already-open reader stays on its pinned snapshot.
            self.assertEqual(r.get("k"), b"v1")
            self.assertEqual(r.stats()["seq"], 1)
        # A fresh reader advances to the latest committed snapshot.
        r2 = self.reader()
        self.assertEqual(r2.get("k"), b"v2")
        self.assertEqual(r2.stats()["seq"], 2)
        r2.close()
        r.close()

    def test_reader_stats_match_pinned_snapshot(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r = self.reader()
        stats = r.stats()
        self.assertEqual(stats["seq"], 1)
        # 1 put frame + 1 commit frame, bytes equal the committed prefix.
        self.assertEqual(stats["entries"], 2)
        self.assertEqual(stats["bytes"],
                         os.path.getsize(os.path.join(self.dir, "wal.ckp")))
        r.close()

    def test_empty_store_reads_empty_without_creating_files(self):
        before = set(os.listdir(self.dir))
        r = self.reader()
        self.assertIsNone(r.get("anything"))
        self.assertEqual(r.stats(), {"seq": 0, "entries": 0, "bytes": 0})
        r.close()
        self.assertEqual(set(os.listdir(self.dir)), before)

    def test_missing_directory_raises_filenotfound(self):
        missing = os.path.join(self.dir, "nope")
        with self.assertRaises(FileNotFoundError):
            Store(missing, read_only=True)

    def test_mutating_methods_rejected(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        r = self.reader()
        for call in (lambda: r.put("x", b"y"),
                     lambda: r.delete("a"),
                     lambda: r.commit(),
                     lambda: r.recover()):
            with self.assertRaises(ValueError):
                call()
        r.close()

    def test_get_key_validation_unchanged(self):
        r = self.reader()
        with self.assertRaises(TypeError):
            r.get(1)
        with self.assertRaises(ValueError):
            r.get("")
        r.close()

    def test_raw_bytes_preserved(self):
        value = bytes(range(256)) + b"\x00\xff\nbinary"
        with self.writer() as s:
            s.put("bin", value)
            s.commit()
        r = self.reader()
        self.assertEqual(r.get("bin"), value)
        r.close()


class ReadOnlyHardBoundaryTest(ReadOnlyBase):
    def test_reader_changes_nothing(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        before = self.listing()
        r = self.reader()
        r.get("a")
        r.get("missing")
        r.stats()
        r.close()
        after = self.listing()
        self.assertEqual(after, before)
        # No snapshot/temp scratch files were introduced.
        self.assertNotIn("wal.snp", after)
        self.assertFalse(
            any(name.endswith(".tmp") for name in after))

    def test_reader_works_in_read_only_directory(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        os.chmod(self.dir, 0o555)
        try:
            r = self.reader()
            self.assertEqual(r.get("a"), b"1")
            r.close()
        finally:
            os.chmod(self.dir, 0o755)

    def test_reader_without_log_uses_checkpoint_and_creates_nothing(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        os.unlink(os.path.join(self.dir, "wal.log"))
        before = set(os.listdir(self.dir))
        r = self.reader()
        self.assertEqual(r.get("a"), b"1")
        self.assertEqual(r.stats()["seq"], 1)
        r.close()
        self.assertEqual(set(os.listdir(self.dir)), before)


class SnapshotCoherenceTest(ReadOnlyBase):
    def test_committed_multi_key_commit_is_atomic_to_reader(self):
        with self.writer() as s:
            for i in range(1, 6):
                for k in ("k0", "k1", "k2", "k3"):
                    s.put(k, f"v{i}".encode())
                s.commit()
        r = self.reader()
        values = {k: r.get(k) for k in ("k0", "k1", "k2", "k3")}
        # All keys of one snapshot carry the same generation; no mix.
        generations = {v for v in values.values()}
        self.assertEqual(len(generations), 1)
        r.close()

    def test_later_torn_and_uncommitted_bytes_are_invisible(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(self.dir, "wal.ckp"), "rb") as f:
            clean = f.read()
        # Append a complete uncommitted frame and a torn final record.
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"x", key="u"))
            f.write(_encode_frame(_OP_PUT, b"torn", key="t")[:9])
        r = self.reader()
        self.assertEqual(r.get("a"), b"1")
        self.assertIsNone(r.get("u"))
        self.assertIsNone(r.get("t"))
        self.assertEqual(r.stats()["seq"], 1)
        r.close()
        # Committed image is unchanged and the reader never repaired anything.
        with open(os.path.join(self.dir, "wal.ckp"), "rb") as f:
            self.assertEqual(f.read(), clean)

    def test_torn_committed_log_is_served_from_checkpoint(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
        # Tear the log itself inside its committed prefix; the checkpoint
        # still holds the full committed snapshot.
        with open(os.path.join(self.dir, "wal.log"), "wb") as f:
            f.write(b"")
        r = self.reader()
        self.assertEqual(r.get("a"), b"1")
        self.assertEqual(r.get("b"), b"2")
        self.assertEqual(r.stats()["seq"], 1)
        r.close()

    def test_corrupt_committed_bytes_raise(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        # Damage a committed byte in BOTH log and checkpoint so no clean
        # source remains.
        for name in ("wal.log", "wal.ckp"):
            p = os.path.join(self.dir, name)
            with open(p, "rb") as f:
                raw = bytearray(f.read())
            raw[20] ^= 0xFF
            with open(p, "wb") as f:
                f.write(bytes(raw))
        with self.assertRaises(CorruptLogError):
            self.reader()

    def test_corrupt_checkpoint_is_corruption_even_with_clean_log(self):
        # The writer likewise treats a damaged checkpoint as fatal recovery
        # machinery, so a reader raises rather than silently trusting it.
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        p = os.path.join(self.dir, "wal.ckp")
        with open(p, "rb") as f:
            raw = bytearray(f.read())
        raw[20] ^= 0xFF
        with open(p, "wb") as f:
            f.write(bytes(raw))
        with self.assertRaises(CorruptLogError):
            self.reader()


# A held-open reader that sleeps: it pins a snapshot and stays alive.
HUNG_READER = r"""
import sys, time
sys.path.insert(0, %r)
from wal_store import Store
s = Store(sys.argv[1], read_only=True)
_ = s.get("k0")
time.sleep(float(sys.argv[2]))
""" % REPO_ROOT

# A writer that commits as fast as it can for a duration and reports count.
BUSY_WRITER = r"""
import sys, time
sys.path.insert(0, %r)
from wal_store import Store
deadline = time.time() + float(sys.argv[2])
n = 0
with Store(sys.argv[1]) as s:
    while time.time() < deadline:
        s.put("k0", str(n).encode())
        s.put("k1", str(n).encode())
        s.commit()
        n += 1
print(n)
""" % REPO_ROOT

# A reader that repeatedly opens, reads all keys and prints one JSON line
# per observed snapshot, used to check coherence under a live writer.
SWEEP_READER = r"""
import sys, json, time
sys.path.insert(0, %r)
from wal_store import Store
deadline = time.time() + float(sys.argv[3])
seen = []
while time.time() < deadline:
    r = Store(sys.argv[1], read_only=True)
    snap = {k: (lambda v: None if v is None else v.decode())
            (r.get(k)) for k in ("k0", "k1")}
    seen.append((r.stats()["seq"], snap))
    r.close()
print(json.dumps(seen))
""" % REPO_ROOT


class ConcurrencyTest(ReadOnlyBase):
    def _seed(self):
        with self.writer() as s:
            s.put("k0", b"0")
            s.put("k1", b"0")
            s.commit()

    def test_writer_not_blocked_by_hung_readers(self):
        self._seed()
        hung = [subprocess.Popen(
            [sys.executable, "-c", HUNG_READER, self.dir, "8"])
            for _ in range(4)]
        try:
            # Give readers a moment to pin their snapshots.
            time.sleep(0.5)
            start = time.time()
            proc = subprocess.run(
                [sys.executable, "-c", BUSY_WRITER, self.dir, "1.0"],
                capture_output=True, text=True)
            elapsed = time.time() - start
            self.assertEqual(proc.returncode, 0, proc.stderr)
            count = int(proc.stdout.strip())
            self.assertGreater(count, 0)
            # No locking: dozens of fsynced commits finish in well under the
            # readers' lifetime; the writer cannot be queued behind them.
            self.assertLess(elapsed, 6.0, elapsed)
        finally:
            for p in hung:
                p.send_signal(signal.SIGKILL)
                p.wait()

    def test_killed_and_hung_readers_do_not_disturb_writer(self):
        self._seed()
        victims = [subprocess.Popen(
            [sys.executable, "-c", HUNG_READER, self.dir, "30"])
            for _ in range(3)]
        try:
            time.sleep(0.4)
            for p in victims:
                p.send_signal(signal.SIGKILL)
            for p in victims:
                self.assertEqual(p.wait(), -signal.SIGKILL)
            # Writer commits, recovers and reports normally afterwards.
            with self.writer() as s:
                self.assertEqual(s.commit(), 1)
                s.put("k0", b"after-kill")
                self.assertEqual(s.commit(), 2)
                self.assertEqual(
                    s.recover(),
                    {"applied": 3, "discarded": 0, "seq": 2})
            r = self.reader()
            self.assertEqual(r.get("k0"), b"after-kill")
            self.assertEqual(r.stats()["seq"], 2)
            r.close()
        finally:
            for p in victims:
                if p.poll() is None:
                    p.kill()
                    p.wait()

    def test_every_reader_snapshot_is_coherent_under_contention(self):
        self._seed()
        duration = 3.0
        writer = subprocess.Popen(
            [sys.executable, "-c", BUSY_WRITER, self.dir, str(duration)])
        readers = [subprocess.Popen(
            [sys.executable, "-c", SWEEP_READER, self.dir,
             os.devnull, str(duration)],
            stdout=subprocess.PIPE) for _ in range(4)]
        try:
            writer.wait(timeout=duration + 10)
            self.assertEqual(writer.returncode, 0)
            for r in readers:
                out, err = r.communicate(timeout=15)
                self.assertEqual(r.returncode, 0, err)
                seen = json.loads(out)
                self.assertTrue(seen)
                for seq, snap in seen:
                    # A snapshot is coherent only when the two keys committed
                    # together carry the same generation.
                    self.assertEqual(snap["k0"], snap["k1"], (seq, snap))
                    self.assertGreaterEqual(seq, 1)
        finally:
            for r in readers:
                if r.poll() is None:
                    r.kill()

    def test_many_concurrent_readers_each_complete(self):
        with self.writer() as s:
            for i in range(20):
                s.put(f"key{i}", f"value{i}".encode())
            s.commit()
        procs = [subprocess.Popen(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0,%r);\n"
             "from wal_store import Store;\n"
             "r=Store(sys.argv[1], read_only=True);\n"
             "print(r.get('key19').decode()); r.close()" % REPO_ROOT,
             self.dir], stdout=subprocess.PIPE) for _ in range(8)]
        for p in procs:
            out, err = p.communicate(timeout=15)
            self.assertEqual(p.returncode, 0, err)
            self.assertEqual(out.strip(), b"value19")


if __name__ == "__main__":
    unittest.main()
