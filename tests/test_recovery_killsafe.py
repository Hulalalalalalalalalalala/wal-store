"""Kill-safe recovery and deterministic tear injection.

These tests cover:

* ``wal_store.inject_tear`` byte-exact copies and their bounds;
* recovery of a torn log at *every* byte offset of a log containing empty
  values, delete-then-rewrite and repeated writes to one key;
* report/state stability across repeated and interrupted recoveries;
* a recovery interrupted mid-convergence (sidecars half-written) converging
  silently to the same result;
* the durable sequence never regressing after repeated kills;
* genuine corruption still raising ``CorruptLogError``;
* the Windows directory-fsync ``PermissionError`` workaround.
"""

import json
import os
import signal
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import zlib

from wal_store import CorruptLogError, Store, inject_tear
from wal_store.store import (
    _encode_frame,
    _OP_COMMIT,
    _OP_DELETE,
    _OP_PUT,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TearBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.src = os.path.join(self.dir, "src")
        os.makedirs(self.src)

    def tearDown(self):
        self._tmp.cleanup()

    def build(self):
        """Edge keys: empty value, delete then rewrite, repeated writes."""
        with Store(self.src) as s:
            s.put("once", b"one")
            s.put("repeat", b"first")
            s.put("repeat", b"second")
            s.put("empty", b"")
            s.commit()                            # seq 1
            s.delete("once")
            s.put("once", b"rewritten")
            s.commit()                            # seq 2
            s.put("repeat", b"uncommitted-third")
            s.delete("empty")
        with open(os.path.join(self.src, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"zebra-torn", key="z")[:11])
        with open(os.path.join(self.src, "wal.log"), "rb") as f:
            return f.read()

    def torn_copy(self, off, name="work"):
        dst_dir = os.path.join(self.dir, name)
        os.makedirs(dst_dir, exist_ok=True)
        for entry in os.listdir(dst_dir):
            os.unlink(os.path.join(dst_dir, entry))
        # Sidecars travel with the copy.
        import shutil
        for entry in os.listdir(self.src):
            if entry != "wal.log":
                shutil.copy(os.path.join(self.src, entry),
                            os.path.join(dst_dir, entry))
        return inject_tear(self.src,
                           os.path.join(dst_dir, "wal.log"), off), dst_dir

    EXPECTED_STATE = {"repeat": b"second", "empty": b"",
                      "once": b"rewritten"}
    EXPECTED_REPORT_BASE = {"applied": 6, "seq": 2}


def scan_frames(data):
    """Minimal independent WAL2 scan of a prefix image.

    Returns ``(last_commit_end, torn_start_or_None)``.
    """
    pos = 0
    last_commit_end = 0
    while pos < len(data):
        if len(data) - pos < 16:
            return last_commit_end, pos
        magic, length, hcrc = struct.unpack_from(">4sQI", data, pos)
        if magic != b"WAL2" or (hcrc & 0xFFFFFFFF) != zlib.crc32(
                data[pos:pos + 12]):
            return last_commit_end, pos
        end = pos + 16 + length + 4
        if end > len(data):
            return last_commit_end, pos
        payload = data[pos + 16:pos + 16 + length]
        tcrc, = struct.unpack_from(">I", data, pos + 16 + length)
        if (tcrc & 0xFFFFFFFF) != zlib.crc32(payload):
            return last_commit_end, pos
        nl = payload.index(b"\n")
        meta = json.loads(payload[:nl])
        pos = end
        if meta["t"] == _OP_COMMIT:
            last_commit_end = end
    return last_commit_end, None


class InjectTearTest(TearBase):
    def test_copy_is_byte_exact_prefix(self):
        self.build()
        src_log = os.path.join(self.src, "wal.log")
        for off in (0, 1, 7, 16, 100, 300):
            dst = os.path.join(self.dir, f"t{off}.log")
            inject_tear(src_log, dst, off)
            with open(src_log, "rb") as f:
                original = f.read()
            with open(dst, "rb") as f:
                self.assertEqual(f.read(), original[:off])
            self.assertEqual(os.path.getsize(dst), off)

    def test_full_length_copy_is_clean_control(self):
        full = self.build()
        dst = os.path.join(self.dir, "full.log")
        inject_tear(self.src, dst, len(full))
        self.assertEqual(open(dst, "rb").read(), full)

    def test_negative_offset_raises_index_error(self):
        full = self.build()
        dst = os.path.join(self.dir, "n.log")
        for bad in (-1, -2, -10 ** 9):
            with self.assertRaises(IndexError):
                inject_tear(self.src, dst, bad)

    def test_past_end_offset_raises_index_error(self):
        full = self.build()
        dst = os.path.join(self.dir, "o.log")
        with self.assertRaises(IndexError):
            inject_tear(self.src, dst, len(full) + 1)
        with self.assertRaises(IndexError):
            inject_tear(self.src, dst, len(full) + 1000)

    def test_non_integer_offset_raises_type_error(self):
        self.build()
        dst = os.path.join(self.dir, "t.log")
        for bad in (1.0, "1", None, b"1"):
            with self.assertRaises(TypeError):
                inject_tear(self.src, dst, bad)

    def test_source_is_never_modified(self):
        full = self.build()
        dst = os.path.join(self.dir, "s.log")
        inject_tear(self.src, dst, 3)
        inject_tear(self.src, dst, 0)
        inject_tear(self.src, dst, len(full))
        with open(os.path.join(self.src, "wal.log"), "rb") as f:
            self.assertEqual(f.read(), full)

    def test_directory_paths_resolve_to_wal_log(self):
        full = self.build()
        dst_dir = os.path.join(self.dir, "dd")
        os.makedirs(dst_dir)
        out = inject_tear(self.src, dst_dir, 21)
        self.assertEqual(out, os.path.join(dst_dir, "wal.log"))
        self.assertEqual(os.path.getsize(out), 21)

    def test_same_source_and_destination_rejected(self):
        self.build()
        with self.assertRaises(ValueError):
            inject_tear(self.src, self.src, 5)


class EveryOffsetRecoveryTest(TearBase):
    def test_every_offset_converges_to_committed_state(self):
        full = self.build()
        boundary, _ = scan_frames(full)
        size = len(full)
        for off in range(size + 1):
            _path, work = self.torn_copy(off, f"o{off}")
            with Store(work) as s:
                state = {k: s.get(k) for k in self.EXPECTED_STATE}
                first = s.recover()
                second = s.recover()
            self.assertEqual(state, self.EXPECTED_STATE, off)
            # Uncommitted keys never leak.
            with Store(work) as s2:
                self.assertIsNone(s2.get("z"), off)
                self.assertIsNone(s2.get("repeat-uncommitted"), off)
            # discarded is 1 only if the torn fragment survives past the
            # durable commit boundary.
            _b, torn = scan_frames(full[:off])
            expected_discarded = 1 if torn is not None and torn >= boundary \
                else 0
            expected_report = dict(self.EXPECTED_REPORT_BASE,
                                   discarded=expected_discarded)
            self.assertEqual(first, expected_report, off)
            self.assertEqual(second, first, off)

    def test_every_offset_log_is_rebuilt_to_commit_boundary(self):
        full = self.build()
        boundary, _ = scan_frames(full)
        for off in (0, 1, boundary - 1, boundary, boundary + 1,
                    len(full) - 1, len(full)):
            _path, work = self.torn_copy(off, f"b{off}")
            with Store(work) as s:
                s.recover()
            self.assertEqual(
                os.path.getsize(os.path.join(work, "wal.log")), boundary,
                off)
            with open(os.path.join(work, "wal.log"), "rb") as f:
                self.assertEqual(f.read(), full[:boundary], off)

    def test_seq_never_regresses_after_offset_tears(self):
        full = self.build()
        for off in range(0, len(full) + 1, 7):
            _path, work = self.torn_copy(off, f"s{off}")
            for _ in range(3):
                with Store(work) as s:
                    self.assertEqual(s.stats()["seq"], 2, off)
            with Store(work) as s:
                s.put("new", b"v")
                self.assertEqual(s.commit(), 3, off)
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], 3, off)

    def test_zero_byte_and_empty_logs(self):
        # Torn at offset 0 of a committed log.
        full = self.build()
        _path, work = self.torn_copy(0, "zero")
        self.assertEqual(os.path.getsize(
            os.path.join(work, "wal.log")), 0)
        with Store(work) as s:
            self.assertEqual(s.recover(),
                             {"applied": 6, "discarded": 0, "seq": 2})
        # Brand new empty directory.
        empty = os.path.join(self.dir, "empty")
        os.makedirs(empty)
        with Store(empty) as s:
            self.assertEqual(s.recover(),
                             {"applied": 0, "discarded": 0, "seq": 0})

    def test_cli_recover_every_offset_is_stable_exit_0(self):
        full = self.build()
        boundary, _ = scan_frames(full)
        for off in range(0, len(full) + 1, 13):
            _path, work = self.torn_copy(off, f"c{off}")
            _b, torn = scan_frames(full[:off])
            expected_discarded = 1 if torn is not None and torn >= boundary \
                else 0
            line = ('{"applied":6,"discarded":%d,"seq":2}\n'
                    % expected_discarded).encode()
            for _ in range(2):
                proc = subprocess.run(
                    [sys.executable, "-m", "wal_store",
                     "--path", work, "recover"], capture_output=True)
                self.assertEqual(proc.returncode, 0, (off, proc.stderr))
                self.assertEqual(proc.stdout, line, off)


# Crash points: raise SystemExit on the nth primitive used while converging.
CRASH_RECOVER = r"""
import sys, os
sys.path.insert(0, %r)
n = int(sys.argv[2]); count = [0]
real_replace, real_fsync, real_truncate = os.replace, os.fsync, os.ftruncate
def tick():
    count[0] += 1
    if count[0] == n:
        os._exit(9)
def wrapped(real):
    def inner(*a, **k):
        tick()
        return real(*a, **k)
    return inner
os.replace, os.fsync, os.ftruncate = (wrapped(real_replace),
                                      wrapped(real_fsync),
                                      wrapped(real_truncate))
from wal_store import Store
with Store(sys.argv[1]) as store:
    store.recover()
""" % REPO_ROOT


class InterruptedRecoveryTest(TearBase):
    def _crashed_recover(self, work, point):
        return subprocess.run(
            [sys.executable, "-c", CRASH_RECOVER, work, str(point)],
            capture_output=True)

    def test_crash_at_every_convergence_step_converges(self):
        full = self.build()
        boundary, _ = scan_frames(full)
        for off in (0, 8, boundary, boundary + 5, len(full) - 1):
            for point in range(1, 30):
                _path, work = self.torn_copy(off, f"k{off}-{point}")
                proc = self._crashed_recover(work, point)
                # If the process exited 0 the crash point was never reached.
                self.assertIn(proc.returncode, (0, -9, 9),
                              (off, point, proc.stderr))
                with Store(work) as s:
                    state = {k: s.get(k) for k in self.EXPECTED_STATE}
                    report = s.recover()
                    again = s.recover()
                self.assertEqual(state, self.EXPECTED_STATE, (off, point))
                self.assertEqual(report, again, (off, point))
                self.assertEqual(report["seq"], 2, (off, point))
                with Store(work) as s2:
                    s2.put("new", b"v")
                    self.assertEqual(s2.commit(), 3, (off, point))

    def test_chained_interruptions(self):
        full = self.build()
        import itertools
        import shutil
        chains = list(itertools.product(range(1, 9), repeat=2))
        chains += [(2, 5, 3), (7, 1, 8), (4, 4, 4)]
        for off in (7, len(full) - 1):
            for chain in chains:
                _path, work = self.torn_copy(off, "chain")
                for point in chain:
                    self._crashed_recover(work, point)
                with Store(work) as s:
                    state = {k: s.get(k) for k in self.EXPECTED_STATE}
                    report = s.recover()
                self.assertEqual(state, self.EXPECTED_STATE,
                                 (off, chain))
                self.assertEqual(report["seq"], 2, (off, chain))
                with Store(work) as s2:
                    s2.put("new", b"v")
                    self.assertEqual(s2.commit(), 3, (off, chain))
                # reset for next chain
                shutil.rmtree(work, ignore_errors=True)

    def test_timing_based_signals_always_converge(self):
        full = self.build()
        rng_seed = 987654
        import random
        rng = random.Random(rng_seed)
        for trial in range(40):
            off = rng.randrange(0, len(full) + 1)
            _path, work = self.torn_copy(off, f"sig{trial}")
            for _ in range(rng.randrange(0, 5)):
                proc = subprocess.Popen(
                    [sys.executable, "-m", "wal_store",
                     "--path", work, "recover"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                deadline = time.time() + rng.uniform(0.0, 0.04)
                while time.time() < deadline and proc.poll() is None:
                    time.sleep(0.0005)
                if proc.poll() is None:
                    proc.send_signal(signal.SIGKILL)
                    proc.wait()
            proc = subprocess.run(
                [sys.executable, "-m", "wal_store",
                 "--path", work, "recover"], capture_output=True)
            self.assertEqual(proc.returncode, 0, (trial, off, proc.stderr))
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["seq"], 2, (trial, off))
            with Store(work) as s:
                state = {k: s.get(k) for k in self.EXPECTED_STATE}
            self.assertEqual(state, self.EXPECTED_STATE, (trial, off))

    def test_half_repair_sidecars_are_not_a_torn_tail(self):
        # A wal.log.tmp left by a kill during the atomic rebuild must be
        # removed silently and the repair completed on reopen.
        full = self.build()
        boundary, _ = scan_frames(full)
        _path, work = self.torn_copy(12, "tmp")
        with open(os.path.join(work, "wal.log.tmp"), "wb") as f:
            f.write(b"partial garbage that must be ignored")
        with open(os.path.join(work, "wal.ckp.tmp"), "wb") as f:
            f.write(b"x")
        with Store(work) as s:
            report = s.recover()
        self.assertEqual(report["seq"], 2)
        self.assertFalse(
            os.path.exists(os.path.join(work, "wal.log.tmp")))
        self.assertFalse(
            os.path.exists(os.path.join(work, "wal.ckp.tmp")))
        self.assertEqual(
            os.path.getsize(os.path.join(work, "wal.log")), boundary)

    def test_new_write_after_repaired_epoch_counts_fresh_tear(self):
        # After a repair (marker kept until the next commit), a *new* torn
        # write is a new discard decision, not the old marker's.
        d = os.path.join(self.dir, "epoch")
        os.makedirs(d)
        with Store(d) as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(d, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"first-tail", key="b")[:7])
        with Store(d) as s:
            self.assertEqual(s.recover()["discarded"], 1)
        with open(os.path.join(d, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"second", key="c"))
            f.write(_encode_frame(_OP_PUT, b"second-tail", key="d")[:9])
        with Store(d) as s:
            report = s.recover()
            self.assertEqual(report["discarded"], 1)
            self.assertIsNone(s.get("b"))
            self.assertIsNone(s.get("c"))
            self.assertIsNone(s.get("d"))
        # The next commit closes the epoch and resets the count.
        with Store(d) as s:
            s.put("e", b"5")
            self.assertEqual(s.commit(), 2)
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 2})
        self.assertFalse(
            os.path.exists(os.path.join(d, "wal.rec")))


class CorruptionStillFatalTest(TearBase):
    def test_committed_frame_damage_raises_and_touches_nothing(self):
        full = self.build()
        boundary, _ = scan_frames(full)
        prefix = bytearray(full[:boundary])
        prefix[20] ^= 0xFF
        _path, work = self.torn_copy(0, "corrupt")  # create dir
        with open(os.path.join(work, "wal.log"), "wb") as f:
            f.write(bytes(prefix))
        with Store(work) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()
        # Left exactly as found.
        with open(os.path.join(work, "wal.log"), "rb") as f:
            self.assertEqual(f.read(), bytes(prefix))

    def test_length_out_of_bounds_raises(self):
        d = os.path.join(self.dir, "oob")
        os.makedirs(d)
        with Store(d) as s:
            s.put("a", b"1")
            s.commit()
        import zlib
        p = b"WAL2" + (1 << 41).to_bytes(8, "big")
        header = p + zlib.crc32(p).to_bytes(4, "big")
        with open(os.path.join(d, "wal.log"), "ab") as f:
            f.write(header)
        with Store(d) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()

    def test_sequence_regression_raises(self):
        d = os.path.join(self.dir, "regress")
        os.makedirs(d)
        frames = (_encode_frame(_OP_PUT, b"1", key="a")
                  + _encode_frame(_OP_COMMIT, seq=1)
                  + _encode_frame(_OP_PUT, b"2", key="b")
                  + _encode_frame(_OP_COMMIT, seq=1))
        with open(os.path.join(d, "wal.log"), "wb") as f:
            f.write(frames)
        with Store(d) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


class FsyncDirPlatformTest(unittest.TestCase):
    def test_windows_skips_directory_fsync(self):
        from unittest import mock
        from wal_store.store import _fsync_dir
        with tempfile.TemporaryDirectory() as d:
            with mock.patch("wal_store.store.sys.platform", "win32"):
                # Must return without touching os.open/fsync on Windows.
                with mock.patch("wal_store.store.os.open",
                                side_effect=AssertionError("os.open called")):
                    _fsync_dir(d)

    def test_permission_error_is_absorbed(self):
        from unittest import mock
        from wal_store.store import _fsync_dir
        with tempfile.TemporaryDirectory() as d:
            with mock.patch("wal_store.store.sys.platform", "linux"):
                with mock.patch("wal_store.store.os.fsync",
                                side_effect=PermissionError("denied")):
                    _fsync_dir(d)  # must not raise

    def test_store_creation_survives_directory_fsync_failure(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as d:
            fresh = os.path.join(d, "store")
            os.makedirs(fresh)
            # On Windows the directory sync is skipped, so file fsync calls
            # keep working and store creation never sees PermissionError.
            with mock.patch("wal_store.store.sys.platform", "win32"):
                with Store(fresh) as s:
                    self.assertEqual(
                        s.recover(),
                        {"applied": 0, "discarded": 0, "seq": 0})
            # Same outcome on another platform whose mount denies it: the
            # PermissionError raised by the directory fsync is absorbed.
            fresh2 = os.path.join(d, "store2")
            os.makedirs(fresh2)
            real_fsync = os.fsync

            def fsync_filter(fd):
                # Only the directory (opened O_RDONLY by _fsync_dir) fails;
                # distinguish it from the writable log fd.
                import stat as statmod
                try:
                    mode = os.fstat(fd).st_mode
                except OSError:
                    return real_fsync(fd)
                if statmod.S_ISDIR(mode):
                    raise PermissionError("denied")
                return real_fsync(fd)

            with mock.patch("wal_store.store.sys.platform", "linux"):
                with mock.patch("wal_store.store.os.fsync",
                                side_effect=fsync_filter):
                    with Store(fresh2) as s:
                        self.assertEqual(
                            s.recover(),
                            {"applied": 0, "discarded": 0, "seq": 0})

    def test_any_directory_sync_refusal_is_absorbed(self):
        # The spec fixes this for *any* refusal style: a non-Permission
        # OSError on the directory open or its fsync must not abort store
        # creation, commits, checkpoints or compaction, nor later writes.
        from unittest import mock
        import stat as statmod
        real_fsync = os.fsync
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "store")
            os.makedirs(store)

            def refuse_fsync(fd):
                try:
                    if statmod.S_ISDIR(os.fstat(fd).st_mode):
                        raise OSError(22, "Invalid argument")
                except OSError:
                    raise
                return real_fsync(fd)

            with mock.patch("wal_store.store.os.fsync",
                            side_effect=refuse_fsync):
                with Store(store) as s:
                    self.assertEqual(s.commit(), 1)       # empty commit
                    s.put("a", b"1")
                    self.assertEqual(s.commit(), 2)
                    self.assertEqual(s.compact()["seq"], 2)
                    self.assertEqual(s.commit(), 3)

            # And a refusal to even open the directory for fsync.
            store2 = os.path.join(d, "store2")
            os.makedirs(store2)
            real_open = os.open

            def refuse_open(path, flags, *a, **k):
                if path == store2 and not (flags & os.O_WRONLY):
                    raise OSError(13, "Permission denied")
                return real_open(path, flags, *a, **k)

            with mock.patch("wal_store.store.os.open",
                            side_effect=refuse_open):
                with Store(store2) as s:
                    s.put("a", b"1")
                    self.assertEqual(s.commit(), 1)
            with Store(store2, read_only=True) as r:
                self.assertEqual(r.get("a"), b"1")


if __name__ == "__main__":
    unittest.main()
