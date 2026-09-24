"""Tests for kill-safe recovery and the deterministic tear-injection aid.

Coverage:

* ``wal_store.inject_tear`` produces byte-exact prefix copies and bounds
  them (negative / past-end offsets raise ``IndexError``);
* recovering a log torn at *every* byte offset yields, key by key, exactly
  the last committed state -- empty values, delete-then-rewrite and
  repeated writes included -- and never an uncommitted mutation;
* the report (applied / discarded / seq) and the durable sequence stay
  unchanged across repeated and arbitrarily interrupted recoveries;
* a half-finished convergence left by a kill (a sidecar temp, a log shrunk
  mid-truncate) is silently repaired to the same clean result, exit code 0;
* genuine corruption (a damaged committed record, an out-of-bounds length,
  a regressing sequence) still raises ``CorruptLogError``;
* store creation no longer fails on the Windows directory-fsync
  ``PermissionError``.
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
    _OP_PUT,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def scan_prefix(data: bytes):
    """Independent WAL2 scan of a log image.

    Returns ``(commit_boundary, torn_start)`` where ``commit_boundary`` is
    the offset just past the last intact commit marker and ``torn_start`` is
    where the final incomplete frame begins (``None`` when the image ends
    cleanly at a frame boundary).
    """
    pos = 0
    boundary = 0
    n = len(data)
    while pos < n:
        if n - pos < 16:
            return boundary, pos
        magic, length, hcrc = struct.unpack_from(">4sQI", data, pos)
        if magic != b"WAL2" or (hcrc & 0xFFFFFFFF) != zlib.crc32(
                data[pos:pos + 12]):
            return boundary, pos
        end = pos + 16 + length + 4
        if end > n:
            return boundary, pos
        payload = data[pos + 16:pos + 16 + length]
        tcrc, = struct.unpack_from(">I", data, pos + 16 + length)
        if (tcrc & 0xFFFFFFFF) != zlib.crc32(payload):
            return boundary, pos
        meta = json.loads(payload[:payload.index(b"\n")])
        pos = end
        if meta["t"] == _OP_COMMIT:
            boundary = end
    return boundary, None


class TearCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.src = os.path.join(self.dir, "source")
        os.makedirs(self.src)

    def tearDown(self):
        self._tmp.cleanup()

    def build_source(self) -> bytes:
        """Committed history exercising the edge keys, then an uncommitted
        torn tail: an empty value, a key deleted and rewritten, and a key
        written repeatedly."""
        with Store(self.src) as s:
            s.put("repeat", b"first")
            s.put("repeat", b"second")
            s.put("empty", b"")
            s.put("once", b"one")
            s.commit()                              # seq 1
            s.delete("once")
            s.put("once", b"rewritten")
            s.commit()                              # seq 2
            s.put("repeat", b"third-uncommitted")
            s.delete("empty")
        with open(os.path.join(self.src, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"torn", key="z")[:11])
        with open(os.path.join(self.src, "wal.log"), "rb") as f:
            return f.read()

    def torn_copy(self, offset: int, name: str) -> str:
        import shutil
        dst = os.path.join(self.dir, name)
        os.makedirs(dst, exist_ok=True)
        for entry in os.listdir(dst):
            os.unlink(os.path.join(dst, entry))
        # Sidecars are part of the durable image and travel with the copy.
        for entry in os.listdir(self.src):
            if entry != "wal.log":
                shutil.copy(os.path.join(self.src, entry),
                            os.path.join(dst, entry))
        inject_tear(self.src, os.path.join(dst, "wal.log"), offset)
        return dst

    # Committed truth after a clean recovery of build_source().
    STATE = {"repeat": b"second", "empty": b"", "once": b"rewritten"}
    APPLIED = 6
    SEQ = 2


class InjectTearTest(TearCase):
    def test_destination_is_byte_exact_prefix(self):
        full = self.build_source()
        for off in (0, 1, 5, 12, 16, 17, 64, len(full)):
            dst = os.path.join(self.dir, f"p{off}.log")
            inject_tear(os.path.join(self.src, "wal.log"), dst, off)
            with open(dst, "rb") as f:
                self.assertEqual(f.read(), full[:off], off)
            self.assertEqual(os.path.getsize(dst), off, off)

    def test_full_length_is_clean_control(self):
        full = self.build_source()
        dst = os.path.join(self.dir, "full.log")
        inject_tear(self.src, dst, len(full))
        with open(dst, "rb") as f:
            self.assertEqual(f.read(), full)

    def test_negative_offset_raises_index_error(self):
        self.build_source()
        dst = os.path.join(self.dir, "neg.log")
        for bad in (-1, -2, -10_000):
            with self.assertRaises(IndexError):
                inject_tear(self.src, dst, bad)

    def test_offset_past_end_raises_index_error(self):
        full = self.build_source()
        dst = os.path.join(self.dir, "past.log")
        with self.assertRaises(IndexError):
            inject_tear(self.src, dst, len(full) + 1)
        with self.assertRaises(IndexError):
            inject_tear(self.src, dst, len(full) + 999)

    def test_non_integer_offset_raises_type_error(self):
        self.build_source()
        dst = os.path.join(self.dir, "t.log")
        for bad in (1.0, "7", None, b"7"):
            with self.assertRaises(TypeError):
                inject_tear(self.src, dst, bad)

    def test_source_never_modified(self):
        full = self.build_source()
        dst = os.path.join(self.dir, "imm.log")
        for off in (0, 3, len(full) // 2, len(full)):
            inject_tear(self.src, dst, off)
        with open(os.path.join(self.src, "wal.log"), "rb") as f:
            self.assertEqual(f.read(), full)

    def test_directory_destination_resolves_to_wal_log(self):
        full = self.build_source()
        out_dir = os.path.join(self.dir, "outdir")
        os.makedirs(out_dir)
        returned = inject_tear(self.src, out_dir, 19)
        self.assertEqual(returned, os.path.join(out_dir, "wal.log"))
        self.assertEqual(os.path.getsize(returned), 19)

    def test_same_source_and_destination_rejected(self):
        self.build_source()
        with self.assertRaises(ValueError):
            inject_tear(self.src, self.src, 5)
        with self.assertRaises(ValueError):
            inject_tear(os.path.join(self.src, "wal.log"),
                        os.path.join(self.src, "wal.log"), 5)


class EveryOffsetRecoveryTest(TearCase):
    def test_every_offset_recovers_committed_state_and_report(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        for off in range(len(full) + 1):
            work = self.torn_copy(off, f"off{off}")
            with Store(work) as s:
                state = {k: s.get(k) for k in self.STATE}
                first = s.recover()
                second = s.recover()
            self.assertEqual(state, self.STATE, off)
            with Store(work) as again:
                self.assertIsNone(again.get("z"), off)
                self.assertIsNone(again.get("repeat-uncommitted"), off)
            _, torn = scan_prefix(full[:off])
            discarded = 1 if torn is not None and torn >= boundary else 0
            expected = {"applied": self.APPLIED, "discarded": discarded,
                        "seq": self.SEQ}
            self.assertEqual(first, expected, off)
            self.assertEqual(second, first, off)

    def test_log_is_rebuilt_to_commit_boundary(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        for off in (0, 1, boundary - 1, boundary, boundary + 1,
                    len(full) - 1, len(full)):
            work = self.torn_copy(off, f"b{off}")
            with Store(work) as s:
                s.recover()
            log = os.path.join(work, "wal.log")
            self.assertEqual(os.path.getsize(log), boundary, off)
            with open(log, "rb") as f:
                self.assertEqual(f.read(), full[:boundary], off)

    def test_sequence_never_regresses_and_next_commit_is_plus_one(self):
        full = self.build_source()
        for off in range(0, len(full) + 1, 7):
            work = self.torn_copy(off, f"seq{off}")
            for _ in range(3):
                with Store(work) as s:
                    self.assertEqual(s.stats()["seq"], self.SEQ, off)
            with Store(work) as s:
                s.put("new", b"v")
                self.assertEqual(s.commit(), self.SEQ + 1, off)
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], self.SEQ + 1, off)

    def test_zero_byte_log_and_brand_new_directory(self):
        self.build_source()
        work = self.torn_copy(0, "zero")
        self.assertEqual(os.path.getsize(os.path.join(work, "wal.log")), 0)
        with Store(work) as s:
            self.assertEqual(
                s.recover(),
                {"applied": self.APPLIED, "discarded": 0, "seq": self.SEQ})
        empty = os.path.join(self.dir, "empty")
        os.makedirs(empty)
        with Store(empty) as s:
            self.assertEqual(s.recover(),
                             {"applied": 0, "discarded": 0, "seq": 0})

    def test_cli_recover_is_stable_for_every_offset(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        for off in range(0, len(full) + 1, 13):
            work = self.torn_copy(off, f"cli{off}")
            _, torn = scan_prefix(full[:off])
            discarded = 1 if torn is not None and torn >= boundary else 0
            line = ('{"applied":%d,"discarded":%d,"seq":%d}\n'
                    % (self.APPLIED, discarded, self.SEQ)).encode()
            for _ in range(2):
                proc = subprocess.run(
                    [sys.executable, "-m", "wal_store",
                     "--path", work, "recover"], capture_output=True)
                self.assertEqual(proc.returncode, 0, (off, proc.stderr))
                self.assertEqual(proc.stdout, line, off)


# Kill the process on the nth convergence primitive.
CRASH_SCRIPT = r"""
import os, sys
sys.path.insert(0, %r)
n = int(sys.argv[2])
calls = [0]
real = {"replace": os.replace, "fsync": os.fsync,
        "truncate": os.ftruncate, "unlink": os.unlink}

def tick(name):
    fn = real[name]

    def wrapped(*a, **k):
        calls[0] += 1
        if calls[0] == n:
            os._exit(9)
        return fn(*a, **k)
    return wrapped

os.replace = tick("replace")
os.fsync = tick("fsync")
os.ftruncate = tick("truncate")
os.unlink = tick("unlink")

from wal_store import Store
with Store(sys.argv[1]) as store:
    store.recover()
""" % REPO_ROOT


class InterruptedRecoveryTest(TearCase):
    def crash_recover(self, work: str, point: int):
        return subprocess.run(
            [sys.executable, "-c", CRASH_SCRIPT, work, str(point)],
            capture_output=True)

    def test_kill_at_each_convergence_step_still_converges(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        for off in (0, 8, boundary, boundary + 5, len(full) - 1):
            for point in range(1, 32):
                work = self.torn_copy(off, f"k{off}-{point}")
                proc = self.crash_recover(work, point)
                self.assertIn(proc.returncode, (0, 9, -9),
                              (off, point, proc.stderr))
                with Store(work) as s:
                    state = {k: s.get(k) for k in self.STATE}
                    report = s.recover()
                    again = s.recover()
                self.assertEqual(state, self.STATE, (off, point))
                self.assertEqual(report, again, (off, point))
                self.assertEqual(report["seq"], self.SEQ, (off, point))
                with Store(work) as s:
                    s.put("new", b"v")
                    self.assertEqual(s.commit(), self.SEQ + 1, (off, point))

    def test_chains_of_interruptions(self):
        import itertools
        import shutil
        full = self.build_source()
        chains = list(itertools.product(range(1, 9), repeat=2))
        chains += [(2, 5, 3), (7, 1, 8), (4, 4, 4)]
        for off in (7, len(full) - 1):
            for chain in chains:
                work = self.torn_copy(off, "chain")
                for point in chain:
                    self.crash_recover(work, point)
                with Store(work) as s:
                    state = {k: s.get(k) for k in self.STATE}
                    report = s.recover()
                self.assertEqual(state, self.STATE, (off, chain))
                self.assertEqual(report["seq"], self.SEQ, (off, chain))
                with Store(work) as s:
                    s.put("new", b"v")
                    self.assertEqual(s.commit(), self.SEQ + 1, (off, chain))
                shutil.rmtree(work, ignore_errors=True)

    def test_sigkill_at_random_times_always_converges(self):
        import random
        full = self.build_source()
        rng = random.Random(424242)
        for trial in range(40):
            off = rng.randrange(0, len(full) + 1)
            work = self.torn_copy(off, f"sig{trial}")
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
            self.assertEqual(payload["seq"], self.SEQ, (trial, off))
            with Store(work) as s:
                state = {k: s.get(k) for k in self.STATE}
            self.assertEqual(state, self.STATE, (trial, off))

    def test_half_written_sidecar_temps_are_swept(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        work = self.torn_copy(12, "temps")
        with open(os.path.join(work, "wal.log.tmp"), "wb") as f:
            f.write(b"partial garbage left by a killed rebuild")
        with open(os.path.join(work, "wal.ckp.tmp"), "wb") as f:
            f.write(b"x")
        with open(os.path.join(work, "wal.rec.tmp"), "wb") as f:
            f.write(b"y")
        with Store(work) as s:
            report = s.recover()
        self.assertEqual(report["seq"], self.SEQ)
        for name in ("wal.log.tmp", "wal.ckp.tmp", "wal.rec.tmp"):
            self.assertFalse(os.path.exists(os.path.join(work, name)))
        self.assertEqual(
            os.path.getsize(os.path.join(work, "wal.log")), boundary)

    def test_shrunk_log_from_killed_truncate_is_not_a_new_tail(self):
        # Simulate the baseline failure mode directly: a log truncated to an
        # arbitrary byte inside its committed prefix, with the recovery
        # marker pinning the real boundary. The next open restores silently
        # with discarded 0 (those bytes were committed).
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        work = self.torn_copy(len(full), "shrunk")
        with Store(work) as s:
            s.recover()
        # Now truncate the clean log into the middle of a committed frame and
        # leave the marker describing the interrupted repair.
        log = os.path.join(work, "wal.log")
        with open(log, "r+b") as f:
            f.truncate(boundary // 2)
        with open(os.path.join(work, "wal.rec"), "wb") as f:
            f.write(json.dumps({"end": boundary, "discarded": 0},
                               separators=(",", ":")).encode())
        with Store(work) as s:
            report = s.recover()
        self.assertEqual(
            report, {"applied": self.APPLIED, "discarded": 0,
                     "seq": self.SEQ})
        with open(log, "rb") as f:
            self.assertEqual(f.read(), full[:boundary])

    def test_new_tear_after_repair_is_a_new_epoch_decision(self):
        d = os.path.join(self.dir, "epoch")
        os.makedirs(d)
        with Store(d) as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(d, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"tail1", key="b")[:7])
        with Store(d) as s:
            self.assertEqual(s.recover()["discarded"], 1)
        # A fully written uncommitted frame plus a fresh torn tail: a new
        # crash decision, again discarded once.
        with open(os.path.join(d, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"second", key="c"))
            f.write(_encode_frame(_OP_PUT, b"tail2", key="e")[:9])
        with Store(d) as s:
            report = s.recover()
            self.assertEqual(report["discarded"], 1)
            self.assertIsNone(s.get("b"))
            self.assertIsNone(s.get("c"))
            self.assertIsNone(s.get("e"))
        # The next commit closes the epoch and clears the count + marker.
        with Store(d) as s:
            s.put("f", b"5")
            self.assertEqual(s.commit(), 2)
            self.assertEqual(s.recover(),
                             {"applied": 2, "discarded": 0, "seq": 2})
        self.assertFalse(os.path.exists(os.path.join(d, "wal.rec")))


class CorruptionStillFatalTest(TearCase):
    def test_damage_inside_committed_prefix_raises_and_changes_nothing(self):
        full = self.build_source()
        boundary, _ = scan_prefix(full)
        damaged = bytearray(full[:boundary])
        damaged[20] ^= 0xFF
        work = self.torn_copy(0, "corrupt")
        log = os.path.join(work, "wal.log")
        with open(log, "wb") as f:
            f.write(bytes(damaged))
        # The healthy checkpoint cannot mask a checksum failure: an invalid
        # frame is corruption, not a torn-away prefix, so recovery refuses.
        self.assertTrue(os.path.exists(os.path.join(work, "wal.ckp")))
        with Store(work) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()
        with open(log, "rb") as f:
            self.assertEqual(f.read(), bytes(damaged))

    def test_length_out_of_bounds_raises(self):
        d = os.path.join(self.dir, "oob")
        os.makedirs(d)
        with Store(d) as s:
            s.put("a", b"1")
            s.commit()
        prefix = b"WAL2" + (1 << 41).to_bytes(8, "big")
        header = prefix + zlib.crc32(prefix).to_bytes(4, "big")
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

    def test_incomplete_record_in_middle_raises(self):
        d = os.path.join(self.dir, "middle")
        os.makedirs(d)
        with Store(d) as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(d, "wal.log"), "rb") as f:
            raw = f.read()
        frame = _encode_frame(_OP_PUT, b"middle", key="b")
        tail = _encode_frame(_OP_PUT, b"after", key="c")
        with open(os.path.join(d, "wal.log"), "wb") as f:
            f.write(raw + frame[:10] + tail)
        with Store(d) as s:
            with self.assertRaises(CorruptLogError):
                s.recover()


class FsyncDirectoryPlatformTest(unittest.TestCase):
    def test_windows_skips_directory_fsync_entirely(self):
        from unittest import mock
        from wal_store.store import _fsync_dir
        with tempfile.TemporaryDirectory() as d:
            with mock.patch("wal_store.store.sys.platform", "win32"):
                with mock.patch("wal_store.store.os.open",
                                side_effect=AssertionError("os.open called")):
                    _fsync_dir(d)

    def test_permission_error_from_directory_fsync_is_absorbed(self):
        from unittest import mock
        from wal_store.store import _fsync_dir
        with tempfile.TemporaryDirectory() as d:
            with mock.patch("wal_store.store.sys.platform", "linux"):
                with mock.patch("wal_store.store.os.fsync",
                                side_effect=PermissionError("denied")):
                    _fsync_dir(d)

    def test_fresh_store_creation_survives_denied_directory_sync(self):
        from unittest import mock
        import stat as statmod
        with tempfile.TemporaryDirectory() as tmp:
            win_dir = os.path.join(tmp, "win")
            os.makedirs(win_dir)
            with mock.patch("wal_store.store.sys.platform", "win32"):
                with Store(win_dir) as s:
                    self.assertEqual(
                        s.recover(),
                        {"applied": 0, "discarded": 0, "seq": 0})

            nix_dir = os.path.join(tmp, "nix")
            os.makedirs(nix_dir)
            real_fsync = os.fsync

            def fsync_filter(fd):
                try:
                    if statmod.S_ISDIR(os.fstat(fd).st_mode):
                        raise PermissionError("denied")
                except OSError:
                    pass
                return real_fsync(fd)

            with mock.patch("wal_store.store.sys.platform", "linux"):
                with mock.patch("wal_store.store.os.fsync",
                                side_effect=fsync_filter):
                    with Store(nix_dir) as s:
                        self.assertEqual(
                            s.recover(),
                            {"applied": 0, "discarded": 0, "seq": 0})


if __name__ == "__main__":
    unittest.main()
