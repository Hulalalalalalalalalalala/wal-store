"""Tests for the command line interface."""

import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from wal_store.cli import main
from wal_store.store import _encode_frame, _OP_PUT


class CliBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def run_cli_get(self, *argv):
        """Run a get-style command capturing raw binary stdout."""
        raw = io.BytesIO()
        wrapper = io.TextIOWrapper(raw, encoding="utf-8",
                                   write_through=True)
        err = io.StringIO()
        with mock.patch.object(sys, "stdout", wrapper), redirect_stderr(err):
            code = main(list(argv))
        wrapper.flush()
        wrapper.detach()
        return code, raw.getvalue(), err.getvalue()

    def write_value_file(self, payload=b"v"):
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(payload)
        return vf


class CliTest(CliBase):
    def test_put_get_recover(self):
        vf = self.write_value_file(b"hello")
        code, _, err = self.run_cli("--path", self.dir, "put",
                                    "k", "--value-file", vf)
        self.assertEqual(code, 0, err)

        code, out, err = self.run_cli_get("--path", self.dir, "get", "k")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, b"hello")

        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":1,"discarded":0,"seq":1}\n')

    def test_get_writes_raw_bytes_to_binary_stdout(self):
        payload = bytes(range(256))
        vf = self.write_value_file(payload)
        self.run_cli("--path", self.dir, "put", "k",
                     "--value-file", vf)

        code, out, err = self.run_cli_get("--path", self.dir, "get", "k")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, payload)

    def test_get_missing_exit_1_no_output(self):
        code, out, err = self.run_cli("--path", self.dir, "get", "nope")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")

    def test_usage_error_exit_2(self):
        with self.assertRaises(SystemExit) as cm:
            with redirect_stderr(io.StringIO()):
                main([])
        self.assertEqual(cm.exception.code, 2)

        with self.assertRaises(SystemExit) as cm:
            with redirect_stderr(io.StringIO()):
                main(["--path", self.dir])
        self.assertEqual(cm.exception.code, 2)

        with self.assertRaises(SystemExit) as cm:
            with redirect_stderr(io.StringIO()):
                main(["--path", self.dir, "put", "k"])  # no --value-file
        self.assertEqual(cm.exception.code, 2)

    def test_storage_error_exit_3(self):
        file_path = os.path.join(self.dir, "not-a-dir")
        with open(file_path, "wb") as f:
            f.write(b"x")
        code, out, err = self.run_cli("--path", file_path, "recover")
        self.assertEqual(code, 3)
        self.assertIn("error", err)
        self.assertEqual(out, "")

        # Empty key -> ValueError from the storage layer.
        vf = self.write_value_file()
        code, _, err = self.run_cli("--path", self.dir, "put", "",
                                    "--value-file", vf)
        self.assertEqual(code, 3)
        self.assertIn("error", err)

    def test_recover_on_fresh_store(self):
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":0,"discarded":0,"seq":0}\n')

    def test_recover_missing_directory_exit_3(self):
        missing = os.path.join(self.dir, "no-such-store")
        code, out, err = self.run_cli("--path", missing, "recover")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertIn("error", err)
        self.assertFalse(os.path.exists(missing))

    def test_recover_reports_discarded_tail(self):
        vf = self.write_value_file()
        code, _, err = self.run_cli("--path", self.dir, "put",
                                    "a", "--value-file", vf)
        self.assertEqual(code, 0, err)
        # A complete uncommitted record plus a torn partial one.
        frame = _encode_frame(_OP_PUT, b"x", key="b")
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(frame + frame[:7])
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":1,"discarded":2,"seq":1}\n')
        # Recovering the now-clean store reports nothing discarded.
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":1,"discarded":0,"seq":1}\n')

    def test_recover_corrupt_exit_3_no_json(self):
        vf = self.write_value_file()
        code, _, err = self.run_cli("--path", self.dir, "put",
                                    "a", "--value-file", vf)
        self.assertEqual(code, 0, err)
        log = os.path.join(self.dir, "wal.log")
        with open(log, "r+b") as f:
            damaged = bytearray(f.read())
            damaged[-1] ^= 0xFF  # break the CRC of the commit frame
            f.seek(0)
            f.write(damaged)
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")  # no JSON line on stdout
        self.assertEqual(len(err.strip().splitlines()), 1)


class CliSubprocessTest(CliBase):
    """Exercise the real ``python -m wal_store`` entry point end to end."""

    def module(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", self.dir, *args],
            capture_output=True)

    def test_module_get_hit_and_miss(self):
        vf = self.write_value_file(b"line1\nline2\nno trailing newline")
        r = self.module("put", "k", "--value-file", vf)
        self.assertEqual(r.returncode, 0, r.stderr)

        r = self.module("get", "k")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, b"line1\nline2\nno trailing newline")

        r = self.module("get", "missing")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(r.stdout, b"")

    def test_module_recover_format(self):
        vf = self.write_value_file()
        self.module("put", "a", "--value-file", vf)
        self.module("put", "b", "--value-file", vf)
        r = self.module("recover")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout, b'{"applied":2,"discarded":0,"seq":2}\n')

    def test_module_recover_corrupt_exit_3(self):
        vf = self.write_value_file()
        r = self.module("put", "a", "--value-file", vf)
        self.assertEqual(r.returncode, 0, r.stderr)
        log = os.path.join(self.dir, "wal.log")
        with open(log, "r+b") as f:
            damaged = bytearray(f.read())
            damaged[-1] ^= 0xFF
            f.seek(0)
            f.write(damaged)
        r = self.module("recover")
        self.assertEqual(r.returncode, 3)
        self.assertEqual(r.stdout, b"")
        self.assertNotEqual(r.stderr, b"")

    def test_module_recover_missing_directory(self):
        missing = os.path.join(self.dir, "gone")
        r = subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", missing, "recover"],
            capture_output=True)
        self.assertEqual(r.returncode, 3)
        self.assertEqual(r.stdout, b"")

    def test_module_usage_error_on_stderr(self):
        r = subprocess.run(
            [sys.executable, "-m", "wal_store", "--bogus"],
            capture_output=True)
        self.assertEqual(r.returncode, 2)
        self.assertNotEqual(r.stderr, b"")


if __name__ == "__main__":
    unittest.main()
