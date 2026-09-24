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


class CliTest(CliBase):
    def test_put_get_recover(self):
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(b"hello")
        code, _, err = self.run_cli("--path", self.dir, "put",
                                    "k", "--value-file", vf)
        self.assertEqual(code, 0, err)

        code, out, err = self.run_cli_get("--path", self.dir, "get", "k")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, b"hello")

        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out,
                         '{"applied":1,"discarded":0,"seq":1}\n')

    def test_get_writes_raw_bytes_to_binary_stdout(self):
        payload = bytes(range(256))
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(payload)
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
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(b"v")
        code, _, err = self.run_cli("--path", self.dir, "put", "",
                                    "--value-file", vf)
        self.assertEqual(code, 3)
        self.assertIn("error", err)

    def test_recover_on_fresh_store(self):
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":0,"discarded":0,"seq":0}\n')

    def test_recover_missing_directory_exit_3(self):
        missing = os.path.join(self.dir, "never", "created")
        code, out, err = self.run_cli("--path", missing, "recover")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertTrue(err.strip())
        self.assertFalse(os.path.exists(missing))

    def test_recover_corrupt_exit_3_no_json(self):
        from wal_store.store import Store
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(self.dir, "wal.log"), "r+b") as f:
            f.seek(-1, os.SEEK_END)
            f.write(b"\x00")
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertTrue(err.strip())

    def test_recover_torn_tail_exit_0_reports_discarded(self):
        from wal_store.store import Store, _encode_frame, _OP_PUT
        with Store(self.dir) as s:
            s.put("a", b"1")
            s.commit()
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(_encode_frame(_OP_PUT, b"tail", key="b")[:7])
        code, out, err = self.run_cli("--path", self.dir, "recover")
        self.assertEqual(code, 0, err)
        self.assertEqual(out, '{"applied":1,"discarded":1,"seq":1}\n')

    def test_put_creates_missing_directory(self):
        new_dir = os.path.join(self.dir, "created", "by", "put")
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(b"v")
        code, _, err = self.run_cli("--path", new_dir, "put", "k",
                                    "--value-file", vf)
        self.assertEqual(code, 0, err)
        self.assertTrue(os.path.isfile(
            os.path.join(new_dir, "wal.log")))


class CliSubprocessTest(CliBase):
    """Exercise the real ``python -m wal_store`` entry point end to end."""

    def module(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "wal_store", "--path", self.dir, *args],
            capture_output=True)

    def test_module_get_hit_and_miss(self):
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(b"line1\nline2\nno trailing newline")
        r = self.module("put", "k", "--value-file", vf)
        self.assertEqual(r.returncode, 0, r.stderr)

        r = self.module("get", "k")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, b"line1\nline2\nno trailing newline")

        r = self.module("get", "missing")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(r.stdout, b"")

    def test_module_recover_format(self):
        vf = os.path.join(self.dir, "v.bin")
        with open(vf, "wb") as f:
            f.write(b"v")
        self.module("put", "a", "--value-file", vf)
        self.module("put", "b", "--value-file", vf)
        r = self.module("recover")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(
            r.stdout, b'{"applied":2,"discarded":0,"seq":2}\n')

    def test_module_usage_error_on_stderr(self):
        r = subprocess.run(
            [sys.executable, "-m", "wal_store", "--bogus"],
            capture_output=True)
        self.assertEqual(r.returncode, 2)
        self.assertNotEqual(r.stderr, b"")


if __name__ == "__main__":
    unittest.main()
