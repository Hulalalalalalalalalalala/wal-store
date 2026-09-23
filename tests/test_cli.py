"""Tests for the ``python3 -m wal_store`` command line interface."""

import io
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

from wal_store import Store
from wal_store.__main__ import main


class _CapturedStdout:
    """Stand-in for sys.stdout exposing both text and binary output."""

    def __init__(self):
        self.text = io.StringIO()
        self.buffer = io.BytesIO()

    def write(self, data):
        return self.text.write(data)

    def flush(self):
        pass


class CliTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="wal-cli-")
        self.path = os.path.join(self.dir, "store")
        self.value_file = os.path.join(self.dir, "value.bin")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def run_cli(self, *argv, stdin=None):
        out, err = _CapturedStdout(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(list(argv))
        return code, out, err.getvalue()

    def test_recover_on_empty_store(self):
        code, out, err = self.run_cli("--path", self.path, "recover")
        self.assertEqual(code, 0)
        self.assertEqual(out.text.getvalue(), "0 0\n")
        self.assertEqual(err, "")

    def test_put_then_get_raw_bytes(self):
        payload = b"line one\n\x00\xffline two"
        with open(self.value_file, "wb") as fh:
            fh.write(payload)

        code, out, err = self.run_cli(
            "--path", self.path, "put", "key", "--value-file", self.value_file
        )
        self.assertEqual(code, 0)

        code, out, err = self.run_cli("--path", self.path, "get", "key")
        self.assertEqual(code, 0)
        self.assertEqual(out.buffer.getvalue(), payload)

        code, out, _ = self.run_cli("--path", self.path, "recover")
        self.assertEqual(code, 0)
        self.assertEqual(out.text.getvalue(), "1 1\n")

    def test_get_missing_exit_code_1(self):
        code, out, err = self.run_cli("--path", self.path, "get", "missing")
        self.assertEqual(code, 1)
        self.assertEqual(out.text.getvalue(), "")

    def test_usage_error_exit_code_2(self):
        import sys

        old_stderr = sys.stderr
        devnull = open(os.devnull, "w")
        sys.stderr = devnull
        try:
            with self.assertRaises(SystemExit) as ctx:
                main(["--path", self.path])  # no subcommand
        finally:
            sys.stderr = old_stderr
            devnull.close()
        self.assertEqual(ctx.exception.code, 2)

    def test_path_that_is_file_exit_code_3(self):
        file_path = os.path.join(self.dir, "plain")
        with open(file_path, "wb") as fh:
            fh.write(b"x")
        code, out, err = self.run_cli("--path", file_path, "recover")
        self.assertEqual(code, 3)
        self.assertIn("plain", err)
        self.assertEqual(out.text.getvalue(), "")

    def test_put_invalid_value_type_path_exit_code_3(self):
        # Missing value file surfaces as an OSError -> exit 3.
        code, _out, err = self.run_cli(
            "--path",
            self.path,
            "put",
            "k",
            "--value-file",
            os.path.join(self.dir, "no-such-file"),
        )
        self.assertEqual(code, 3)
        self.assertNotEqual(err, "")

    def test_data_survives_separate_invocations(self):
        with open(self.value_file, "wb") as fh:
            fh.write(b"persisted")
        self.run_cli(
            "--path", self.path, "put", "k", "--value-file", self.value_file
        )

        # A brand new process (new Store instance) sees the value.
        with Store(self.path) as store:
            self.assertEqual(store.get("k"), b"persisted")
            self.assertEqual(store.stats()["seq"], 1)


if __name__ == "__main__":
    unittest.main()
