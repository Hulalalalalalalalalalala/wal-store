"""Command line interface for wal-store.

Usage::

    python -m wal_store --path DIR recover
    python -m wal_store --path DIR put KEY --value-file FILE
    python -m wal_store --path DIR get KEY

Exit codes: 0 success, 1 missing key on get, 2 usage error, 3 storage error
(including a missing store directory and a corrupt log).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Sequence

from .store import Store


def _write_stdout(data: bytes) -> None:
    """Write raw bytes to stdout without any newline translation.

    Always goes through the binary buffer so a trailing LF cannot be
    expanded to CRLF on Windows. A stdout without a binary buffer (test
    doubles, exotic embeddings) gets the same bytes decoded instead.
    """
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:  # pragma: no cover - text-only stdout environments
        sys.stdout.write(data.decode("utf-8"))
        sys.stdout.flush()


def _write_stderr(data: bytes) -> None:
    """Write raw bytes to stderr without any newline translation.

    Same binary rule as ``_write_stdout``: the one error line must come out
    byte-identical on every platform, never with its LF expanded to CRLF.
    """
    buffer = getattr(sys.stderr, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:  # pragma: no cover - text-only stderr environments
        sys.stderr.write(data.decode("utf-8"))
        sys.stderr.flush()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wal_store",
        description="Write-ahead log key value store.")
    parser.add_argument("--path", required=True,
                        help="store directory (created if missing)")

    commands = parser.add_subparsers(dest="command", required=True)

    p_put = commands.add_parser("put", help="store a key/value pair")
    p_put.add_argument("key")
    p_put.add_argument("--value-file", required=True,
                       help="file whose raw bytes are stored as the value")

    p_get = commands.add_parser("get", help="print a value to stdout")
    p_get.add_argument("key")

    commands.add_parser("recover", help="replay the log and report counts")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "get":
            # Read-only: many get processes may run alongside the one writer
            # and never modify the store.
            with Store(args.path, read_only=True) as store:
                value = store.get(args.key)
            if value is None:
                return 1
            _write_stdout(value)
            return 0

        if args.command == "put":
            with open(args.value_file, "rb") as f:
                value = f.read()
            # Writing initializes a new store; recovery on a missing
            # directory stays an error handled by Store itself.
            os.makedirs(args.path, exist_ok=True)
            with Store(args.path) as store:
                store.put(args.key, value)
                store.commit()
            return 0

        if args.command == "recover":
            with Store(args.path) as store:
                result = store.recover()
            # Key order follows the report: applied, discarded, seq. Written
            # as raw bytes so the trailing LF is not expanded on Windows.
            line = (json.dumps(result, separators=(",", ":")) + "\n")
            _write_stdout(line.encode("utf-8"))
            return 0
    except (ValueError, TypeError, OSError) as exc:
        # One explanatory line on stderr, written as raw bytes so the LF is
        # not expanded to CRLF on Windows; no JSON report on a failure.
        _write_stderr(f"error: {exc}\n".encode("utf-8"))
        return 3

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
