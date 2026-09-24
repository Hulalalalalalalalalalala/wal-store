"""Command line interface for wal-store.

Usage::

    python -m wal_store --path DIR recover
    python -m wal_store --path DIR put KEY --value-file FILE
    python -m wal_store --path DIR get KEY

Exit codes: 0 success, 1 missing key on get, 2 usage error, 3 storage error.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .store import Store


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wal_store",
        description="Write-ahead log key value store.")
    parser.add_argument("--path", required=True,
                        help="store directory (must already exist)")

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
            with Store(args.path) as store:
                value = store.get(args.key)
            if value is None:
                return 1
            sys.stdout.buffer.write(value)
            sys.stdout.buffer.flush()
            return 0

        if args.command == "put":
            with open(args.value_file, "rb") as f:
                value = f.read()
            with Store(args.path) as store:
                store.put(args.key, value)
                store.commit()
            return 0

        if args.command == "recover":
            with Store(args.path) as store:
                result = store.recover()
            sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
            return 0
    except (ValueError, TypeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
