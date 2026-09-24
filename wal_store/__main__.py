"""Command line interface for the wal_store package."""

from __future__ import annotations

import argparse
import json
import sys

from .store import Store


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wal_store",
        description="Write-ahead log key value store.",
    )
    parser.add_argument("--path", required=True, help="store directory")
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    sub.add_parser("recover", help="replay the log and report what was applied")

    p_put = sub.add_parser("put", help="store a value read from a file")
    p_put.add_argument("key")
    p_put.add_argument("--value-file", required=True,
                       help="file whose bytes become the value")

    p_get = sub.add_parser("get", help="write the value for a key to stdout")
    p_get.add_argument("key")

    p_del = sub.add_parser("delete", help="remove a key")
    p_del.add_argument("key")

    sub.add_parser("stats", help="print sequence, key count and log size")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        store = Store(args.path)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    with store:
        if args.command == "put":
            try:
                with open(args.value_file, "rb") as fh:
                    value = fh.read()
            except OSError as exc:
                print(
                    f"error: cannot read value file {args.value_file}: "
                    f"{exc.strerror or exc}",
                    file=sys.stderr,
                )
                return 1
            store.put(args.key, value)
            store.commit()
            return 0

        if args.command == "get":
            store.recover()
            value = store.get(args.key)
            if value is None:
                return 0
            sys.stdout.buffer.write(value)
            sys.stdout.buffer.flush()
            return 0

        if args.command == "delete":
            store.recover()
            store.delete(args.key)
            store.commit()
            return 0

        if args.command == "recover":
            print(json.dumps(store.recover()))
            return 0

        if args.command == "stats":
            store.recover()
            print(json.dumps(store.stats()))
            return 0

    return 2  # unreachable: argparse enforces a known command


if __name__ == "__main__":
    sys.exit(main())
