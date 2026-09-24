"""Command line interface for wal_store.

    python3 -m wal_store --path ./store recover
    python3 -m wal_store --path ./store put <key> --value-file <path>
    python3 -m wal_store --path ./store get <key>
    python3 -m wal_store --path ./store delete <key>
    python3 -m wal_store --path ./store stats
"""

from __future__ import annotations

import argparse
import sys

from .store import Store


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wal_store",
        description="Write-ahead log key value store.",
    )
    parser.add_argument("--path", required=True, help="store directory")
    commands = parser.add_subparsers(dest="command", metavar="<command>", required=True)

    commands.add_parser("recover", help="replay the log and report what was applied")

    put = commands.add_parser("put", help="store a value read from a file")
    put.add_argument("key")
    put.add_argument("--value-file", required=True, help="file whose bytes become the value")

    get = commands.add_parser("get", help="write the current value to stdout")
    get.add_argument("key")

    delete = commands.add_parser("delete", help="remove a key")
    delete.add_argument("key")

    commands.add_parser("stats", help="print sequence, key count and log size")
    return parser


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        store = Store(args.path)
    except FileNotFoundError:
        print(f"wal_store: store directory not found: {args.path}", file=sys.stderr)
        return 1

    try:
        report = store.recover()
    except ValueError as exc:
        print(f"wal_store: {exc}", file=sys.stderr)
        return 1

    try:
        if args.command == "recover":
            print(f"applied={report['applied']} sequence={report['sequence']}")
            return 0

        if args.command == "put":
            try:
                with open(args.value_file, "rb") as value_file:
                    value = value_file.read()
            except OSError as exc:
                reason = exc.strerror or str(exc)
                print(
                    f"wal_store: cannot read value file {args.value_file}: {reason}",
                    file=sys.stderr,
                )
                return 1
            store.put(args.key, value)
            print(store.commit())
            return 0

        if args.command == "get":
            value = store.get(args.key)
            if value is None:
                return 0
            sys.stdout.buffer.write(value)
            sys.stdout.buffer.flush()
            return 0

        if args.command == "delete":
            store.delete(args.key)
            print(store.commit())
            return 0

        if args.command == "stats":
            stats = store.stats()
            print(
                f"sequence={stats['sequence']} "
                f"keys={stats['keys']} "
                f"bytes={stats['bytes']}"
            )
            return 0
    except (TypeError, ValueError) as exc:
        print(f"wal_store: {exc}", file=sys.stderr)
        return 1

    print("wal_store: unknown command", file=sys.stderr)  # pragma: no cover
    return 2  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
