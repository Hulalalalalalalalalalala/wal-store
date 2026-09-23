"""Command line entry point: ``python3 -m wal_store``.

    python3 -m wal_store --path ./store recover
    python3 -m wal_store --path ./store put <key> --value-file <path>
    python3 -m wal_store --path ./store get <key>

Exit codes:
    0  success
    1  get on a missing key
    2  usage error (argparse)
    3  storage layer error
"""

import argparse
import sys

from .store import Store


def _build_parser():
    parser = argparse.ArgumentParser(prog="wal_store")
    parser.add_argument("--path", required=True, help="store directory")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("recover", help="replay the log and report applied/seq")

    put = sub.add_parser("put", help="store a value")
    put.add_argument("key")
    put.add_argument(
        "--value-file",
        required=True,
        help="file whose raw bytes are stored as the value (- for stdin)",
    )

    get = sub.add_parser("get", help="write a value to stdout")
    get.add_argument("key")

    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        store = Store(args.path)
    except OSError as exc:
        print(f"wal_store: {exc}", file=sys.stderr)
        return 3

    try:
        if args.command == "recover":
            report = store.recover()
            sys.stdout.write(f"{report['applied']} {report['seq']}\n")
            return 0

        if args.command == "put":
            if args.value_file == "-":
                value = sys.stdin.buffer.read()
            else:
                with open(args.value_file, "rb") as fh:
                    value = fh.read()
            store.put(args.key, value)
            store.commit()
            return 0

        # get
        value = store.get(args.key)
        if value is None:
            return 1
        sys.stdout.buffer.write(value)
        sys.stdout.buffer.flush()
        return 0
    except (OSError, ValueError, TypeError) as exc:
        print(f"wal_store: {exc}", file=sys.stderr)
        return 3
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
