"""wal-store: write-ahead log key value store with crash recovery."""

from .store import CorruptLogError, Store, inject_tear, open_readonly

__all__ = ["Store", "CorruptLogError", "inject_tear", "open_readonly"]
