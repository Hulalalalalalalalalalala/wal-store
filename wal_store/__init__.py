"""wal-store: write-ahead log key value store with crash recovery."""

from .store import CorruptLogError, Store

__all__ = ["Store", "CorruptLogError"]
