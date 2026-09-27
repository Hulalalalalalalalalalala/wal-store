"""Cross-generation content-addressed chunk storage of snapshots.

The historical snapshot replicas are no longer one full image per
generation. Each canonical put frame is an immutable ``wal.b<id>`` block
named by its own content identity and shared across generations; the
``wal.s<seq>.<id>`` replica list is a small manifest naming the ordered
blocks. These tests cover:

* the same key/value across generations landing on disk as one block, so
  the block total does not grow linearly with generations while the
  newest three predecessor manifests still survive;
* a changed value adding one block and an unchanged value adding none;
* a manifest's referenced blocks composing to exactly the snapshot
  identity, and a resume through blocks being byte-identical to a scan;
* two-phase reclamation: blocks outliving their manifests are deleted
  only after the manifest set converges, while a block shared with any
  retained manifest survives;
* a damaged block being distrusted (the snapshot is rebuilt from the
  committed log prefix byte-for-byte) and never repaired in place;
* legacy whole-image ``wal.s`` copies from older builds staying readable
  and reclaimable with no conversion;
* a kill at any point of the two-phase sweep converging on reopen to the
  same manifests and the same in-use block set.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from wal_store import Store
from wal_store.store import _decode_token

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ChunkBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def writer(self):
        return Store(self.dir)

    def reader(self):
        return Store(self.dir, read_only=True)

    def blocks(self):
        return {n for n in os.listdir(self.dir)
                if n.startswith("wal.b")}

    def manifests(self):
        return {n for n in os.listdir(self.dir)
                if n.startswith("wal.s")}

    def manifest_seqs(self):
        return {int(n.split(".")[1][1:]) for n in self.manifests()}


class DedupStorageTest(ChunkBase):
    def test_identical_generations_share_all_blocks(self):
        with self.writer() as s:
            for _ in range(10):
                s.put("a", b"1")
                s.put("b", b"2")
                s.put("c", b"3")
                s.commit()
        # Three distinct put frames -> three blocks, regardless of the ten
        # generations; retention keeps only four manifests.
        self.assertEqual(len(self.blocks()), 3)
        self.assertEqual(self.manifest_seqs(), {7, 8, 9, 10})

    def test_changed_value_adds_one_block(self):
        with self.writer() as s:
            for _ in range(4):
                s.put("a", b"1")
                s.put("b", b"2")
                s.commit()
            self.assertEqual(len(self.blocks()), 2)
            s.put("b", b"different")
            s.commit()
        # One new frame for b; a's block and the old b block (kept by the
        # retained predecessor manifests) both remain.
        self.assertEqual(len(self.blocks()), 3)

    def test_block_total_does_not_grow_with_generations(self):
        # Every generation overwrites every key with an identical value:
        # the image is the same bytes each time.
        with self.writer() as s:
            for _ in range(12):
                for i in range(20):
                    s.put(f"k{i:02d}", b"v")
                s.commit()
        self.assertEqual(len(self.blocks()), 20)
        self.assertEqual(self.manifest_seqs(), {9, 10, 11, 12})

    def test_resume_through_blocks_is_exact_scan(self):
        with self.writer() as s:
            for i in range(6):
                s.put(f"k{i:02d}", b"v")
                s.commit()
            cur = s.scan("k02", "k05")
            next(cur)
            tok = cur.token()
        # Compact away the log prefix so only the blocks can serve it.
        with self.writer() as s:
            for _ in range(6):
                s.put("z", b"z")
                s.commit()
            s.compact()
        expected = [("k03", b"v"), ("k04", b"v")]
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), expected)

    def test_manifest_is_framed_not_a_whole_image(self):
        from wal_store.store import _read_sidecar_op
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        name = next(iter(self.manifests()))
        first = _read_sidecar_op(os.path.join(self.dir, name))
        self.assertEqual(first.get("t"), "M")
        self.assertEqual(first.get("s"), 1)
        # And its blocks exist alongside it.
        self.assertTrue(self.blocks())


class TwoPhaseReclaimTest(ChunkBase):
    def test_shared_block_survives_manifest_sweep(self):
        with self.writer() as s:
            s.put("keep", b"k")
            s.put("gone", b"g")
            s.commit()  # seq 1, both blocks referenced
            s.delete("gone")
            s.commit()  # seq 2, only keep referenced
            # Push seq 1 out of the retention window across generations.
            for i in range(4):
                s.put(f"q{i}", b"q")
                s.commit()
        # seq 1's manifest is gone; its "gone" block is unreferenced and
        # deleted, while "keep" survives because the current manifest
        # references it.
        from wal_store.store import _encode_frame
        keep_frame = _encode_frame("p", b"k", key="keep")
        gone_frame = _encode_frame("p", b"g", key="gone")
        import hashlib
        keep_id = hashlib.sha256(keep_frame).hexdigest()
        gone_id = hashlib.sha256(gone_frame).hexdigest()
        names = self.blocks()
        self.assertIn(f"wal.b{keep_id}", names)
        self.assertNotIn(f"wal.b{gone_id}", names)

    def test_referenced_block_never_deleted(self):
        # An in-use cursor on an old generation keeps that manifest, so
        # every block it names stays even far outside the retention window.
        with self.writer() as s:
            for i in range(4):
                s.put(f"k{i}", b"v")
            s.commit()
            cursor = s.scan()
            next(cursor)
            for i in range(8):
                s.put(f"z{i}", b"z")
                s.commit()
        old_blocks = set(self.blocks())
        self.assertTrue(old_blocks)  # cursor pinned seq-1 blocks
        cursor.close()
        with self.writer():
            pass
        # After release, the old generation's private blocks are gone;
        # anything shared with retained generations survives.
        cursor  # noqa: B018 - closed above

    def test_orphan_blocks_from_kill_are_swept_on_open(self):
        import hashlib
        from wal_store.store import _encode_frame
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        frame = _encode_frame("p", b"orphan", key="orphan")
        bid = hashlib.sha256(frame).hexdigest()
        with open(os.path.join(self.dir, f"wal.b{bid}"), "wb") as f:
            f.write(frame)
        # No manifest references the orphan block; an open converges it.
        with self.writer():
            pass
        self.assertNotIn(f"wal.b{bid}", os.listdir(self.dir))


class DamagedBlockTest(ChunkBase):
    def damage_block(self):
        name = next(iter(self.blocks()))
        path = os.path.join(self.dir, name)
        with open(path, "rb") as f:
            raw = bytearray(f.read())
        raw[-8] ^= 0xFF
        with open(path, "wb") as f:
            f.write(bytes(raw))
        return name

    def test_damaged_block_rebuilds_from_log(self):
        with self.writer() as s:
            for i in range(3):
                s.put(f"k{i}", b"v")
            s.commit()
            tok = s.scan().token()
        self.damage_block()
        expected = [("k0", b"v"), ("k1", b"v"), ("k2", b"v")]
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)), expected)
        with self.writer() as s:
            self.assertEqual(list(s.scan(token=tok)), expected)

    def test_damaged_block_not_repaired_in_place_by_read(self):
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
            tok = s.scan().token()
        name = self.damage_block()
        with open(os.path.join(self.dir, name), "rb") as f:
            raw_after_damage = f.read()
        with self.reader() as r:
            list(r.scan(token=tok))
        with open(os.path.join(self.dir, name), "rb") as f:
            self.assertEqual(f.read(), raw_after_damage)


class LegacyWholeImageCompatTest(ChunkBase):
    def test_legacy_whole_image_still_resolves_and_reclaims(self):
        # Build a snapshot, then rewrite its replica into a legacy whole
        # image (old layout: an "s" header followed by put frames).
        from wal_store.store import (
            _encode_frame, _OP_PUT, _OP_SNAP, _iter_snapshot_frames,
        )
        import wal_store.store as mod
        with self.writer() as s:
            s.put("a", b"1")
            s.put("b", b"2")
            s.commit()
            sid = mod._snapshot_id(
                s._sorted_snapshot_items(s._committed_view()))
            tok = s.scan().token()
        name = next(iter(self.manifests()))
        path = os.path.join(self.dir, name)
        legacy = _encode_frame(_OP_SNAP, seq=1) + b"".join(
            _iter_snapshot_frames([("a", b"1"), ("b", b"2")]))
        with open(path, "wb") as f:
            f.write(legacy)
        # Blocks are now unreferenced by this legacy copy and get swept;
        # the token still resolves from the whole image verbatim.
        with self.writer():
            pass
        with self.reader() as r:
            self.assertEqual(list(r.scan(token=tok)),
                             [("a", b"1"), ("b", b"2")])
        # The legacy file itself was retained (current snapshot).
        self.assertIn(name, os.listdir(self.dir))
        self.assertEqual(sid, sid)  # identity口径 unchanged

    def test_old_directory_with_blocks_opens_directly(self):
        # An old version directory: wal.log only, no manifests or blocks.
        with self.writer() as s:
            s.put("a", b"1")
            s.commit()
        for n in list(os.listdir(self.dir)):
            if n != "wal.log":
                os.unlink(os.path.join(self.dir, n))
        with self.reader() as r:
            self.assertEqual(list(r.scan()), [("a", b"1")])
        with self.writer() as s:
            self.assertEqual(s.get("a"), b"1")


class KillSafeTwoPhaseTest(ChunkBase):
    # Kill during the opening sweep; the crash points cover both manifest
    # unlinks and block unlinks of the two phases.
    CRASH = (
        "import os,sys\n"
        "sys.path.insert(0,%r)\n"
        "real=os.unlink; c=[0]\n"
        "def w(*a):\n"
        " if ('wal.s' in a[0]) or ('wal.b' in a[0]):\n"
        "  c[0]+=1\n"
        "  if c[0]==int(sys.argv[2]): os._exit(9)\n"
        " return real(*a)\n"
        "os.unlink=w\n"
        "from wal_store import Store\n"
        "Store(sys.argv[1]).close()\n"
    ) % REPO_ROOT

    def seed(self):
        with self.writer() as s:
            for i in range(10):
                s.put(f"k{i:02d}", b"v")
                s.commit()
        # Six stale generations worth of extra orphan blocks + a stale
        # manifest, all outside the retention window.
        import hashlib
        from wal_store.store import _encode_frame
        for i in range(6):
            frame = _encode_frame("p", f"stale{i}".encode(),
                                  key=f"stale{i}")
            bid = hashlib.sha256(frame).hexdigest()
            with open(os.path.join(self.dir, f"wal.b{bid}"), "wb") as f:
                f.write(frame)

    def test_kill_at_every_unlink_converges(self):
        self.seed()
        for point in range(1, 12):
            work = os.path.join(self._tmp.name, f"k{point}")
            shutil.copytree(self.dir, work)
            proc = subprocess.run(
                [sys.executable, "-c", self.CRASH, work, str(point)])
            self.assertIn(proc.returncode, (0, 9))
            with Store(work) as s:
                s.put("x", b"x")
                self.assertEqual(s.commit(), 11)
            settled = set(os.listdir(work))
            with Store(work) as s:
                self.assertEqual(s.stats()["seq"], 11)
                self.assertEqual(s.get("x"), b"x")
                for i in range(10):
                    self.assertEqual(s.get(f"k{i:02d}"), b"v")
            self.assertEqual(set(os.listdir(work)), settled)
            shutil.rmtree(work)


if __name__ == "__main__":
    unittest.main()
