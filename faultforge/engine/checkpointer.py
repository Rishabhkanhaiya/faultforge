"""State snapshot compaction and Merkle-tree verification for deterministic execution."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class StateDriftError(RuntimeError):
    """Raised when replayed state fails Merkle root or cryptographic hash verification."""


@dataclass(frozen=True)
class MerkleNode:
    """Node in a cryptographic state verification tree."""

    hash_val: str
    left: Optional[MerkleNode] = None
    right: Optional[MerkleNode] = None
    key: Optional[str] = None


class MerkleTree:
    """Binary Merkle Tree constructed over sorted key-value state representations."""

    def __init__(self, state_dict: Dict[str, Any]) -> None:
        self.leaves: List[MerkleNode] = []
        # Sort items deterministically
        for k in sorted(state_dict.keys()):
            val_serialized = json.dumps(state_dict[k], sort_keys=True)
            leaf_data = f"{k}:{val_serialized}".encode("utf-8")
            h = hashlib.sha256(leaf_data).hexdigest()
            self.leaves.append(MerkleNode(hash_val=h, key=k))

        self.root: Optional[MerkleNode] = self._build_tree(self.leaves)

    def _build_tree(self, nodes: List[MerkleNode]) -> Optional[MerkleNode]:
        if not nodes:
            return MerkleNode(hash_val=hashlib.sha256(b"empty_state").hexdigest())
        if len(nodes) == 1:
            return nodes[0]

        parent_level: List[MerkleNode] = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            if i + 1 < len(nodes):
                right = nodes[i + 1]
            else:
                # Odd leaf duplicated per Merkle tree standard (RFC 6962 / Bitcoin standard)
                right = nodes[i]

            combined = (left.hash_val + right.hash_val).encode("utf-8")
            parent_hash = hashlib.sha256(combined).hexdigest()
            parent_level.append(MerkleNode(hash_val=parent_hash, left=left, right=right))

        return self._build_tree(parent_level)

    @property
    def root_hash(self) -> str:
        """Return hex representation of Merkle tree root."""
        return self.root.hash_val if self.root else ""


@dataclass(frozen=True)
class CheckpointSnapshot:
    """Compacted state snapshot with cryptographic Merkle proof root."""

    snapshot_id: int
    run_id: str
    wal_seq_watermark: int
    merkle_root: str
    compacted_state: Dict[str, Any]
    created_at: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary for telemetry and inspection."""
        return {
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "wal_seq_watermark": self.wal_seq_watermark,
            "merkle_root": self.merkle_root,
            "compacted_state": self.compacted_state,
            "created_at": self.created_at,
        }


class StateCheckpointer:
    """Manages periodic checkpoint compaction and cryptographic drift verification."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    wal_seq_watermark INTEGER NOT NULL,
                    merkle_root TEXT NOT NULL,
                    state_payload TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshots_run ON state_snapshots(run_id, wal_seq_watermark)"
            )

    def create_snapshot(
        self,
        run_id: str,
        wal_seq_watermark: int,
        current_state: Dict[str, Any],
        now: Optional[float] = None,
    ) -> CheckpointSnapshot:
        """Compact active execution state into an immutable cryptographic snapshot."""
        created_at = now if now is not None else time.time()
        tree = MerkleTree(current_state)
        merkle_root = tree.root_hash
        serialized = json.dumps(current_state, sort_keys=True)

        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO state_snapshots (run_id, wal_seq_watermark, merkle_root, state_payload, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, wal_seq_watermark, merkle_root, serialized, created_at),
            )
            snapshot_id = int(cursor.lastrowid or 0)

        return CheckpointSnapshot(
            snapshot_id=snapshot_id,
            run_id=run_id,
            wal_seq_watermark=wal_seq_watermark,
            merkle_root=merkle_root,
            compacted_state=dict(current_state),
            created_at=created_at,
        )

    def verify_state_drift(self, run_id: str, candidate_state: Dict[str, Any]) -> bool:
        """Verify whether candidate state matches the latest recorded checkpoint root."""
        latest = self.get_latest_snapshot(run_id)
        if latest is None:
            return True  # No prior snapshot to verify against

        candidate_tree = MerkleTree(candidate_state)
        if candidate_tree.root_hash != latest.merkle_root:
            raise StateDriftError(
                f"State drift detected for run '{run_id}': expected Merkle root {latest.merkle_root}, got {candidate_tree.root_hash}"
            )
        return True

    def get_latest_snapshot(self, run_id: str) -> Optional[CheckpointSnapshot]:
        """Retrieve most recent checkpoint for a given run."""
        cursor = self.conn.execute(
            """
            SELECT snapshot_id, run_id, wal_seq_watermark, merkle_root, state_payload, created_at
            FROM state_snapshots
            WHERE run_id = ?
            ORDER BY snapshot_id DESC LIMIT 1
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return CheckpointSnapshot(
            snapshot_id=row[0],
            run_id=row[1],
            wal_seq_watermark=row[2],
            merkle_root=row[3],
            compacted_state=json.loads(row[4]),
            created_at=row[5],
        )
