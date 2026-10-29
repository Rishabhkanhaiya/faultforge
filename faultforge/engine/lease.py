"""Distributed leasing, heartbeat management, and optimistic concurrency control."""

from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class LeaseError(Exception):
    """Base exception for lease operations."""


class LeaseAcquisitionError(LeaseError):
    """Raised when a worker fails to acquire an active step lease."""


class LeaseExpiredError(LeaseError):
    """Raised when an operation is attempted on an expired lease."""


class LeaseVersionConflictError(LeaseError):
    """Raised when optimistic concurrency detects a version collision."""


@dataclass(frozen=True)
class WorkerLease:
    """Represents an active, optimistic-concurrency lease for a workflow step."""

    step_id: str
    owner_id: str
    lease_token: str
    acquired_at: float
    expires_at: float
    version: int
    heartbeat_count: int

    def is_expired(self, current_time: Optional[float] = None) -> bool:
        """Check if the lease has expired past its TTL."""
        now = current_time if current_time is not None else time.time()
        return now >= self.expires_at

    def time_remaining(self, current_time: Optional[float] = None) -> float:
        """Return remaining lease duration in seconds, clamped to 0.0."""
        now = current_time if current_time is not None else time.time()
        return max(0.0, self.expires_at - now)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize lease state to a dictionary."""
        return {
            "step_id": self.step_id,
            "owner_id": self.owner_id,
            "lease_token": self.lease_token,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
            "version": self.version,
            "heartbeat_count": self.heartbeat_count,
            "expired": self.is_expired(),
            "time_remaining_sec": round(self.time_remaining(), 3),
        }


class LeaseManager:
    """ACID SQLite-backed lease coordinator with optimistic concurrency and zombie recovery."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS step_leases (
                    step_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    version INTEGER NOT NULL,
                    heartbeat_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leases_expiry ON step_leases(expires_at)"
            )

    def acquire_lease(
        self,
        step_id: str,
        owner_id: str,
        ttl_seconds: float = 5.0,
        now: Optional[float] = None,
    ) -> WorkerLease:
        """Acquire a lease on a step, renewing if already owned or reclaiming if zombied."""
        if ttl_seconds <= 0.0:
            raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")

        current_time = now if now is not None else time.time()
        new_expires_at = current_time + ttl_seconds
        new_token = str(uuid.uuid4())

        with self.conn:
            cursor = self.conn.execute(
                "SELECT owner_id, lease_token, expires_at, version, heartbeat_count FROM step_leases WHERE step_id = ?",
                (step_id,),
            )
            row = cursor.fetchone()

            if row is None:
                # Brand new lease
                version = 1
                self.conn.execute(
                    """
                    INSERT INTO step_leases (step_id, owner_id, lease_token, acquired_at, expires_at, version, heartbeat_count)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                    """,
                    (step_id, owner_id, new_token, current_time, new_expires_at, version),
                )
                return WorkerLease(
                    step_id=step_id,
                    owner_id=owner_id,
                    lease_token=new_token,
                    acquired_at=current_time,
                    expires_at=new_expires_at,
                    version=version,
                    heartbeat_count=0,
                )

            curr_owner, curr_token, curr_expires_at, curr_version, curr_heartbeats = row

            # If current lease is expired, allow takeover / zombie reclamation
            if current_time >= curr_expires_at:
                new_version = curr_version + 1
                self.conn.execute(
                    """
                    UPDATE step_leases
                    SET owner_id = ?, lease_token = ?, acquired_at = ?, expires_at = ?, version = ?, heartbeat_count = 0
                    WHERE step_id = ? AND version = ?
                    """,
                    (owner_id, new_token, current_time, new_expires_at, new_version, step_id, curr_version),
                )
                return WorkerLease(
                    step_id=step_id,
                    owner_id=owner_id,
                    lease_token=new_token,
                    acquired_at=current_time,
                    expires_at=new_expires_at,
                    version=new_version,
                    heartbeat_count=0,
                )

            # Not expired: check if the requester is the active owner renewing
            if curr_owner == owner_id:
                new_version = curr_version + 1
                self.conn.execute(
                    """
                    UPDATE step_leases
                    SET expires_at = ?, version = ?, heartbeat_count = heartbeat_count + 1
                    WHERE step_id = ? AND version = ?
                    """,
                    (new_expires_at, new_version, step_id, curr_version),
                )
                return WorkerLease(
                    step_id=step_id,
                    owner_id=owner_id,
                    lease_token=curr_token,
                    acquired_at=current_time,
                    expires_at=new_expires_at,
                    version=new_version,
                    heartbeat_count=curr_heartbeats + 1,
                )

            # Owned by another healthy worker
            raise LeaseAcquisitionError(
                f"Step '{step_id}' is actively locked by worker '{curr_owner}' until timestamp {curr_expires_at:.3f}"
            )

    def renew_lease(
        self,
        step_id: str,
        owner_id: str,
        lease_token: str,
        expected_version: int,
        ttl_seconds: float = 5.0,
        now: Optional[float] = None,
    ) -> WorkerLease:
        """Renew an existing lease via optimistic version matching."""
        current_time = now if now is not None else time.time()
        new_expires_at = current_time + ttl_seconds

        with self.conn:
            cursor = self.conn.execute(
                "SELECT owner_id, lease_token, expires_at, version, heartbeat_count FROM step_leases WHERE step_id = ?",
                (step_id,),
            )
            row = cursor.fetchone()

            if row is None:
                raise LeaseExpiredError(f"Lease for step '{step_id}' does not exist.")

            curr_owner, curr_token, curr_expires_at, curr_version, curr_heartbeats = row

            if current_time >= curr_expires_at:
                raise LeaseExpiredError(f"Lease for step '{step_id}' expired at {curr_expires_at:.3f}")

            if curr_owner != owner_id or curr_token != lease_token:
                raise LeaseAcquisitionError(f"Worker '{owner_id}' does not hold valid token for step '{step_id}'")

            if curr_version != expected_version:
                raise LeaseVersionConflictError(
                    f"Version conflict on step '{step_id}': expected {expected_version}, found {curr_version}"
                )

            new_version = curr_version + 1
            new_heartbeats = curr_heartbeats + 1
            self.conn.execute(
                """
                UPDATE step_leases
                SET expires_at = ?, version = ?, heartbeat_count = ?
                WHERE step_id = ? AND version = ?
                """,
                (new_expires_at, new_version, new_heartbeats, step_id, curr_version),
            )

            return WorkerLease(
                step_id=step_id,
                owner_id=owner_id,
                lease_token=lease_token,
                acquired_at=current_time,
                expires_at=new_expires_at,
                version=new_version,
                heartbeat_count=new_heartbeats,
            )

    def release_lease(self, step_id: str, owner_id: str, lease_token: str) -> bool:
        """Release lease ownership if matching owner and token."""
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM step_leases WHERE step_id = ? AND owner_id = ? AND lease_token = ?",
                (step_id, owner_id, lease_token),
            )
            return cursor.rowcount > 0

    def reclaim_zombie_leases(self, now: Optional[float] = None) -> List[str]:
        """Find and delete all expired leases, returning list of recovered step IDs."""
        current_time = now if now is not None else time.time()
        with self.conn:
            cursor = self.conn.execute(
                "SELECT step_id FROM step_leases WHERE expires_at <= ?",
                (current_time,),
            )
            expired_steps = [row[0] for row in cursor.fetchall()]
            if expired_steps:
                self.conn.execute(
                    "DELETE FROM step_leases WHERE expires_at <= ?",
                    (current_time,),
                )
            return expired_steps

    def get_lease(self, step_id: str) -> Optional[WorkerLease]:
        """Fetch current lease status for a given step."""
        cursor = self.conn.execute(
            """
            SELECT step_id, owner_id, lease_token, acquired_at, expires_at, version, heartbeat_count
            FROM step_leases WHERE step_id = ?
            """,
            (step_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return WorkerLease(
            step_id=row[0],
            owner_id=row[1],
            lease_token=row[2],
            acquired_at=row[3],
            expires_at=row[4],
            version=row[5],
            heartbeat_count=row[6],
        )

    def list_active_leases(self) -> List[WorkerLease]:
        """List all currently active (non-expired) leases."""
        now = time.time()
        cursor = self.conn.execute(
            """
            SELECT step_id, owner_id, lease_token, acquired_at, expires_at, version, heartbeat_count
            FROM step_leases WHERE expires_at > ?
            ORDER BY expires_at ASC
            """,
            (now,),
        )
        return [
            WorkerLease(
                step_id=r[0],
                owner_id=r[1],
                lease_token=r[2],
                acquired_at=r[3],
                expires_at=r[4],
                version=r[5],
                heartbeat_count=r[6],
            )
            for r in cursor.fetchall()
        ]
