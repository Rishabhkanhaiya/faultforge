"""Retry policies, exponential backoff with full jitter, and Dead-Letter Queue (DLQ)."""

from __future__ import annotations

import json
import random
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type


class BackoffStrategy(str, Enum):
    EXPONENTIAL = "EXPONENTIAL"
    CONSTANT = "CONSTANT"
    LINEAR = "LINEAR"
    FULL_JITTER = "FULL_JITTER"


@dataclass
class RetryPolicy:
    """Configurable retry policy with deterministic exponential backoff and jitter."""

    max_retries: int = 3
    initial_delay_sec: float = 0.1
    max_delay_sec: float = 10.0
    backoff_multiplier: float = 2.0
    strategy: BackoffStrategy = BackoffStrategy.FULL_JITTER
    non_retryable_exceptions: Tuple[Type[BaseException], ...] = ()
    seed: Optional[int] = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be non-negative, got {self.max_retries}")
        if self.initial_delay_sec < 0.0 or self.max_delay_sec < self.initial_delay_sec:
            raise ValueError("Invalid delay boundaries for RetryPolicy")
        self._rng = random.Random(self.seed if self.seed is not None else 42)

    def should_retry(self, exception: BaseException, attempt: int) -> bool:
        """Evaluate if an exception at the given attempt index is retryable."""
        if attempt > self.max_retries:
            return False
        for non_retryable in self.non_retryable_exceptions:
            if isinstance(exception, non_retryable):
                return False
        return True

    def compute_backoff(self, attempt: int) -> float:
        """Compute backoff delay in seconds using the configured strategy."""
        if attempt <= 0:
            return 0.0

        if self.strategy == BackoffStrategy.CONSTANT:
            delay = self.initial_delay_sec
        elif self.strategy == BackoffStrategy.LINEAR:
            delay = self.initial_delay_sec * attempt
        elif self.strategy in (BackoffStrategy.EXPONENTIAL, BackoffStrategy.FULL_JITTER):
            exponent = min(attempt - 1, 16)  # Prevent extreme float overflow
            delay = self.initial_delay_sec * (self.backoff_multiplier ** exponent)
        else:
            delay = self.initial_delay_sec

        capped = min(self.max_delay_sec, delay)

        if self.strategy == BackoffStrategy.FULL_JITTER:
            # Deterministic PRNG if seed is provided
            return self._rng.uniform(0.0, capped)

        return capped


@dataclass(frozen=True)
class DLQRecord:
    """Immutable dead-letter record representing an unrecoverable step failure."""

    dlq_id: int
    run_id: str
    step_id: str
    error_type: str
    error_message: str
    attempts: int
    payload: Dict[str, Any]
    failed_at: float
    redrive_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary for JSON serialization."""
        return {
            "dlq_id": self.dlq_id,
            "run_id": self.run_id,
            "step_id": self.step_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "attempts": self.attempts,
            "payload": self.payload,
            "failed_at": self.failed_at,
            "redrive_count": self.redrive_count,
        }


class DeadLetterQueue:
    """ACID SQLite storage for failed tasks needing inspection, alerting, or manual redrive."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dlq_records (
                    dlq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    failed_at REAL NOT NULL,
                    redrive_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dlq_lookup ON dlq_records(run_id, step_id)"
            )

    def route_to_dlq(
        self,
        run_id: str,
        step_id: str,
        error: BaseException,
        attempts: int,
        payload: Optional[Dict[str, Any]] = None,
        now: Optional[float] = None,
    ) -> DLQRecord:
        """Route an exhausted or non-retryable step failure into the dead-letter queue."""
        failed_at = now if now is not None else time.time()
        serialized = json.dumps(payload or {}, sort_keys=True)
        err_type = type(error).__name__
        err_msg = str(error)

        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO dlq_records (run_id, step_id, error_type, error_message, attempts, payload, failed_at, redrive_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (run_id, step_id, err_type, err_msg, attempts, serialized, failed_at),
            )
            dlq_id = int(cursor.lastrowid or 0)

        return DLQRecord(
            dlq_id=dlq_id,
            run_id=run_id,
            step_id=step_id,
            error_type=err_type,
            error_message=err_msg,
            attempts=attempts,
            payload=payload or {},
            failed_at=failed_at,
            redrive_count=0,
        )

    def list_records(self, limit: int = 50, offset: int = 0) -> List[DLQRecord]:
        """Fetch dead-letter records with pagination."""
        cursor = self.conn.execute(
            """
            SELECT dlq_id, run_id, step_id, error_type, error_message, attempts, payload, failed_at, redrive_count
            FROM dlq_records
            ORDER BY dlq_id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        return [
            DLQRecord(
                dlq_id=r[0],
                run_id=r[1],
                step_id=r[2],
                error_type=r[3],
                error_message=r[4],
                attempts=r[5],
                payload=json.loads(r[6]),
                failed_at=r[7],
                redrive_count=r[8],
            )
            for r in cursor.fetchall()
        ]

    def get_record(self, dlq_id: int) -> Optional[DLQRecord]:
        """Fetch a single DLQ record by ID."""
        cursor = self.conn.execute(
            """
            SELECT dlq_id, run_id, step_id, error_type, error_message, attempts, payload, failed_at, redrive_count
            FROM dlq_records WHERE dlq_id = ?
            """,
            (dlq_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return DLQRecord(
            dlq_id=row[0],
            run_id=row[1],
            step_id=row[2],
            error_type=row[3],
            error_message=row[4],
            attempts=row[5],
            payload=json.loads(row[6]),
            failed_at=row[7],
            redrive_count=row[8],
        )

    def redrive(self, dlq_id: int) -> Optional[DLQRecord]:
        """Increment redrive count when an operator initiates manual workflow redrive."""
        with self.conn:
            self.conn.execute(
                "UPDATE dlq_records SET redrive_count = redrive_count + 1 WHERE dlq_id = ?",
                (dlq_id,),
            )
            return self.get_record(dlq_id)

    def purge(self, dlq_id: int) -> bool:
        """Remove a DLQ record after successful resolution."""
        with self.conn:
            cursor = self.conn.execute("DELETE FROM dlq_records WHERE dlq_id = ?", (dlq_id,))
            return cursor.rowcount > 0

    def count(self) -> int:
        """Return total pending DLQ entries."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM dlq_records")
        row = cursor.fetchone()
        return int(row[0]) if row else 0
