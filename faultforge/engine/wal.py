import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class EventType(str, Enum):
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMPLETED = "STEP_COMPLETED"
    STEP_FAILED = "STEP_FAILED"
    WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED"


@dataclass(frozen=True)
class WALEntry:
    seq_id: int
    run_id: str
    step_id: str
    event_type: EventType
    idempotency_key: str
    payload: Dict[str, Any]
    timestamp: float


class WriteAheadLog:
    """Append-only Write-Ahead Log guaranteeing deterministic workflow replay."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS wal_events (
                    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    timestamp REAL NOT NULL
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_wal_lookup ON wal_events(run_id, step_id, event_type)"
            )

    @staticmethod
    def compute_idempotency_key(run_id: str, step_id: str, event_type: EventType, attempt: int = 1) -> str:
        raw = f"{run_id}:{step_id}:{event_type.value}:{attempt}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def append(
        self,
        run_id: str,
        step_id: str,
        event_type: EventType,
        payload: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> WALEntry:
        now = time.time()
        clean_payload = payload or {}
        serialized = json.dumps(clean_payload, sort_keys=True)
        idem_key = self.compute_idempotency_key(run_id, step_id, event_type, attempt)

        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO wal_events (run_id, step_id, event_type, idempotency_key, payload, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, step_id, event_type.value, idem_key, serialized, now),
            )
            seq_id = int(cursor.lastrowid or 0)

        return WALEntry(
            seq_id=seq_id,
            run_id=run_id,
            step_id=step_id,
            event_type=event_type,
            idempotency_key=idem_key,
            payload=clean_payload,
            timestamp=now,
        )

    def get_completed_step(self, run_id: str, step_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.execute(
            """
            SELECT payload FROM wal_events
            WHERE run_id = ? AND step_id = ? AND event_type = ?
            ORDER BY seq_id DESC LIMIT 1
            """,
            (run_id, step_id, EventType.STEP_COMPLETED.value),
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def get_history(self, run_id: str) -> List[WALEntry]:
        cursor = self.conn.execute(
            """
            SELECT seq_id, run_id, step_id, event_type, idempotency_key, payload, timestamp
            FROM wal_events
            WHERE run_id = ?
            ORDER BY seq_id ASC
            """,
            (run_id,),
        )
        return [
            WALEntry(
                seq_id=row[0],
                run_id=row[1],
                step_id=row[2],
                event_type=EventType(row[3]),
                idempotency_key=row[4],
                payload=json.loads(row[5]),
                timestamp=row[6],
            )
            for row in cursor.fetchall()
        ]
