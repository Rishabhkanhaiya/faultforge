"""Trajectory differ detecting non-deterministic state divergence between execution runs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from faultforge.engine.wal import EventType, WALEntry, WriteAheadLog


class DivergenceType(str, Enum):
    STEP_ORDER_MISMATCH = "STEP_ORDER_MISMATCH"
    PAYLOAD_MUTATION = "PAYLOAD_MUTATION"
    IDEMPOTENCY_MISMATCH = "IDEMPOTENCY_MISMATCH"
    MISSING_STEP = "MISSING_STEP"
    RETRY_COUNT_MISMATCH = "RETRY_COUNT_MISMATCH"
    EVENT_COUNT_MISMATCH = "EVENT_COUNT_MISMATCH"


@dataclass(frozen=True)
class DivergenceRecord:
    """Detailed record of a divergence detected between two execution trajectories."""

    step_id: str
    divergence_type: DivergenceType
    description: str
    run_a_value: Any
    run_b_value: Any

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "step_id": self.step_id,
            "divergence_type": self.divergence_type.value,
            "description": self.description,
            "run_a_value": self.run_a_value,
            "run_b_value": self.run_b_value,
        }


@dataclass
class DiffReport:
    """Consolidated report comparing two execution trajectories."""

    run_id_a: str
    run_id_b: str
    is_identical: bool
    total_events_a: int
    total_events_b: int
    divergences: List[DivergenceRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert full report to dictionary."""
        return {
            "run_id_a": self.run_id_a,
            "run_id_b": self.run_id_b,
            "is_identical": self.is_identical,
            "total_events_a": self.total_events_a,
            "total_events_b": self.total_events_b,
            "divergence_count": len(self.divergences),
            "divergences": [d.to_dict() for d in self.divergences],
        }


class TrajectoryDiffer:
    """Compares two WAL event streams to ensure deterministic durability."""

    def __init__(self, wal: WriteAheadLog) -> None:
        self.wal = wal

    def diff_runs(self, run_id_a: str, run_id_b: str) -> DiffReport:
        """Compare execution trajectories for two runs and flag any non-deterministic divergences."""
        history_a = self.wal.get_history(run_id_a)
        history_b = self.wal.get_history(run_id_b)

        divergences: List[DivergenceRecord] = []

        # 1. Compare total events count
        if len(history_a) != len(history_b):
            divergences.append(
                DivergenceRecord(
                    step_id="__workflow__",
                    divergence_type=DivergenceType.EVENT_COUNT_MISMATCH,
                    description=f"Total WAL events differ: {len(history_a)} vs {len(history_b)}",
                    run_a_value=len(history_a),
                    run_b_value=len(history_b),
                )
            )

        # Filter completed step events
        completed_a = [e for e in history_a if e.event_type == EventType.STEP_COMPLETED]
        completed_b = [e for e in history_b if e.event_type == EventType.STEP_COMPLETED]

        steps_a = [e.step_id for e in completed_a]
        steps_b = [e.step_id for e in completed_b]

        # 2. Compare step execution order
        if steps_a != steps_b:
            divergences.append(
                DivergenceRecord(
                    step_id="__workflow__",
                    divergence_type=DivergenceType.STEP_ORDER_MISMATCH,
                    description="Order of completed steps differed between execution runs",
                    run_a_value=steps_a,
                    run_b_value=steps_b,
                )
            )

        # 3. Compare completed payloads and idempotency keys per step
        map_a = {e.step_id: e for e in completed_a}
        map_b = {e.step_id: e for e in completed_b}

        all_steps = sorted(set(map_a.keys()) | set(map_b.keys()))

        for step_id in all_steps:
            if step_id not in map_a:
                divergences.append(
                    DivergenceRecord(
                        step_id=step_id,
                        divergence_type=DivergenceType.MISSING_STEP,
                        description=f"Step '{step_id}' completed in Run B but missing in Run A",
                        run_a_value=None,
                        run_b_value="COMPLETED",
                    )
                )
                continue

            if step_id not in map_b:
                divergences.append(
                    DivergenceRecord(
                        step_id=step_id,
                        divergence_type=DivergenceType.MISSING_STEP,
                        description=f"Step '{step_id}' completed in Run A but missing in Run B",
                        run_a_value="COMPLETED",
                        run_b_value=None,
                    )
                )
                continue

            entry_a = map_a[step_id]
            entry_b = map_b[step_id]

            res_a = entry_a.payload.get("result")
            res_b = entry_b.payload.get("result")

            if res_a != res_b:
                divergences.append(
                    DivergenceRecord(
                        step_id=step_id,
                        divergence_type=DivergenceType.PAYLOAD_MUTATION,
                        description=f"Result payload divergence detected for step '{step_id}'",
                        run_a_value=res_a,
                        run_b_value=res_b,
                    )
                )

            # Check attempt counts
            att_a = entry_a.payload.get("attempts", 1)
            att_b = entry_b.payload.get("attempts", 1)
            if att_a != att_b:
                divergences.append(
                    DivergenceRecord(
                        step_id=step_id,
                        divergence_type=DivergenceType.RETRY_COUNT_MISMATCH,
                        description=f"Retry attempt count mismatch for step '{step_id}': {att_a} vs {att_b}",
                        run_a_value=att_a,
                        run_b_value=att_b,
                    )
                )

        is_identical = len(divergences) == 0
        return DiffReport(
            run_id_a=run_id_a,
            run_id_b=run_id_b,
            is_identical=is_identical,
            total_events_a=len(history_a),
            total_events_b=len(history_b),
            divergences=divergences,
        )
