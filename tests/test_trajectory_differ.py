"""Tests for trajectory differ and non-deterministic state detection."""

from faultforge.engine.wal import EventType, WriteAheadLog
from faultforge.telemetry.differ import DivergenceType, TrajectoryDiffer


def test_identical_trajectories_report_zero_divergence() -> None:
    wal = WriteAheadLog()

    for run_id in ("run_1", "run_2"):
        wal.append(run_id, "__workflow__", EventType.WORKFLOW_STARTED, {"workflow": "order"})
        wal.append(run_id, "step_1", EventType.STEP_STARTED, {"attempt": 1})
        wal.append(run_id, "step_1", EventType.STEP_COMPLETED, {"result": {"sku": "A1"}, "attempts": 1})
        wal.append(run_id, "__workflow__", EventType.WORKFLOW_COMPLETED, {"status": "SUCCESS"})

    differ = TrajectoryDiffer(wal)
    report = differ.diff_runs("run_1", "run_2")

    assert report.is_identical is True
    assert len(report.divergences) == 0


def test_detect_payload_mutation() -> None:
    wal = WriteAheadLog()

    # Run 1 produces price=100
    wal.append("run_1", "pricing", EventType.STEP_COMPLETED, {"result": {"price": 100}, "attempts": 1})

    # Run 2 produces price=120 (non-deterministic mutation)
    wal.append("run_2", "pricing", EventType.STEP_COMPLETED, {"result": {"price": 120}, "attempts": 1})

    differ = TrajectoryDiffer(wal)
    report = differ.diff_runs("run_1", "run_2")

    assert report.is_identical is False
    types = [d.divergence_type for d in report.divergences]
    assert DivergenceType.PAYLOAD_MUTATION in types


def test_detect_missing_step() -> None:
    wal = WriteAheadLog()

    wal.append("run_a", "step_init", EventType.STEP_COMPLETED, {"result": 1})
    wal.append("run_a", "step_notify", EventType.STEP_COMPLETED, {"result": 2})

    wal.append("run_b", "step_init", EventType.STEP_COMPLETED, {"result": 1})
    # step_notify missing in run_b

    differ = TrajectoryDiffer(wal)
    report = differ.diff_runs("run_a", "run_b")

    assert report.is_identical is False
    missing = [d for d in report.divergences if d.divergence_type == DivergenceType.MISSING_STEP]
    assert len(missing) == 1
    assert missing[0].step_id == "step_notify"


def test_detect_retry_count_mismatch() -> None:
    wal = WriteAheadLog()

    wal.append("run_1", "step_load", EventType.STEP_COMPLETED, {"result": "ok", "attempts": 1})
    wal.append("run_2", "step_load", EventType.STEP_COMPLETED, {"result": "ok", "attempts": 3})

    differ = TrajectoryDiffer(wal)
    report = differ.diff_runs("run_1", "run_2")

    assert report.is_identical is False
    types = [d.divergence_type for d in report.divergences]
    assert DivergenceType.RETRY_COUNT_MISMATCH in types
