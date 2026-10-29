"""Tests for exponential backoff, Merkle trees, and snapshot drift verification."""

import pytest
from faultforge.engine.checkpointer import (
    MerkleTree,
    StateCheckpointer,
    StateDriftError,
)
from faultforge.engine.retry import (
    BackoffStrategy,
    DeadLetterQueue,
    RetryPolicy,
)


def test_exponential_backoff_jitter_bounds() -> None:
    policy = RetryPolicy(
        max_retries=5,
        initial_delay_sec=0.2,
        max_delay_sec=5.0,
        backoff_multiplier=2.0,
        strategy=BackoffStrategy.FULL_JITTER,
        seed=42,
    )

    for attempt in range(1, 6):
        delay = policy.compute_backoff(attempt)
        assert 0.0 <= delay <= 5.0

    # Non-retryable check
    class NonRetryableFatal(Exception):
        pass

    policy.non_retryable_exceptions = (NonRetryableFatal,)
    assert policy.should_retry(RuntimeError("transient"), attempt=1) is True
    assert policy.should_retry(NonRetryableFatal("fatal"), attempt=1) is False
    assert policy.should_retry(RuntimeError("transient"), attempt=6) is False


def test_merkle_tree_consistency() -> None:
    state_a = {"user_id": "u123", "balance": 450, "active": True}
    state_b = {"user_id": "u123", "balance": 450, "active": True}
    state_c = {"user_id": "u123", "balance": 999, "active": True}

    tree_a = MerkleTree(state_a)
    tree_b = MerkleTree(state_b)
    tree_c = MerkleTree(state_c)

    assert tree_a.root_hash == tree_b.root_hash
    assert tree_a.root_hash != tree_c.root_hash


def test_checkpoint_snapshot_creation_and_restoration() -> None:
    checkpointer = StateCheckpointer()
    state = {"step_a": {"result": 10}, "step_b": {"result": 20}}

    snapshot = checkpointer.create_snapshot("run_check_01", wal_seq_watermark=15, current_state=state)
    assert snapshot.run_id == "run_check_01"
    assert snapshot.wal_seq_watermark == 15
    assert len(snapshot.merkle_root) == 64

    retrieved = checkpointer.get_latest_snapshot("run_check_01")
    assert retrieved is not None
    assert retrieved.merkle_root == snapshot.merkle_root
    assert retrieved.compacted_state == state


def test_state_drift_detection_raises_error() -> None:
    checkpointer = StateCheckpointer()
    valid_state = {"order_id": "ord_99", "total": 500}
    checkpointer.create_snapshot("run_drift_test", wal_seq_watermark=5, current_state=valid_state)

    # Identical state verifies cleanly
    assert checkpointer.verify_state_drift("run_drift_test", valid_state) is True

    # Mutated state triggers StateDriftError
    mutated_state = {"order_id": "ord_99", "total": 9999}
    with pytest.raises(StateDriftError) as exc_info:
        checkpointer.verify_state_drift("run_drift_test", mutated_state)

    assert "State drift detected" in str(exc_info.value)
