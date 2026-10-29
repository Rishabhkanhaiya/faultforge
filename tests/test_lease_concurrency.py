"""Tests for distributed leasing, heartbeats, optimistic locking, and zombie reclamation."""

import pytest
from faultforge.engine.lease import (
    LeaseAcquisitionError,
    LeaseExpiredError,
    LeaseManager,
    LeaseVersionConflictError,
)


def test_lease_acquisition_and_expiry() -> None:
    mgr = LeaseManager()
    lease = mgr.acquire_lease("step_charge", "worker_1", ttl_seconds=5.0, now=100.0)

    assert lease.step_id == "step_charge"
    assert lease.owner_id == "worker_1"
    assert lease.version == 1
    assert lease.heartbeat_count == 0
    assert not lease.is_expired(now=102.0)
    assert lease.is_expired(now=106.0)
    assert lease.time_remaining(now=102.0) == 3.0


def test_lease_collision_by_competing_worker() -> None:
    mgr = LeaseManager()
    mgr.acquire_lease("step_charge", "worker_alpha", ttl_seconds=10.0, now=100.0)

    # Competing worker attempts acquisition while lease is unexpired
    with pytest.raises(LeaseAcquisitionError) as exc_info:
        mgr.acquire_lease("step_charge", "worker_beta", ttl_seconds=10.0, now=104.0)

    assert "actively locked by worker 'worker_alpha'" in str(exc_info.value)


def test_optimistic_version_conflict_on_renew() -> None:
    mgr = LeaseManager()
    lease = mgr.acquire_lease("step_audit", "worker_alpha", ttl_seconds=10.0, now=100.0)

    # Successfully renew once
    renewed = mgr.renew_lease(
        step_id="step_audit",
        owner_id="worker_alpha",
        lease_token=lease.lease_token,
        expected_version=1,
        ttl_seconds=10.0,
        now=102.0,
    )
    assert renewed.version == 2
    assert renewed.heartbeat_count == 1

    # Second renewal with stale version 1 must trigger conflict
    with pytest.raises(LeaseVersionConflictError):
        mgr.renew_lease(
            step_id="step_audit",
            owner_id="worker_alpha",
            lease_token=lease.lease_token,
            expected_version=1,
            ttl_seconds=10.0,
            now=103.0,
        )


def test_zombie_lease_reclamation_and_takeover() -> None:
    mgr = LeaseManager()
    # Worker 1 acquires lease with 5s TTL
    mgr.acquire_lease("step_ship", "crashed_worker", ttl_seconds=5.0, now=100.0)

    # Time advances 10s: lease is now expired (zombied)
    now = 110.0
    stolen_lease = mgr.acquire_lease("step_ship", "healthy_worker", ttl_seconds=10.0, now=now)

    assert stolen_lease.owner_id == "healthy_worker"
    assert stolen_lease.version == 2  # Incremented from zombie version 1
    assert not stolen_lease.is_expired(now=now)


def test_release_lease_allows_immediate_reacquisition() -> None:
    mgr = LeaseManager()
    lease = mgr.acquire_lease("step_cleanup", "worker_1", ttl_seconds=30.0, now=100.0)

    released = mgr.release_lease("step_cleanup", "worker_1", lease.lease_token)
    assert released is True

    # Another worker can acquire immediately
    reacquired = mgr.acquire_lease("step_cleanup", "worker_2", ttl_seconds=10.0, now=101.0)
    assert reacquired.owner_id == "worker_2"
    assert reacquired.version == 1
