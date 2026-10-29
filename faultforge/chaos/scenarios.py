"""Pre-built chaos stress scenarios for distributed resilience evaluation."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy, InjectedFaultError
from faultforge.chaos.network_sim import (
    LatencyDistribution,
    NetworkChaosConfig,
    NetworkChaosSimulator,
    SimulatedTimeoutError,
)
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.lease import LeaseAcquisitionError, LeaseManager
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import EventType, WriteAheadLog


def build_split_brain_partition_scenario(
    seed: int = 101,
) -> Tuple[WorkflowDAG, LeaseManager, NetworkChaosSimulator]:
    """
    Scenario 1: Split-Brain Network Partition.
    Simulates two workers contending for step locks where worker 1 is severed
    from the coordinator and its leases become zombied.
    """
    dag = WorkflowDAG("split_brain_partition_dag")
    dag.add_step(
        "partition_leader_election",
        lambda ctx: {"leader": ctx.get("preferred_node", "worker-node-1")},
        max_retries=3,
    )
    dag.add_step(
        "sync_distributed_state",
        lambda ctx: {"synced_epoch": 42, "replica": ctx["partition_leader_election"]["leader"]},
        depends_on=["partition_leader_election"],
        max_retries=3,
    )

    lease_mgr = LeaseManager()
    net_config = NetworkChaosConfig(
        seed=seed,
        drop_probability=0.5,
        timeout_probability=0.2,
        distribution=LatencyDistribution.LOG_NORMAL,
    )
    net_sim = NetworkChaosSimulator(net_config)

    return dag, lease_mgr, net_sim


def build_thundering_herd_scenario(
    seed: int = 202,
) -> Tuple[WorkflowDAG, ChaosInjector]:
    """
    Scenario 2: Thundering Herd Retry.
    High-concurrency downstream step with rapid transient faults, testing
    jittered backoff to prevent synchronized worker retry storms.
    """
    dag = WorkflowDAG("thundering_herd_dag")
    dag.add_step("fanout_trigger", lambda ctx: {"event_count": 100}, max_retries=2)
    dag.add_step(
        "bottleneck_service",
        lambda ctx: {"processed": ctx["fanout_trigger"]["event_count"] * 2},
        depends_on=["fanout_trigger"],
        max_retries=10,
    )
    dag.add_step(
        "sink_aggregator",
        lambda ctx: {"completed": True, "total": ctx["bottleneck_service"]["processed"]},
        depends_on=["bottleneck_service"],
        max_retries=3,
    )

    chaos = ChaosInjector(ChaosPolicy(seed=seed, failure_probability=0.4))
    return dag, chaos


def build_cascading_timeout_scenario(
    seed: int = 303,
) -> Tuple[WorkflowDAG, NetworkChaosSimulator]:
    """
    Scenario 3: Cascading Downstream Timeout.
    Upstream step delays accumulate along a critical DAG path, triggering
    timeout thresholds in downstream dependencies.
    """
    dag = WorkflowDAG("cascading_timeout_dag")
    dag.add_step("fetch_upstream_catalog", lambda ctx: {"catalog_size": 500}, max_retries=4)
    dag.add_step(
        "pricing_engine",
        lambda ctx: {"prices": [19.99, 49.99, 99.99]},
        depends_on=["fetch_upstream_catalog"],
        max_retries=4,
    )
    dag.add_step(
        "apply_dynamic_discounts",
        lambda ctx: {"discounted_sum": sum(ctx["pricing_engine"]["prices"]) * 0.9},
        depends_on=["pricing_engine"],
        max_retries=4,
    )

    net_config = NetworkChaosConfig(
        seed=seed,
        base_latency_ms=100.0,
        latency_jitter_ms=150.0,
        timeout_probability=0.25,
        distribution=LatencyDistribution.PARETO,
    )
    net_sim = NetworkChaosSimulator(net_config)
    return dag, net_sim


def build_corrupted_wal_tail_scenario(
    run_id: str = "corrupted_tail_001",
) -> Tuple[WriteAheadLog, WorkflowDAG]:
    """
    Scenario 4: Corrupted WAL Tail Recovery.
    Simulates a worker crash during an append resulting in malformed JSON or
    incomplete step records at the tail of the WAL.
    """
    wal = WriteAheadLog()
    wal.append(run_id, "__workflow__", EventType.WORKFLOW_STARTED, {"workflow": "wal_tail_test"})
    wal.append(run_id, "step_1", EventType.STEP_STARTED, {"attempt": 1}, attempt=1)
    wal.append(
        run_id,
        "step_1",
        EventType.STEP_COMPLETED,
        {"result": {"val": 100}, "attempts": 1},
        attempt=1,
    )
    # Simulate uncompleted or interrupted step_2
    wal.append(run_id, "step_2", EventType.STEP_STARTED, {"attempt": 1}, attempt=1)

    dag = WorkflowDAG("corrupted_tail_dag")
    dag.add_step("step_1", lambda ctx: {"val": 100}, max_retries=2)
    dag.add_step("step_2", lambda ctx: {"val": ctx["step_1"]["val"] * 5}, depends_on=["step_1"], max_retries=2)

    return wal, dag
