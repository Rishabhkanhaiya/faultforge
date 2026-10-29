"""Tests for deterministic network simulator, latency models, and corruption."""

import pytest
from faultforge.chaos.network_sim import (
    LatencyDistribution,
    NetworkChaosConfig,
    NetworkChaosSimulator,
    SimulatedPacketDropError,
    SimulatedTimeoutError,
)


def test_deterministic_latency_distributions() -> None:
    for dist in (LatencyDistribution.UNIFORM, LatencyDistribution.LOG_NORMAL, LatencyDistribution.PARETO):
        config = NetworkChaosConfig(
            seed=42,
            base_latency_ms=50.0,
            latency_jitter_ms=25.0,
            distribution=dist,
        )
        sim = NetworkChaosSimulator(config)
        lat = sim.compute_simulated_latency()
        assert lat >= 50.0
        assert sim.simulated_elapsed_ms == lat


def test_packet_drop_and_timeout_exceptions() -> None:
    # Always drop
    drop_cfg = NetworkChaosConfig(seed=1, drop_probability=1.0)
    sim_drop = NetworkChaosSimulator(drop_cfg)
    with pytest.raises(SimulatedPacketDropError):
        sim_drop.evaluate_transit("op_payment")

    # Always timeout
    timeout_cfg = NetworkChaosConfig(seed=1, timeout_probability=1.0)
    sim_timeout = NetworkChaosSimulator(timeout_cfg)
    with pytest.raises(SimulatedTimeoutError):
        sim_timeout.evaluate_transit("op_ledger")


def test_payload_corruption() -> None:
    config = NetworkChaosConfig(seed=7, corruption_probability=1.0)
    sim = NetworkChaosSimulator(config)
    original = {"token": "secure_session_token_xyz", "amount": 100}

    corrupted = sim.maybe_corrupt_payload(original)
    assert corrupted != original
    assert sim.corrupted_payloads == 1


def test_clock_skew_simulation() -> None:
    config = NetworkChaosConfig(clock_skew_sec=120.0)
    sim = NetworkChaosSimulator(config)
    skewed = sim.get_skewed_time(base_time=1000.0)
    assert skewed == 1120.0
