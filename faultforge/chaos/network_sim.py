"""Deterministic network, latency jitter, and resource fault simulator."""

from __future__ import annotations

import copy
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional
from faultforge.chaos.injector import InjectedFaultError


class LatencyDistribution(str, Enum):
    CONSTANT = "CONSTANT"
    UNIFORM = "UNIFORM"
    LOG_NORMAL = "LOG_NORMAL"
    PARETO = "PARETO"


class SimulatedNetworkError(InjectedFaultError):
    """Base exception for simulated network anomalies."""


class SimulatedTimeoutError(SimulatedNetworkError):
    """Simulated network connection timeout."""


class SimulatedPacketDropError(SimulatedNetworkError):
    """Simulated transport-layer packet drop."""


class SimulatedCorruptionError(SimulatedNetworkError):
    """Simulated payload corruption / checksum mismatch."""


@dataclass
class NetworkChaosConfig:
    """Configuration parameters for network chaos injection."""

    seed: int = 42
    drop_probability: float = 0.0
    timeout_probability: float = 0.0
    corruption_probability: float = 0.0
    base_latency_ms: float = 0.0
    latency_jitter_ms: float = 0.0
    distribution: LatencyDistribution = LatencyDistribution.UNIFORM
    clock_skew_sec: float = 0.0


class NetworkChaosSimulator:
    """Simulates realistic distributed network conditions using deterministic pseudo-randomness."""

    def __init__(self, config: Optional[NetworkChaosConfig] = None) -> None:
        self.config = config or NetworkChaosConfig()
        self._rng = random.Random(self.config.seed)
        self.dropped_packets: int = 0
        self.timeouts_injected: int = 0
        self.corrupted_payloads: int = 0
        self.simulated_elapsed_ms: float = 0.0

    def compute_simulated_latency(self) -> float:
        """Calculate simulated latency delay in milliseconds based on configured distribution."""
        base = self.config.base_latency_ms
        jitter = self.config.latency_jitter_ms
        if jitter <= 0.0:
            return max(0.0, base)

        if self.config.distribution == LatencyDistribution.CONSTANT:
            delay = base
        elif self.config.distribution == LatencyDistribution.UNIFORM:
            delay = base + self._rng.uniform(0.0, jitter)
        elif self.config.distribution == LatencyDistribution.LOG_NORMAL:
            # Shape parameters producing long-tail distribution
            mu = 0.0
            sigma = 0.5
            delay = base + (self._rng.lognormvariate(mu, sigma) * (jitter / 2.0))
        elif self.config.distribution == LatencyDistribution.PARETO:
            alpha = 3.0  # Pareto shape parameter
            delay = base + ((self._rng.paretovariate(alpha) - 1.0) * jitter)
        else:
            delay = base

        clamped = max(0.0, delay)
        self.simulated_elapsed_ms += clamped
        return clamped

    def evaluate_transit(self, operation_id: str) -> float:
        """Evaluate network transit for an operation, potentially throwing drop or timeout."""
        # 1. Check packet drop
        if self.config.drop_probability > 0.0:
            if self._rng.random() < self.config.drop_probability:
                self.dropped_packets += 1
                raise SimulatedPacketDropError(
                    f"Simulated network packet drop during operation '{operation_id}'"
                )

        # 2. Check timeout
        if self.config.timeout_probability > 0.0:
            if self._rng.random() < self.config.timeout_probability:
                self.timeouts_injected += 1
                raise SimulatedTimeoutError(
                    f"Simulated socket timeout (>30000ms) waiting for '{operation_id}'"
                )

        return self.compute_simulated_latency()

    def maybe_corrupt_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Optionally flip bits or mutate values to simulate payload corruption in transit."""
        if self.config.corruption_probability <= 0.0:
            return payload

        if self._rng.random() >= self.config.corruption_probability:
            return payload

        self.corrupted_payloads += 1
        mutated = copy.deepcopy(payload)

        if not mutated:
            mutated["__corrupted_flag__"] = True
            return mutated

        # Corrupt a randomly selected key's value
        target_key = self._rng.choice(list(mutated.keys()))
        val = mutated[target_key]

        if isinstance(val, str):
            if val:
                chars = list(val)
                idx = self._rng.randint(0, len(chars) - 1)
                chars[idx] = chr(ord(chars[idx]) ^ 0x07)
                mutated[target_key] = "".join(chars)
            else:
                mutated[target_key] = "\x00CORRUPT"
        elif isinstance(val, (int, float)):
            mutated[target_key] = type(val)(-val - 999)
        elif isinstance(val, bool):
            mutated[target_key] = not val
        else:
            mutated[target_key] = None

        return mutated

    def get_skewed_time(self, base_time: Optional[float] = None) -> float:
        """Return timestamp adjusted by simulated clock drift."""
        current = base_time if base_time is not None else time.time()
        return current + self.config.clock_skew_sec
