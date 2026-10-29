import random
from dataclasses import dataclass, field
from typing import List


class InjectedFaultError(RuntimeError):
    """Simulated transient infrastructure or network failure injected by FaultForge."""


@dataclass
class ChaosPolicy:
    seed: int = 42
    failure_probability: float = 0.0


@dataclass
class ChaosInjector:
    """Deterministic fault injector using a seeded PRNG for reproducible chaos testing."""

    policy: ChaosPolicy
    injected_faults: int = 0
    fault_log: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not (0.0 <= self.policy.failure_probability <= 1.0):
            raise ValueError("failure_probability must be between 0.0 and 1.0")
        self._rng = random.Random(self.policy.seed)

    def maybe_inject_fault(self, step_id: str, attempt: int) -> None:
        if self.policy.failure_probability <= 0.0:
            return
        roll = self._rng.random()
        if roll < self.policy.failure_probability:
            self.injected_faults += 1
            msg = f"Seed={self.policy.seed} fault injected at step='{step_id}' (attempt={attempt}, roll={roll:.3f})"
            self.fault_log.append(msg)
            raise InjectedFaultError(msg)
