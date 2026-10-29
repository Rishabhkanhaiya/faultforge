"""Delta-debugging fault shrinker to discover 1-minimal failure schedules."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence


@dataclass(frozen=True)
class FaultStep:
    """Individual fault injection decision point in an execution schedule."""

    step_id: str
    attempt: int
    fault_type: str = "transient_error"
    error_message: Optional[str] = None

    def key(self) -> str:
        """Unique identifier for this specific fault occurrence."""
        return f"{self.step_id}#att{self.attempt}:{self.fault_type}"


@dataclass
class FaultSchedule:
    """Ordered sequence of fault injection events during workflow execution."""

    faults: List[FaultStep] = field(default_factory=list)

    def contains(self, step_id: str, attempt: int) -> bool:
        """Check if this schedule dictates injecting a fault for the given step and attempt."""
        for f in self.faults:
            if f.step_id == step_id and f.attempt == attempt:
                return True
        return False

    def get_fault(self, step_id: str, attempt: int) -> Optional[FaultStep]:
        """Fetch matching fault step if present."""
        for f in self.faults:
            if f.step_id == step_id and f.attempt == attempt:
                return f
        return None

    def __len__(self) -> int:
        return len(self.faults)


@dataclass(frozen=True)
class ShrinkResult:
    """Outcome of Delta-Debugging minimization."""

    original_fault_count: int
    minimal_fault_count: int
    minimal_schedule: List[FaultStep]
    evaluation_runs: int
    elapsed_seconds: float

    @property
    def reduction_percentage(self) -> float:
        """Percentage of faults eliminated during minimization."""
        if self.original_fault_count == 0:
            return 0.0
        reduced = self.original_fault_count - self.minimal_fault_count
        return (reduced / self.original_fault_count) * 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize shrink results to dictionary."""
        return {
            "original_fault_count": self.original_fault_count,
            "minimal_fault_count": self.minimal_fault_count,
            "reduction_percentage": round(self.reduction_percentage, 2),
            "evaluation_runs": self.evaluation_runs,
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "minimal_schedule": [
                {
                    "step_id": f.step_id,
                    "attempt": f.attempt,
                    "fault_type": f.fault_type,
                    "error_message": f.error_message,
                }
                for f in self.minimal_schedule
            ],
        }


class FaultShrinker:
    """Minimizes failing fault traces to the 1-minimal subset using Delta Debugging (ddmin)."""

    def __init__(self, failure_predicate: Callable[[List[FaultStep]], bool]) -> None:
        """
        Initialize shrinker.
        :param failure_predicate: Callable that takes a candidate list of FaultSteps,
                                  executes the workflow/simulation, and returns True
                                  if the target failure reproduced, False otherwise.
        """
        self.failure_predicate = failure_predicate
        self.evaluations: int = 0

    def _eval(self, candidate: List[FaultStep]) -> bool:
        self.evaluations += 1
        return self.failure_predicate(candidate)

    def shrink(self, initial_faults: Sequence[FaultStep]) -> ShrinkResult:
        """
        Execute Zeller's ddmin algorithm to find the 1-minimal fault sequence.
        Guarantees that removing any single fault from the returned schedule
        causes the workflow to succeed (or no longer produce the target failure).
        """
        start_time = time.time()
        self.evaluations = 0

        current: List[FaultStep] = list(initial_faults)
        n = len(current)

        if n <= 1:
            # Already minimal or empty
            return ShrinkResult(
                original_fault_count=n,
                minimal_fault_count=n,
                minimal_schedule=current,
                evaluation_runs=1 if n == 1 else 0,
                elapsed_seconds=time.time() - start_time,
            )

        # Baseline check: ensure full schedule actually reproduces the failure
        if not self._eval(current):
            # Baseline does not reproduce failure
            return ShrinkResult(
                original_fault_count=n,
                minimal_fault_count=n,
                minimal_schedule=current,
                evaluation_runs=self.evaluations,
                elapsed_seconds=time.time() - start_time,
            )

        k = 2
        while len(current) >= 2:
            subsets: List[List[FaultStep]] = []
            chunk_size = max(1, len(current) // k)
            remainder = len(current) % k

            idx = 0
            for i in range(k):
                current_chunk = chunk_size + (1 if i < remainder else 0)
                subsets.append(current[idx : idx + current_chunk])
                idx += current_chunk

            reduced = False

            # Phase 1: Test individual chunks C_i
            for subset in subsets:
                if subset and self._eval(subset):
                    current = subset
                    k = 2
                    reduced = True
                    break

            if reduced:
                continue

            # Phase 2: Test complements C \ C_i
            for subset in subsets:
                complement = [item for item in current if item not in subset]
                if complement and self._eval(complement):
                    current = complement
                    k = max(k - 1, 2)
                    reduced = True
                    break

            if reduced:
                continue

            # Phase 3: Increase granularity
            if k < len(current):
                k = min(len(current), 2 * k)
            else:
                # 1-minimal reached!
                break

        return ShrinkResult(
            original_fault_count=n,
            minimal_fault_count=len(current),
            minimal_schedule=current,
            evaluation_runs=self.evaluations,
            elapsed_seconds=time.time() - start_time,
        )
