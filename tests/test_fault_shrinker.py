"""Tests for Delta-Debugging fault schedule minimization."""

from typing import List
from faultforge.chaos.shrinker import FaultShrinker, FaultStep


def test_shrink_15_faults_to_single_critical_fault() -> None:
    # Build schedule with 15 candidate faults
    all_faults: List[FaultStep] = [
        FaultStep(step_id=f"step_{i}", attempt=1, fault_type="transient_network_error")
        for i in range(15)
    ]

    # Target invariant: failure is only triggered if step_7 is included
    def failure_predicate(subset: List[FaultStep]) -> bool:
        return any(f.step_id == "step_7" for f in subset)

    shrinker = FaultShrinker(failure_predicate)
    result = shrinker.shrink(all_faults)

    assert result.original_fault_count == 15
    assert result.minimal_fault_count == 1
    assert len(result.minimal_schedule) == 1
    assert result.minimal_schedule[0].step_id == "step_7"
    assert result.reduction_percentage > 90.0
    assert result.evaluation_runs < 20  # Delta debugging efficiency check


def test_shrink_to_pair_of_interacting_faults() -> None:
    # 8 candidate faults where failure requires both step_2 AND step_5 simultaneously
    all_faults: List[FaultStep] = [
        FaultStep(step_id=f"step_{i}", attempt=1, fault_type="timeout")
        for i in range(8)
    ]

    def failure_predicate(subset: List[FaultStep]) -> bool:
        has_2 = any(f.step_id == "step_2" for f in subset)
        has_5 = any(f.step_id == "step_5" for f in subset)
        return has_2 and has_5

    shrinker = FaultShrinker(failure_predicate)
    result = shrinker.shrink(all_faults)

    assert result.original_fault_count == 8
    assert result.minimal_fault_count == 2
    steps = {f.step_id for f in result.minimal_schedule}
    assert steps == {"step_2", "step_5"}


def test_shrink_already_minimal() -> None:
    single = [FaultStep(step_id="only_step", attempt=1)]
    shrinker = FaultShrinker(lambda s: True)
    result = shrinker.shrink(single)

    assert result.minimal_fault_count == 1
    assert result.minimal_schedule == single


def test_shrink_non_reproducing_baseline() -> None:
    faults = [FaultStep(step_id="step_a", attempt=1), FaultStep(step_id="step_b", attempt=1)]
    # Predicate always returns False (bug does not reproduce)
    shrinker = FaultShrinker(lambda s: False)
    result = shrinker.shrink(faults)

    assert result.minimal_fault_count == 2
    assert result.reduction_percentage == 0.0
