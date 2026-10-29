import pytest
from faultforge.engine.dag import CycleError, WorkflowDAG


def test_diamond_dependency_topological_order() -> None:
    dag = WorkflowDAG("diamond_graph")
    dag.add_step("init", lambda ctx: 1)
    dag.add_step("branch_a", lambda ctx: 2, depends_on=["init"])
    dag.add_step("branch_b", lambda ctx: 3, depends_on=["init"])
    dag.add_step("join", lambda ctx: 4, depends_on=["branch_a", "branch_b"])

    order = [node.step_id for node in dag.topological_order()]
    assert order[0] == "init"
    assert order[-1] == "join"
    assert set(order[1:3]) == {"branch_a", "branch_b"}


def test_cycle_detection_raises_error() -> None:
    dag = WorkflowDAG("cyclic_graph")
    dag.add_step("step_1", lambda ctx: 1, depends_on=["step_3"])
    dag.add_step("step_2", lambda ctx: 2, depends_on=["step_1"])
    dag.add_step("step_3", lambda ctx: 3, depends_on=["step_2"])

    with pytest.raises(CycleError):
        dag.topological_order()


def test_missing_dependency_raises_key_error() -> None:
    dag = WorkflowDAG("invalid_deps")
    dag.add_step("orphan", lambda ctx: 1, depends_on=["non_existent"])

    with pytest.raises(KeyError):
        dag.topological_order()
