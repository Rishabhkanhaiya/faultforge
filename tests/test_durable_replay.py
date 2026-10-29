import pytest
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import EventType, WriteAheadLog


def test_deterministic_replay_skips_completed_side_effects() -> None:
    wal = WriteAheadLog()
    state = {"card_charges": 0, "crash_second_step": True}

    def charge_payment(ctx):
        state["card_charges"] += 1
        return {"Receipt": "rcpt_1001"}

    def dispatch_shipment(ctx):
        if state["crash_second_step"]:
            raise RuntimeError("Worker node crashed before shipment ACK")
        return {"shipped": True}

    dag = WorkflowDAG("checkout_flow")
    dag.add_step("charge", charge_payment, max_retries=0)
    dag.add_step("ship", dispatch_shipment, depends_on=["charge"], max_retries=0)

    runtime = DurableRuntime(wal=wal)

    with pytest.raises(RuntimeError):
        runtime.execute("order_77", dag)

    assert state["card_charges"] == 1

    # Recover workflow from WAL: 'charge' must NOT execute a second time
    state["crash_second_step"] = False
    final_ctx = runtime.execute("order_77", dag)

    assert state["card_charges"] == 1
    assert final_ctx["charge"] == {"Receipt": "rcpt_1001"}
    assert final_ctx["ship"] == {"shipped": True}


def test_chaos_injection_recovers_deterministically() -> None:
    wal = WriteAheadLog()
    chaos = ChaosInjector(ChaosPolicy(seed=19, failure_probability=0.55))

    dag = WorkflowDAG("chaos_pipeline")
    dag.add_step("ingest", lambda ctx: 10, max_retries=8)
    dag.add_step("transform", lambda ctx: ctx["ingest"] * 3, depends_on=["ingest"], max_retries=8)
    dag.add_step("load", lambda ctx: ctx["transform"] + 7, depends_on=["transform"], max_retries=8)

    runtime = DurableRuntime(wal=wal, chaos=chaos)
    ctx = runtime.execute("chaos_run_19", dag)

    assert ctx["load"] == 37
    assert chaos.injected_faults >= 1

    history = wal.get_history("chaos_run_19")
    failed_events = [e for e in history if e.event_type == EventType.STEP_FAILED]
    assert len(failed_events) == chaos.injected_faults
