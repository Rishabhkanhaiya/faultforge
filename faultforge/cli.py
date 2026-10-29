import argparse
import json
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import WriteAheadLog


def build_demo_workflow() -> WorkflowDAG:
    dag = WorkflowDAG("distributed_order_settlement")
    dag.add_step("reserve_inventory", lambda ctx: {"reserved": True, "sku": "GPU-NODE-01"}, max_retries=5)
    dag.add_step(
        "authorize_payment",
        lambda ctx: {"auth_id": "auth_9981", "amount_usd": 450},
        depends_on=["reserve_inventory"],
        max_retries=5,
    )
    dag.add_step(
        "commit_ledger",
        lambda ctx: {"settled": True, "tx_ref": ctx["authorize_payment"]["auth_id"]},
        depends_on=["authorize_payment"],
        max_retries=5,
    )
    return dag


def main() -> None:
    parser = argparse.ArgumentParser(description="FaultForge Deterministic Chaos CLI Runner")
    parser.add_argument("--seed", type=int, default=42, help="PRNG seed for deterministic fault injection")
    parser.add_argument("--fail-rate", type=float, default=0.35, help="Fault probability per step (0.0 - 1.0)")
    parser.add_argument("--run-id", type=str, default="cli_run_001", help="Workflow execution run ID")
    args = parser.parse_args()

    wal = WriteAheadLog()
    chaos = ChaosInjector(ChaosPolicy(seed=args.seed, failure_probability=args.fail_rate))
    runtime = DurableRuntime(wal=wal, chaos=chaos)
    dag = build_demo_workflow()

    result = runtime.execute(args.run_id, dag)
    history = wal.get_history(args.run_id)

    summary = {
        "run_id": args.run_id,
        "seed": args.seed,
        "fail_rate": args.fail_rate,
        "injected_faults": chaos.injected_faults,
        "wal_events_recorded": len(history),
        "final_output": result,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
