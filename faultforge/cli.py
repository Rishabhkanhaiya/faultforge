"""FaultForge CLI: Multi-command runner, WAL inspection, delta shrinking, and trajectory diffing."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy, InjectedFaultError
from faultforge.chaos.shrinker import FaultShrinker, FaultStep
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import EventType, WriteAheadLog
from faultforge.telemetry.differ import TrajectoryDiffer
from faultforge.telemetry.tracer import TimelineTracer


def build_demo_workflow() -> WorkflowDAG:
    """Standard demo workflow representing a distributed order settlement flow."""
    dag = WorkflowDAG("distributed_order_settlement")
    dag.add_step(
        "reserve_inventory",
        lambda ctx: {"reserved": True, "sku": "GPU-NODE-01"},
        max_retries=5,
    )
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


def cmd_run(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Execute workflow with seeded chaos injection."""
    chaos = ChaosInjector(ChaosPolicy(seed=args.seed, failure_probability=args.fail_rate))
    runtime = DurableRuntime(wal=wal, chaos=chaos)
    dag = build_demo_workflow()

    try:
        result = runtime.execute(args.run_id, dag)
    except Exception as exc:
        print(f"Error: Workflow failed after exhausting retries: {exc}", file=sys.stderr)
        return 1

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
    return 0


def cmd_replay(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Replay an existing run from WAL, verifying zero duplicate side effects."""
    dag = build_demo_workflow()
    runtime = DurableRuntime(wal=wal)
    try:
        result = runtime.execute(args.run_id, dag)
        print(json.dumps({"replayed_run_id": args.run_id, "status": "SUCCESS", "output": result}, indent=2))
        return 0
    except Exception as exc:
        print(f"Error during replay: {exc}", file=sys.stderr)
        return 1


def cmd_inspect_wal(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Display tabular WAL event records for a specific run ID."""
    history = wal.get_history(args.run_id)
    if not history:
        print(f"No WAL records found for run '{args.run_id}'.", file=sys.stderr)
        return 1

    header = f"{'SEQ':<6} | {'STEP_ID':<22} | {'EVENT_TYPE':<20} | {'IDEMPOTENCY_KEY':<16} | {'PAYLOAD'}"
    separator = "-" * len(header)
    print(separator)
    print(header)
    print(separator)
    for entry in history:
        payload_str = json.dumps(entry.payload)
        if len(payload_str) > 40:
            payload_str = payload_str[:37] + "..."
        print(
            f"{entry.seq_id:<6} | {entry.step_id:<22} | {entry.event_type.value:<20} | {entry.idempotency_key[:16]:<16} | {payload_str}"
        )
    print(separator)
    return 0


def cmd_diff(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Compare two execution runs and report any non-deterministic state divergence."""
    differ = TrajectoryDiffer(wal)
    report = differ.diff_runs(args.run_a, args.run_b)
    print(json.dumps(report.to_dict(), indent=2))
    return 0 if report.is_identical else 2


def cmd_export_dag(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Export Mermaid DAG visualization or OpenTelemetry trace."""
    dag = build_demo_workflow()
    history = wal.get_history(args.run_id) if args.run_id else None
    tracer = TimelineTracer()

    if args.format == "mermaid":
        print(tracer.generate_mermaid_diagram(dag, history))
    else:
        print(tracer.to_otel_json())
    return 0


def cmd_shrink(args: argparse.Namespace, wal: WriteAheadLog) -> int:
    """Execute Delta-Debugging to minimize a failing chaos fault schedule."""
    # Build candidate fault schedule
    candidates = [
        FaultStep(step_id="reserve_inventory", attempt=1, fault_type="timeout"),
        FaultStep(step_id="authorize_payment", attempt=1, fault_type="network_drop"),
        FaultStep(step_id="authorize_payment", attempt=2, fault_type="transient_500"),
        FaultStep(step_id="authorize_payment", attempt=3, fault_type="transient_500"),
        FaultStep(step_id="commit_ledger", attempt=1, fault_type="timeout"),
    ]

    # Failure predicate requiring authorize_payment to fail at least once
    def predicate(subset: List[FaultStep]) -> bool:
        return any(f.step_id == "authorize_payment" for f in subset)

    shrinker = FaultShrinker(predicate)
    result = shrinker.shrink(candidates)
    print(json.dumps(result.to_dict(), indent=2))
    return 0


def main() -> None:
    """Main CLI entrypoint supporting both legacy direct flags and modern subcommands."""
    parser = argparse.ArgumentParser(
        description="FaultForge Deterministic Chaos & Durable Systems CLI",
    )
    # Global / fallback flags for backward compatibility
    parser.add_argument("--seed", type=int, default=42, help="PRNG seed for fault injection")
    parser.add_argument("--fail-rate", type=float, default=0.35, help="Fault probability (0.0 - 1.0)")
    parser.add_argument("--run-id", type=str, default="cli_run_001", help="Workflow run ID")

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # run subcommand
    run_parser = subparsers.add_parser("run", help="Run workflow with chaos injection")
    run_parser.add_argument("--seed", type=int, default=42, help="PRNG seed")
    run_parser.add_argument("--fail-rate", type=float, default=0.35, help="Fault probability")
    run_parser.add_argument("--run-id", type=str, default="cli_run_001", help="Run ID")

    # replay subcommand
    replay_parser = subparsers.add_parser("replay", help="Replay completed workflow from WAL")
    replay_parser.add_argument("--run-id", type=str, required=True, help="Run ID to replay")

    # inspect-wal subcommand
    wal_parser = subparsers.add_parser("inspect-wal", help="Inspect WAL records for a run")
    wal_parser.add_argument("--run-id", type=str, required=True, help="Run ID")

    # diff subcommand
    diff_parser = subparsers.add_parser("diff", help="Diff two workflow execution trajectories")
    diff_parser.add_argument("--run-a", type=str, required=True, help="First run ID")
    diff_parser.add_argument("--run-b", type=str, required=True, help="Second run ID")

    # export-dag subcommand
    export_parser = subparsers.add_parser("export-dag", help="Export DAG Mermaid diagram or OTel trace")
    export_parser.add_argument("--run-id", type=str, default=None, help="Optional run ID for annotations")
    export_parser.add_argument("--format", choices=["mermaid", "otel"], default="mermaid", help="Export format")

    # shrink subcommand
    shrink_parser = subparsers.add_parser("shrink", help="Minimize a failing chaos fault schedule")

    args = parser.parse_args()
    wal = WriteAheadLog()

    if args.command == "run" or args.command is None:
        sys.exit(cmd_run(args, wal))
    elif args.command == "replay":
        sys.exit(cmd_replay(args, wal))
    elif args.command == "inspect-wal":
        sys.exit(cmd_inspect_wal(args, wal))
    elif args.command == "diff":
        sys.exit(cmd_diff(args, wal))
    elif args.command == "export-dag":
        sys.exit(cmd_export_dag(args, wal))
    elif args.command == "shrink":
        sys.exit(cmd_shrink(args, wal))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
