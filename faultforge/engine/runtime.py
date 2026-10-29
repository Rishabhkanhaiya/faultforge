from typing import Any, Dict, Optional
from faultforge.chaos.injector import ChaosInjector
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.wal import EventType, WriteAheadLog


class DurableRuntime:
    """Executes workflow DAGs with automatic WAL checkpointing and deterministic state replay."""

    def __init__(self, wal: WriteAheadLog, chaos: Optional[ChaosInjector] = None) -> None:
        self.wal = wal
        self.chaos = chaos

    def execute(
        self,
        run_id: str,
        dag: WorkflowDAG,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = dict(initial_context or {})
        ordered_steps = dag.topological_order()
        self.wal.append(run_id, "__workflow__", EventType.WORKFLOW_STARTED, {"workflow": dag.name})

        for node in ordered_steps:
            cached = self.wal.get_completed_step(run_id, node.step_id)
            if cached is not None:
                context[node.step_id] = cached.get("result")
                continue

            attempts = 0
            while True:
                attempts += 1
                self.wal.append(
                    run_id,
                    node.step_id,
                    EventType.STEP_STARTED,
                    {"attempt": attempts},
                    attempt=attempts,
                )
                try:
                    if self.chaos:
                        self.chaos.maybe_inject_fault(node.step_id, attempts)
                    result = node.handler(context)
                    self.wal.append(
                        run_id,
                        node.step_id,
                        EventType.STEP_COMPLETED,
                        {"result": result, "attempts": attempts},
                        attempt=attempts,
                    )
                    context[node.step_id] = result
                    break
                except Exception as exc:
                    self.wal.append(
                        run_id,
                        node.step_id,
                        EventType.STEP_FAILED,
                        {"attempt": attempts, "error": str(exc)},
                        attempt=attempts,
                    )
                    if attempts > node.max_retries:
                        raise

        self.wal.append(run_id, "__workflow__", EventType.WORKFLOW_COMPLETED, {"status": "SUCCESS"})
        return context
