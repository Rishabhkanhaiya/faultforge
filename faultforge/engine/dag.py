from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set


class CycleError(Exception):
    """Raised when a workflow dependency graph contains a cycle."""


@dataclass
class StepNode:
    step_id: str
    handler: Callable[[Dict[str, Any]], Any]
    depends_on: Set[str] = field(default_factory=set)
    max_retries: int = 3


class WorkflowDAG:
    """Directed Acyclic Graph representing deterministic step execution order."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.nodes: Dict[str, StepNode] = {}

    def add_step(
        self,
        step_id: str,
        handler: Callable[[Dict[str, Any]], Any],
        depends_on: Optional[List[str]] = None,
        max_retries: int = 3,
    ) -> None:
        if step_id in self.nodes:
            raise ValueError(f"Duplicate step_id '{step_id}' in workflow '{self.name}'")
        self.nodes[step_id] = StepNode(
            step_id=step_id,
            handler=handler,
            depends_on=set(depends_on or []),
            max_retries=max_retries,
        )

    def topological_order(self) -> List[StepNode]:
        in_degree: Dict[str, int] = {k: 0 for k in self.nodes}
        adj: Dict[str, List[str]] = {k: [] for k in self.nodes}

        for step_id, node in self.nodes.items():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise KeyError(f"Step '{step_id}' depends on unregistered step '{dep}'")
                adj[dep].append(step_id)
                in_degree[step_id] += 1

        queue = deque(sorted([k for k, deg in in_degree.items() if deg == 0]))
        ordered: List[StepNode] = []

        while queue:
            curr = queue.popleft()
            ordered.append(self.nodes[curr])
            for neighbor in sorted(adj[curr]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(self.nodes):
            raise CycleError(f"Circular dependency detected in workflow DAG '{self.name}'")

        return ordered
