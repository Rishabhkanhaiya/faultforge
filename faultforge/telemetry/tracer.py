"""Execution timeline tracing, OpenTelemetry-compatible spans, and Mermaid DAG generation."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from faultforge.engine.dag import WorkflowDAG
from faultforge.engine.wal import EventType, WALEntry


class SpanStatus(str, Enum):
    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


@dataclass
class TraceSpan:
    """Execution span capturing latency, retry watermarks, and WAL correlation."""

    span_id: str
    trace_id: str
    name: str
    step_id: str
    parent_span_id: Optional[str]
    start_time_unix_nano: int
    end_time_unix_nano: int
    status: SpanStatus
    attributes: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    wal_seq_watermark: int = 0

    @property
    def latency_ms(self) -> float:
        """Duration of span in milliseconds."""
        diff_nano = self.end_time_unix_nano - self.start_time_unix_nano
        return max(0.0, diff_nano / 1_000_000.0)

    def to_otel_dict(self) -> Dict[str, Any]:
        """Format span as OpenTelemetry-compliant dictionary."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "start_time_unix_nano": self.start_time_unix_nano,
            "end_time_unix_nano": self.end_time_unix_nano,
            "duration_ms": round(self.latency_ms, 3),
            "status": {"code": self.status.value},
            "attributes": {
                **self.attributes,
                "faultforge.step_id": self.step_id,
                "faultforge.retry_count": self.retry_count,
                "faultforge.wal_watermark": self.wal_seq_watermark,
            },
        }


class TimelineTracer:
    """Tracer constructing hierarchical DAG spans and visual diagrams from WAL history."""

    def __init__(self, trace_id: Optional[str] = None) -> None:
        self.trace_id = trace_id or str(uuid.uuid4()).replace("-", "")[:32]
        self.spans: List[TraceSpan] = []

    def record_step_span(
        self,
        step_id: str,
        start_time: float,
        end_time: float,
        status: SpanStatus,
        retry_count: int = 0,
        wal_seq_watermark: int = 0,
        attributes: Optional[Dict[str, Any]] = None,
        parent_span_id: Optional[str] = None,
    ) -> TraceSpan:
        """Create and store a trace span for a completed or failed step execution."""
        span = TraceSpan(
            span_id=str(uuid.uuid4()).replace("-", "")[:16],
            trace_id=self.trace_id,
            name=f"faultforge.step:{step_id}",
            step_id=step_id,
            parent_span_id=parent_span_id,
            start_time_unix_nano=int(start_time * 1_000_000_000),
            end_time_unix_nano=int(end_time * 1_000_000_000),
            status=status,
            attributes=attributes or {},
            retry_count=retry_count,
            wal_seq_watermark=wal_seq_watermark,
        )
        self.spans.append(span)
        return span

    def to_otel_json(self) -> str:
        """Export all recorded spans in standard OpenTelemetry JSON format."""
        payload = {
            "resource_spans": [
                {
                    "resource": {
                        "attributes": {
                            "service.name": "faultforge-engine",
                            "telemetry.sdk.language": "python",
                        }
                    },
                    "scope_spans": [
                        {
                            "scope": {"name": "faultforge.telemetry"},
                            "spans": [s.to_otel_dict() for s in self.spans],
                        }
                    ],
                }
            ]
        }
        return json.dumps(payload, indent=2)

    def generate_mermaid_diagram(
        self,
        dag: WorkflowDAG,
        wal_entries: Optional[List[WALEntry]] = None,
    ) -> str:
        """
        Generate rich Mermaid flowchart diagram annotated with runtime step statuses,
        retry counts, and dependency edges.
        """
        lines = ["flowchart TD"]

        # Map steps to outcomes from WAL if provided
        step_status: Dict[str, str] = {}
        step_retries: Dict[str, int] = {}

        if wal_entries:
            for entry in wal_entries:
                if entry.step_id == "__workflow__":
                    continue
                if entry.event_type == EventType.STEP_COMPLETED:
                    step_status[entry.step_id] = "COMPLETED"
                    step_retries[entry.step_id] = entry.payload.get("attempts", 1) - 1
                elif entry.event_type == EventType.STEP_FAILED:
                    if step_status.get(entry.step_id) != "COMPLETED":
                        step_status[entry.step_id] = "FAILED"
                    step_retries[entry.step_id] = step_retries.get(entry.step_id, 0) + 1

        for step_id, node in dag.nodes.items():
            status = step_status.get(step_id, "PENDING")
            retries = step_retries.get(step_id, 0)
            retry_badge = f" [Retries: {retries}]" if retries > 0 else ""
            label = f"{step_id}<br/>({status}{retry_badge})"
            lines.append(f'    {step_id}["{label}"]')

        for step_id, node in dag.nodes.items():
            for dep in sorted(node.depends_on):
                lines.append(f"    {dep} --> {step_id}")

        # Add styling classes
        lines.append("    classDef completed fill:#28a745,stroke:#1e7e34,color:#fff;")
        lines.append("    classDef failed fill:#dc3545,stroke:#bd2130,color:#fff;")
        lines.append("    classDef pending fill:#6c757d,stroke:#545b62,color:#fff;")

        for step_id in dag.nodes:
            status = step_status.get(step_id, "PENDING")
            if status == "COMPLETED":
                lines.append(f"    class {step_id} completed;")
            elif status == "FAILED":
                lines.append(f"    class {step_id} failed;")
            else:
                lines.append(f"    class {step_id} pending;")

        return "\n".join(lines)
