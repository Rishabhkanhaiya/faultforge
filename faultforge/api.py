"""FastAPI Inspection Server for FaultForge: WAL events, leases, DLQ redrive, and diffing."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy
from faultforge.cli import build_demo_workflow
from faultforge.engine.lease import (
    LeaseAcquisitionError,
    LeaseExpiredError,
    LeaseManager,
    LeaseVersionConflictError,
)
from faultforge.engine.retry import DeadLetterQueue
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import WriteAheadLog
from faultforge.telemetry.differ import TrajectoryDiffer
from faultforge.telemetry.tracer import TimelineTracer

app = FastAPI(
    title="FaultForge Inspector API",
    description="Durable execution, distributed leasing, DLQ inspection, and trajectory diffing.",
    version="0.2.0",
)

# Shared in-memory coordinators for server lifetime
shared_wal = WriteAheadLog()
shared_lease_mgr = LeaseManager()
shared_dlq = DeadLetterQueue()


# ------------------ Request Models ------------------


class SimulationRequest(BaseModel):
    run_id: str = Field(..., examples=["sim_run_101"])
    seed: int = Field(default=42)
    failure_probability: float = Field(default=0.25, ge=0.0, le=0.9)


class LeaseAcquireRequest(BaseModel):
    step_id: str = Field(..., examples=["reserve_inventory"])
    owner_id: str = Field(..., examples=["worker_node_1"])
    ttl_seconds: float = Field(default=10.0, gt=0.0)


class LeaseRenewRequest(BaseModel):
    step_id: str
    owner_id: str
    lease_token: str
    expected_version: int
    ttl_seconds: float = Field(default=10.0, gt=0.0)


class LeaseReleaseRequest(BaseModel):
    step_id: str
    owner_id: str
    lease_token: str


# ------------------ Health & Status ------------------


@app.get("/health")
def health_check() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "engine": "FaultForge v0.2.0",
        "active_leases": len(shared_lease_mgr.list_active_leases()),
        "dlq_records": shared_dlq.count(),
    }


# ------------------ Simulation & WAL ------------------


@app.post("/simulations/run")
def run_simulation(req: SimulationRequest) -> Dict[str, Any]:
    chaos = ChaosInjector(ChaosPolicy(seed=req.seed, failure_probability=req.failure_probability))
    runtime = DurableRuntime(wal=shared_wal, chaos=chaos)
    dag = build_demo_workflow()
    try:
        output = runtime.execute(req.run_id, dag)
    except Exception as exc:
        shared_dlq.route_to_dlq(
            run_id=req.run_id,
            step_id="workflow_failure",
            error=exc,
            attempts=5,
            payload={"seed": req.seed, "failure_probability": req.failure_probability},
        )
        raise HTTPException(status_code=500, detail=f"Workflow exhausted retries: {exc}") from exc

    return {
        "run_id": req.run_id,
        "injected_faults": chaos.injected_faults,
        "output": output,
    }


@app.get("/runs/{run_id}/wal")
def get_run_wal(run_id: str) -> List[Dict[str, Any]]:
    entries = shared_wal.get_history(run_id)
    if not entries:
        raise HTTPException(status_code=404, detail=f"No WAL history found for run_id='{run_id}'")
    return [
        {
            "seq_id": e.seq_id,
            "step_id": e.step_id,
            "event_type": e.event_type.value,
            "idempotency_key": e.idempotency_key,
            "payload": e.payload,
            "timestamp": e.timestamp,
        }
        for e in entries
    ]


# ------------------ Distributed Leases ------------------


@app.get("/leases")
def list_leases() -> List[Dict[str, Any]]:
    leases = shared_lease_mgr.list_active_leases()
    return [lease.to_dict() for lease in leases]


@app.post("/leases/acquire")
def acquire_lease(req: LeaseAcquireRequest) -> Dict[str, Any]:
    try:
        lease = shared_lease_mgr.acquire_lease(
            step_id=req.step_id,
            owner_id=req.owner_id,
            ttl_seconds=req.ttl_seconds,
        )
        return {"status": "ACQUIRED", "lease": lease.to_dict()}
    except LeaseAcquisitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/leases/renew")
def renew_lease(req: LeaseRenewRequest) -> Dict[str, Any]:
    try:
        lease = shared_lease_mgr.renew_lease(
            step_id=req.step_id,
            owner_id=req.owner_id,
            lease_token=req.lease_token,
            expected_version=req.expected_version,
            ttl_seconds=req.ttl_seconds,
        )
        return {"status": "RENEWED", "lease": lease.to_dict()}
    except LeaseVersionConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (LeaseExpiredError, LeaseAcquisitionError) as exc:
        raise HTTPException(status_code=410, detail=str(exc)) from exc


@app.post("/leases/release")
def release_lease(req: LeaseReleaseRequest) -> Dict[str, Any]:
    released = shared_lease_mgr.release_lease(
        step_id=req.step_id,
        owner_id=req.owner_id,
        lease_token=req.lease_token,
    )
    if not released:
        raise HTTPException(status_code=404, detail="Lease not found or worker credentials mismatch")
    return {"status": "RELEASED", "step_id": req.step_id}


# ------------------ Dead-Letter Queue (DLQ) ------------------


@app.get("/dlq")
def list_dlq(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    records = shared_dlq.list_records(limit=limit, offset=offset)
    return [r.to_dict() for r in records]


@app.post("/dlq/{dlq_id}/redrive")
def redrive_dlq(dlq_id: int) -> Dict[str, Any]:
    record = shared_dlq.redrive(dlq_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"DLQ record {dlq_id} not found")
    return {"status": "REDRIVEN", "record": record.to_dict()}


@app.delete("/dlq/{dlq_id}")
def purge_dlq(dlq_id: int) -> Dict[str, Any]:
    purged = shared_dlq.purge(dlq_id)
    if not purged:
        raise HTTPException(status_code=404, detail=f"DLQ record {dlq_id} not found")
    return {"status": "PURGED", "dlq_id": dlq_id}


# ------------------ Telemetry & Diffing ------------------


@app.get("/traces/{run_id}/mermaid")
def export_mermaid_trace(run_id: str) -> Response:
    history = shared_wal.get_history(run_id)
    dag = build_demo_workflow()
    tracer = TimelineTracer()
    mermaid_code = tracer.generate_mermaid_diagram(dag, history)
    return Response(content=mermaid_code, media_type="text/vnd.mermaid")


@app.get("/diff/{run_a}/{run_b}")
def diff_trajectories(run_a: str, run_b: str) -> Dict[str, Any]:
    differ = TrajectoryDiffer(shared_wal)
    report = differ.diff_runs(run_a, run_b)
    return report.to_dict()
