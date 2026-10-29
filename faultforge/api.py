from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from faultforge.chaos.injector import ChaosInjector, ChaosPolicy
from faultforge.cli import build_demo_workflow
from faultforge.engine.runtime import DurableRuntime
from faultforge.engine.wal import WriteAheadLog

app = FastAPI(title="FaultForge Inspector API", version="0.1.0")
shared_wal = WriteAheadLog()


class SimulationRequest(BaseModel):
    run_id: str = Field(..., examples=["sim_run_101"])
    seed: int = Field(default=42)
    failure_probability: float = Field(default=0.25, ge=0.0, le=0.9)


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "healthy", "engine": "FaultForge v0.1.0"}


@app.post("/simulations/run")
def run_simulation(req: SimulationRequest) -> Dict[str, Any]:
    chaos = ChaosInjector(ChaosPolicy(seed=req.seed, failure_probability=req.failure_probability))
    runtime = DurableRuntime(wal=shared_wal, chaos=chaos)
    dag = build_demo_workflow()
    try:
        output = runtime.execute(req.run_id, dag)
    except Exception as exc:
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
