"""Integration tests for FastAPI server endpoints using TestClient."""

from fastapi.testclient import TestClient
from faultforge.api import app

client = TestClient(app)


def test_health_endpoint() -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "active_leases" in data
    assert "dlq_records" in data


def test_simulation_run_and_wal_inspection() -> None:
    # 1. Trigger simulation run
    sim_payload = {"run_id": "test_api_sim_01", "seed": 42, "failure_probability": 0.1}
    run_resp = client.post("/simulations/run", json=sim_payload)
    assert run_resp.status_code == 200
    run_data = run_resp.json()
    assert run_data["run_id"] == "test_api_sim_01"
    assert "output" in run_data

    # 2. Inspect WAL records
    wal_resp = client.get("/runs/test_api_sim_01/wal")
    assert wal_resp.status_code == 200
    wal_events = wal_resp.json()
    assert len(wal_events) >= 3
    event_types = [e["event_type"] for e in wal_events]
    assert "WORKFLOW_STARTED" in event_types
    assert "WORKFLOW_COMPLETED" in event_types


def test_lease_lifecycle_api() -> None:
    # 1. Acquire lease
    acquire_payload = {
        "step_id": "api_test_step",
        "owner_id": "worker_api_1",
        "ttl_seconds": 15.0,
    }
    acq_resp = client.post("/leases/acquire", json=acquire_payload)
    assert acq_resp.status_code == 200
    acq_data = acq_resp.json()
    assert acq_data["status"] == "ACQUIRED"
    token = acq_data["lease"]["lease_token"]
    version = acq_data["lease"]["version"]

    # 2. List active leases
    list_resp = client.get("/leases")
    assert list_resp.status_code == 200
    leases = list_resp.json()
    assert any(l["step_id"] == "api_test_step" for l in leases)

    # 3. Renew lease
    renew_payload = {
        "step_id": "api_test_step",
        "owner_id": "worker_api_1",
        "lease_token": token,
        "expected_version": version,
        "ttl_seconds": 20.0,
    }
    ren_resp = client.post("/leases/renew", json=renew_payload)
    assert ren_resp.status_code == 200
    assert ren_resp.json()["status"] == "RENEWED"
    assert ren_resp.json()["lease"]["version"] == version + 1

    # 4. Release lease
    rel_resp = client.post(
        "/leases/release",
        json={"step_id": "api_test_step", "owner_id": "worker_api_1", "lease_token": token},
    )
    assert rel_resp.status_code == 200
    assert rel_resp.json()["status"] == "RELEASED"


def test_lease_conflict_api_error() -> None:
    # Worker A acquires
    client.post(
        "/leases/acquire",
        json={"step_id": "conflict_step", "owner_id": "worker_a", "ttl_seconds": 30.0},
    )

    # Worker B attempts acquisition -> Expect 409 Conflict
    resp = client.post(
        "/leases/acquire",
        json={"step_id": "conflict_step", "owner_id": "worker_b", "ttl_seconds": 10.0},
    )
    assert resp.status_code == 409
    assert "actively locked" in resp.json()["detail"]


def test_dlq_inspection_and_redrive() -> None:
    from faultforge.api import shared_dlq

    # Seed a record into shared DLQ
    rec = shared_dlq.route_to_dlq(
        run_id="dlq_test_run",
        step_id="payment_fail",
        error=RuntimeError("Gateway 504"),
        attempts=3,
        payload={"amount": 99},
    )

    # Inspect via GET /dlq
    list_resp = client.get("/dlq")
    assert list_resp.status_code == 200
    records = list_resp.json()
    assert any(r["dlq_id"] == rec.dlq_id for r in records)

    # Redrive via POST /dlq/{id}/redrive
    redrive_resp = client.post(f"/dlq/{rec.dlq_id}/redrive")
    assert redrive_resp.status_code == 200
    assert redrive_resp.json()["record"]["redrive_count"] == 1

    # Purge via DELETE /dlq/{id}
    purge_resp = client.delete(f"/dlq/{rec.dlq_id}")
    assert purge_resp.status_code == 200
    assert purge_resp.json()["status"] == "PURGED"


def test_mermaid_trace_and_diff_endpoints() -> None:
    # Run two simulations
    client.post("/simulations/run", json={"run_id": "trace_run_1", "seed": 42, "failure_probability": 0.0})
    client.post("/simulations/run", json={"run_id": "trace_run_2", "seed": 42, "failure_probability": 0.0})

    # Test mermaid diagram export
    mermaid_resp = client.get("/traces/trace_run_1/mermaid")
    assert mermaid_resp.status_code == 200
    assert "flowchart TD" in mermaid_resp.text
    assert "reserve_inventory" in mermaid_resp.text

    # Test trajectory diff endpoint
    diff_resp = client.get("/diff/trace_run_1/trace_run_2")
    assert diff_resp.status_code == 200
    diff_data = diff_resp.json()
    assert diff_data["run_id_a"] == "trace_run_1"
    assert diff_data["run_id_b"] == "trace_run_2"
    assert diff_data["is_identical"] is True
