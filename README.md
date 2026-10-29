# FaultForge

**Deterministic Durable Execution Engine & Automated Chaos-Testing Framework for Distributed Workflows.**

FaultForge guarantees fault-tolerant, zero-data-loss execution of multi-step distributed workflows using an **event-sourced Write-Ahead Log (WAL)**, **Directed Acyclic Graph (DAG) dependency resolution**, **optimistic-concurrency distributed step leasing**, and an **automated Delta-Debugging fault schedule shrinker**.

---

## Architecture Overview (v0.2.0 — 40% Completed Architecture)

```
                            +-----------------------------------+
                            |       Workflow DAG Scheduler      |
                            |       (Topological / Kahn's)      |
                            +-----------------+-----------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
                     v                                                 v
         +-----------------------+                         +-----------------------+
         |     DurableRuntime    |                         |  Distributed Leasing  |
         | (Zero Side-Effects)   |                         |  (Versioned Locks/TTL)|
         +-----------+-----------+                         +-----------+-----------+
                     |                                                 |
         +-----------v-----------+                         +-----------v-----------+
         |    Write-Ahead Log    |                         |   Dead-Letter Queue   |
         | (ACID SQLite / Append)|                         | (Retry / Jitter DLQ)  |
         +-----------+-----------+                         +-----------------------+
                     |
         +-----------v-----------+
         |   Merkle Checkpointer |
         | (SHA-256 Drift Proof) |
         +-----------------------+
```

### Key Subsystems & Modules
- **Engine Core:**
  - `faultforge/engine/wal.py`: Append-only SQLite WAL with SHA-256 idempotency deduplication.
  - `faultforge/engine/dag.py`: Kahn's algorithm topological DAG scheduler with `CycleError` validation.
  - `faultforge/engine/runtime.py`: Crash recovery runtime with idempotent step replay.
  - `faultforge/engine/lease.py`: Distributed step leasing with version counters, TTLs, and zombie worker reclamation.
  - `faultforge/engine/retry.py`: Exponential backoff with full jitter, non-retryable exceptions, and Dead-Letter Queue (DLQ).
  - `faultforge/engine/checkpointer.py`: State snapshot compaction with binary Merkle trees for cryptographic drift detection.
- **Chaos Engineering & Delta Debugging:**
  - `faultforge/chaos/injector.py`: Seeded pseudo-random fault injection.
  - `faultforge/chaos/network_sim.py`: Latency modeling (log-normal, Pareto), packet drops, timeouts, and payload bit flips.
  - `faultforge/chaos/shrinker.py`: Delta-Debugging ($ddmin$) 1-minimal fault schedule minimizer.
  - `faultforge/chaos/scenarios.py`: Pre-built split-brain, thundering herd, cascading timeout, and corrupted WAL scenarios.
- **Telemetry & Trajectory Diffing:**
  - `faultforge/telemetry/tracer.py`: OpenTelemetry-compatible JSON traces and Mermaid DAG flowchart generation.
  - `faultforge/telemetry/differ.py`: `TrajectoryDiffer` detecting order mismatches, mutated results, and missing steps.
- **Interfaces:**
  - `faultforge/cli.py`: Multi-subcommand CLI (`run`, `replay`, `inspect-wal`, `diff`, `export-dag`, `shrink`).
  - `faultforge/api.py`: FastAPI inspection server for WAL, leases, DLQ redrive, diffing, and Mermaid traces.

---

## Quickstart

### Installation & Test Suite
```bash
pip install -e ".[dev]"
pytest -v   # Executes 32 unit, concurrency, chaos, and API tests
```

### CLI Commands
```bash
# Execute workflow with seeded chaos
python -m faultforge.cli run --seed 42 --fail-rate 0.35

# Inspect WAL event records
python -m faultforge.cli inspect-wal --run-id cli_run_001

# Minimize a multi-fault failure schedule down to 1-minimal trace
python -m faultforge.cli shrink

# Detect state mutations and trajectory divergences between runs
python -m faultforge.cli diff --run-a run_01 --run-b run_02

# Export visual Mermaid flowchart diagram of workflow DAG
python -m faultforge.cli export-dag --format mermaid
```

### FastAPI Inspection Server
```bash
uvicorn faultforge.api:app --reload --port 8000
```
- Interactive Swagger docs: `http://localhost:8000/docs`
- Health status: `GET /health`
- Active leases: `GET /leases`
- Dead-Letter Queue: `GET /dlq`
- Visual Mermaid DAG: `GET /traces/{run_id}/mermaid`
- Trajectory diff: `GET /diff/{run_a}/{run_b}`

---

## Engineering Roadmap

### Phase 1: Core Durable Engine & Local WAL (Completed)
- [x] Append-only SQLite Write-Ahead Log (`faultforge/engine/wal.py`)
- [x] Topological DAG scheduler & cycle verification (`faultforge/engine/dag.py`)
- [x] Durable runtime with idempotent step replay (`faultforge/engine/runtime.py`)
- [x] Seeded deterministic chaos fault injector (`faultforge/chaos/injector.py`)

### Phase 2: Distributed Leasing, Delta Debugging & Observability (Completed v0.2.0)
- [x] Optimistic concurrency leasing & zombie worker reclamation (`faultforge/engine/lease.py`)
- [x] Exponential backoff with full jitter & Dead-Letter Queue (`faultforge/engine/retry.py`)
- [x] Merkle-tree state snapshot compaction & drift verification (`faultforge/engine/checkpointer.py`)
- [x] Network & resource chaos simulation with log-normal/Pareto jitter (`faultforge/chaos/network_sim.py`)
- [x] Delta-Debugging ($ddmin$) automated fault schedule shrinker (`faultforge/chaos/shrinker.py`)
- [x] Trajectory differ and OpenTelemetry / Mermaid telemetry export (`faultforge/telemetry/`)
- [x] 32 comprehensive tests across 7 test files (`tests/`)

### Phase 3: Distributed Storage Adapters & Network Chaos Proxy (60% Remaining Backlog)
- [ ] PostgreSQL storage adapter using `SELECT FOR UPDATE SKIP LOCKED`
- [ ] Redis Streams consumer group worker queue & distributed heartbeats
- [ ] gRPC binary worker communication protocol with protobuf schemas
- [ ] Toxiproxy TCP fault harness for socket partition simulation
- [ ] Prometheus metrics exporter for queue latency and lease contention

### Phase 4: Real-Time DAG Observability UI
- [ ] Next.js + React Flow live DAG execution visualizer & time-travel replay debugger
