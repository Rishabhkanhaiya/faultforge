# FaultForge

**Deterministic Durable Execution Engine & Automated Chaos-Testing Framework for Distributed Workflows.**

FaultForge guarantees fault-tolerant execution of multi-step workflows using an **event-sourced Write-Ahead Log (WAL)**, **Directed Acyclic Graph (DAG) dependency resolution**, and a **seeded deterministic Chaos Fault Injector**.

## Key Capabilities
- **Zero Duplicate Side Effects:** Completed workflow steps are checkpointed to an ACID Write-Ahead Log. When a crashed worker resumes a workflow, completed steps are replayed deterministically from the WAL without re-triggering external APIs or payments.
- **Topological DAG Scheduler:** Resolves diamond dependencies and detects circular dependencies prior to runtime execution.
- **Deterministic Chaos Harness:** Injects seeded transient failures, network timeouts, and process crashes during execution to prove recovery invariants in CI.

## Quickstart
```bash
pip install -e ".[dev]"
pytest -v
python -m faultforge.cli --seed 42 --fail-rate 0.4
```

## Engineering Roadmap

### Phase 1: Core Durable Engine & Local WAL (Completed)
- [x] Append-only SQLite Write-Ahead Log (`faultforge/engine/wal.py`)
- [x] Topological DAG scheduler & cycle verification (`faultforge/engine/dag.py`)
- [x] Durable runtime with idempotent step replay (`faultforge/engine/runtime.py`)
- [x] Seeded deterministic chaos fault injector (`faultforge/chaos/injector.py`)

### Phase 2: Distributed Storage & Concurrency Control (Active)
- [ ] PostgreSQL WAL storage adapter using `SELECT FOR UPDATE SKIP LOCKED`
- [ ] Worker heartbeat leases & zombie task reclamation
- [ ] Exponential backoff retry policies with Dead-Letter Queue (DLQ) sinks
- [ ] Property-based state-machine fuzzing with Hypothesis

### Phase 3: Network Chaos Proxy & Multi-Worker CLI
- [ ] TCP/HTTP proxy fault injection (packet drops, latency jitter, clock skew)
- [ ] Automated trace minimization for failing chaos seeds

### Phase 4: Real-Time DAG Observability UI
- [ ] Next.js + React Flow live DAG execution visualizer & time-travel replay debugger
