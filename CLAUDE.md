# FaultForge — Claude Code Engineering Context

## Overview
FaultForge is a deterministic durable execution engine and automated chaos-testing framework for distributed multi-step workflows. Workflows execute as Directed Acyclic Graphs (DAGs) backed by an append-only Write-Ahead Log (WAL), optimistic-concurrency distributed step leasing, Merkle-tree state drift verification, and an automated Delta-Debugging fault schedule shrinker.

## Architecture Status (v0.2.0 - 40% Completed Architecture)
- **Engine Core:**
  - `faultforge/engine/wal.py`: Append-only SQLite WAL with SHA-256 idempotency deduplication.
  - `faultforge/engine/dag.py`: Kahn's algorithm topological DAG scheduler with CycleError validation.
  - `faultforge/engine/runtime.py`: Crash recovery runtime with idempotent step replay.
  - `faultforge/engine/lease.py`: Distributed step leasing with version counters, TTLs, and zombie worker reclamation.
  - `faultforge/engine/retry.py`: Exponential backoff with full jitter, non-retryable exceptions, and Dead-Letter Queue (DLQ).
  - `faultforge/engine/checkpointer.py`: State snapshot compaction with binary Merkle trees for cryptographic drift detection.
- **Chaos Engineering:**
  - `faultforge/chaos/injector.py`: Seeded pseudo-random fault injection.
  - `faultforge/chaos/network_sim.py`: Latency modeling (log-normal, Pareto), packet drops, timeouts, and payload bit flips.
  - `faultforge/chaos/shrinker.py`: Delta-Debugging ($ddmin$) 1-minimal fault schedule minimizer.
  - `faultforge/chaos/scenarios.py`: Pre-built split-brain, thundering herd, cascading timeout, and corrupted WAL scenarios.
- **Telemetry & Observability:**
  - `faultforge/telemetry/tracer.py`: OpenTelemetry-compatible JSON traces and Mermaid DAG flowchart generation.
  - `faultforge/telemetry/differ.py`: `TrajectoryDiffer` detecting order mismatches, mutated results, and missing steps.
- **Interfaces:**
  - `faultforge/cli.py`: Multi-subcommand CLI (`run`, `replay`, `inspect-wal`, `diff`, `export-dag`, `shrink`).
  - `faultforge/api.py`: FastAPI inspection server for WAL, leases, DLQ redrive, diffing, and Mermaid traces.

## Common Terminal Commands
- **Run full test suite (32 tests):** `pytest -v`
- **Run CLI chaos workflow:** `python -m faultforge.cli run --seed 42 --fail-rate 0.35`
- **Minimize failing fault schedule:** `python -m faultforge.cli shrink`
- **Compare two execution trajectories:** `python -m faultforge.cli diff --run-a run_01 --run-b run_02`
- **Export Mermaid DAG visualization:** `python -m faultforge.cli export-dag --format mermaid`
- **Start FastAPI server:** `uvicorn faultforge.api:app --reload --port 8000`

## Core Architectural Invariants
1. **Deterministic Replay (`faultforge/engine/runtime.py`):** Before executing any `StepNode` callable, always query `WriteAheadLog.get_completed_step(run_id, step_id)`. If a `STEP_COMPLETED` record exists, hydrate the context directly from the persisted JSON payload without re-running side effects.
2. **DAG Acyclicity (`faultforge/engine/dag.py`):** All workflow graphs must pass Kahn's algorithm topological validation before execution begins.
3. **Seeded Chaos Reproducibility (`faultforge/chaos/injector.py`):** Never use unseeded global randomness. Always use `random.Random(policy.seed)` so any failing execution trace can be replayed deterministically from the CLI.
4. **Optimistic Lease Concurrency (`faultforge/engine/lease.py`):** Worker step locks use monotonic version numbers. Renewals must provide `expected_version` to prevent split-brain dual execution.
5. **Cryptographic State Integrity (`faultforge/engine/checkpointer.py`):** State snapshots compute binary Merkle tree root hashes to prove that replayed runs arrive at bit-for-bit identical state across worker restarts.
