# FaultForge — Claude Code Engineering Context

## Overview
FaultForge is a deterministic durable execution engine and chaos-testing framework for distributed multi-step workflows. Workflows execute as Directed Acyclic Graphs (DAGs) backed by an append-only Write-Ahead Log (WAL) for zero-data-loss crash recovery.

## Common Terminal Commands
- **Run full test suite:** `pytest -v`
- **Run specific test file:** `pytest tests/test_durable_replay.py -v`
- **Run CLI chaos benchmark:** `python -m faultforge.cli --seed 42 --fail-rate 0.35`
- **Start FastAPI inspector:** `uvicorn faultforge.api:app --reload --port 8000`
- **Type check codebase:** `mypy faultforge tests`

## Core Architectural Invariants
1. **Deterministic Replay (`faultforge/engine/runtime.py`):** Before executing any `StepNode` callable, always query `WriteAheadLog.get_completed_step(run_id, step_id)`. If a `STEP_COMPLETED` record exists, hydrate the context directly from the persisted JSON payload without re-running side effects.
2. **DAG Acyclicity (`faultforge/engine/dag.py`):** All workflow graphs must pass Kahn's algorithm topological validation before execution begins.
3. **Seeded Chaos Reproducibility (`faultforge/chaos/injector.py`):** Never use unseeded global randomness. Always use `random.Random(policy.seed)` so any failing execution trace can be replayed deterministically from the CLI.
