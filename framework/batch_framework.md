# Azure Batch Data Product — Pre-Development Framework

## Purpose

Define the rules of the Azure batch system before execution.

This document declares:

- What the system guarantees  
- What it explicitly does not attempt  
- What evidence must exist for a run to be considered valid  

This is a rulebook.  
It does not describe implementation details.

---

## System Overview

Execution flow:

Pipeline → Notebook → Runner (from `azure_batch_code.zip`) → ADLS

The runner performs:

1. Strict schema read
2. Fail-fast input contract validation
3. Minimal declared transforms
4. Deterministic metric computation
5. Run-scoped publication
6. Single run log creation

---

## Scope

### In Scope

- Full batch recompute of a finite historical dataset
- Azure Data Lake Storage Gen2
- Synapse Spark execution
- Run isolation using `run_id`
- Exactly six published metrics
- Single structured run log as evidence

### Out of Scope

- Streaming or incremental processing
- ML, forecasting, optimisation
- Performance tuning
- Hash-based determinism proofs
- Atomic staging frameworks
- Multiple evidence artifacts
- Cost estimation

This system is intentionally minimal.

---

## Core Guarantees

### 1. Contract-First Validation

Input must satisfy:

- Exact schema (no missing or extra columns)
- Required non-null fields
- Duplicate key rejection
- Score consistency rule
- Basic domain sanity

Any violation causes immediate failure.

No silent fixes.
No coercion.
No enrichment.

---

### 2. Deterministic Batch

Given the same input file, the system produces identical metric content.

Determinism is defined at the data level, not file-level ordering.

---

### 3. Reconciliation Invariant

The number of rows written to curated output must equal:

```
rows_read_raw
```

No rows are dropped.
No rows are imputed.

---

### 4. Played-Game Rule

Metrics are computed using only rows where:

```
score_home IS NOT NULL AND score_away IS NOT NULL
```

Scheduled/future games are excluded from metric computation but remain in curated output.

---

### 5. Metrics

Exactly six metrics are produced:

- M1 — Games played per team per season  
- M2 — Win / loss / tie counts per team per season  
- M3 — Points for and against per team per season  
- M4 — Total games per season  
- M5 — Regular vs playoff game counts per season  
- M6 — Neutral-venue game count per season  

No additional metrics exist in v1.

Any change requires a version bump.

---

### 6. Run Isolation

Each execution generates a unique:

```
run_id
```

All outputs are written under:

```
curated/run_id=<run_id>/
metrics/run_id=<run_id>/
logs/run_id=<run_id>/
```

No run overwrites another.

---

### 7. Publish Rule

If any step fails:

- The run status is recorded as `failed`
- Metric outputs for that run are deleted

Only successful runs retain published metrics.

---

## Evidence Model

Each run produces exactly one artifact:

```
logs/run_id=<run_id>/run.log
```

The run log contains:

- run_id  
- input_path  
- curated_path  
- metrics_root  
- metric list  
- rows_read_raw  
- rows_curated  
- per_metric_rows  
- status  
- failure_reason (if failed)

This file is the single source of truth.

If:

```
status = success
```

metrics exist and are considered published.

If:

```
status = failed
```

metrics for that run must not remain.

---

## Notebook Policy

Notebooks:

- Do not execute pipelines
- Do not recompute metrics
- Do not mutate data
- Only inspect existing artifacts

Evidence is inspection-only.

---

## Definition of Complete

The system satisfies this framework when:

- Input contract is enforced fail-fast
- Reconciliation invariant holds
- Exactly six metrics are produced
- Run isolation is respected
- A single run log is written
- Evidence aligns with outputs
- No claim exceeds what artifacts prove

---

## Design Philosophy

- Minimal complexity
- Deterministic logic
- Explicit guarantees
- Evidence over narrative
- No overengineering
- First-job ready with senior discipline

This framework governs all v1 Azure batch executions.
