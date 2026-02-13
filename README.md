# Azure Batch Data Product (Synapse Spark)

## What this is

This is a contract-first Azure batch system built on Synapse Spark.

It reads a historical NFL dataset (`spreadspoke_scores.csv`), validates it strictly, computes six deterministic metrics, and publishes them to ADLS under a run-scoped path.

Each execution produces exactly one run log that serves as the single source of truth.

No streaming.  
No incremental logic.  
No enrichment.  
No hidden mutation.

Just clean, deterministic batch.

---

## What it produces

Six metrics (played games only — both scores present):

- **M1** – Games played per team per season  
- **M2** – Win / loss / tie counts per team per season  
- **M3** – Points for and against per team per season  
- **M4** – Total games per season  
- **M5** – Regular vs playoff game counts per season  
- **M6** – Neutral venue game count per season  

Scheduled/future games (both scores null) are excluded from metric computation.

---

## How to Run

This project is executed through an Azure Synapse Pipeline.

The pipeline triggers a Synapse notebook, and the notebook calls the batch runner packaged in:

```
code/azure_batch_code.zip
```

---

### Prerequisites

- Azure Synapse workspace
- Spark pool available and running
- ADLS Gen2 container containing:
  - `raw/spreadspoke_scores.csv`
  - `code/azure_batch_code.zip`

The zip file must contain the `src/` package (runner, jobs, contracts).

---

### Run the Batch

1. Open **Synapse Studio**
2. Navigate to **Integrate → Pipelines**
3. Open the batch pipeline
4. Click **Trigger → Trigger now**

The pipeline will:

1. Start a Spark session
2. Execute the notebook
3. The notebook imports the runner from `azure_batch_code.zip`
4. The runner:
   - Validates the input contract
   - Writes curated output
   - Builds M1–M6 metrics
   - Writes a single `run.log`

---

### Output Location

After successful execution, a new `run_id` folder will be created:

```
curated/run_id=<run_id>/
metrics/run_id=<run_id>/M1..M6
logs/run_id=<run_id>/run.log
```

Open:

```
logs/run_id=<run_id>/run.log
```

and confirm:

```
"status": "success"
```

If `"status": "failed"`, the `failure_reason` field will explain the failure, and metrics for that run will not remain published.

---

## Storage layout (ADLS Gen2)

- raw/spreadspoke_scores.csv
- code/azure_batch_code.zip
- logs/run_id=b7919d1111974426b23e31ccc0807276/run.log
- curated/run_id=b7919d1111974426b23e31ccc0807276/curated.parquet/
  - _SUCCESS+parquet part file
- metrics/run_id=b7919d1111974426b23e31ccc0807276/
    - M1/ _SUCCESS + parquet part file
    - M2/ _SUCCESS + parquet part file
    - M3/ _SUCCESS + parquet part file
    - M4/ _SUCCESS + parquet part file
    - M5/ _SUCCESS + parquet part file
    - M6/ _SUCCESS + parquet part file

---

## Run evidence

Each run produces one file:

- logs/run_id=<run_id>/run.log

**Structure:**

 - run_id
 - input_path
 - curated_path
 - metrics_root
 - metrics
 - rows_read_raw
 - rows_curated
 - per_metric_rows
 - status
 - failure_reason (if failed)

If status = success, metrics exist.
If status = failed, metrics were removed.


This file is the single source of truth.

If it says `status: success`, metrics exist.
If it says `status: failed`, metrics were removed.

---

## Design principles

- Contract-first validation
- Deterministic full recompute batch
- No deduplication
- No enrichment joins
- No imputation
- No partial publish
- Evidence over claims
- Notebook is read-only review

---

## What this project demonstrates

- Clean cloud batch design
- Strict data contracts
- Deterministic metric computation
- Explicit reconciliation invariant
- Run isolation in ADLS
- Evidence-driven engineering

It is intentionally small and controlled.
