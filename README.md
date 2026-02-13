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

The pipeline triggers a Synapse notebook, and the notebook executes the runner packaged in:

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

---

### Step 1 — Notebook Execution Cell

The Synapse notebook contains a single execution cell that loads the code bundle and runs the batch:

```python
import sys

zip_path = "abfss://data@<batchrawdata>.dfs.core.windows.net/code/azure_batch_code.zip"
sys.path.insert(0, zip_path)

from src.runner.run_azure_batch import main

sys.argv = [
    "run_azure_batch",
    "--input_path", "abfss://data@<batchrawdata>.dfs.core.windows.net/raw/spreadspoke_scores.csv",
    "--curated_base", "abfss://data@<batchrawdata>.dfs.core.windows.net/curated",
    "--metrics_base", "abfss://data@<batchrawdata>.dfs.core.windows.net/metrics",
    "--logs_base", "abfss://data@<batchrawdata>.dfs.core.windows.net/logs",
]

main()
```

This cell is the only execution logic.  
All batch guarantees are enforced inside the runner.

---

### Step 2 — Trigger the Pipeline

1. Open **Synapse Studio**
2. Navigate to **Integrate → Pipelines**
3. Open the batch pipeline
4. Click **Trigger → Trigger now**

The pipeline:

- Starts a Spark session  
- Executes the notebook  
- The notebook calls the runner  
- The runner writes curated data, metrics, and `run.log`  

---

### Step 3 — Verify Output

After successful execution, a new run folder will exist:

```
curated/run_id=<run_id>/
metrics/run_id=<run_id>/M1..M6
logs/run_id=<run_id>/run.log
```

Open:

```
logs/run_id=<run_id>/run.log
```

Confirm:

```
"status": "success"
```

If `"status": "failed"`, inspect `"failure_reason"` in the run log.

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
