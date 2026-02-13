from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from src.jobs.ingest_validate_transform import ingest_validate_transform
from src.jobs.build_and_publish import build_and_publish


def join_uri(base: str, *parts: str) -> str:
    base_clean = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts if p)
    return f"{base_clean}/{tail}" if tail else base_clean


def _mssparkutils():
    try:
        from notebookutils import mssparkutils  # type: ignore
        return mssparkutils
    except Exception:
        return None


def ms_exists(ms, path: str) -> bool:
    try:
        return bool(ms.fs.exists(path))
    except Exception:
        return False


def best_effort_rm(ms, path: str) -> None:
    """Synapse-safe best-effort recursive delete."""
    try:
        ms.fs.rm(path, recurse=True)
        return
    except Exception:
        pass

    # Fallback: try deleting children
    try:
        for f in ms.fs.ls(path):
            try:
                ms.fs.rm(f.path, recurse=True)
            except Exception:
                pass
    except Exception:
        pass


def main() -> int:
    p = argparse.ArgumentParser(description="Azure Batch Data Product — Runner (Synapse, minimal)")
    p.add_argument("--input_path", required=True)
    p.add_argument("--curated_base", required=True)
    p.add_argument("--metrics_base", required=True)
    p.add_argument("--logs_base", required=True)
    args = p.parse_args()

    ms = _mssparkutils()
    if not ms:
        raise RuntimeError("This runner expects Synapse notebookutils.mssparkutils.")

    run_id = uuid.uuid4().hex
    t0 = time.perf_counter()

    curated_path = join_uri(args.curated_base, f"run_id={run_id}", "curated.parquet")
    metrics_root = join_uri(args.metrics_base, f"run_id={run_id}")
    logs_root = join_uri(args.logs_base, f"run_id={run_id}")
    run_log_path = join_uri(logs_root, "run.log")

    metrics_list = ["M1", "M2", "M3", "M4", "M5", "M6"]

    spark = SparkSession.builder.appName(f"azure-batch-{run_id}").getOrCreate()

    rows_read_raw: Optional[int] = None
    rows_curated: Optional[int] = None
    per_metric_rows: Dict[str, int] = {}

    status = "running"
    failure_reason: Optional[str] = None

    try:
        df_curated, ingest_stats = ingest_validate_transform(
            spark=spark,
            input_path=args.input_path,
            curated_path=curated_path,
        )

        rows_read_raw = int(ingest_stats.get("rows_read_raw", -1))
        rows_curated = int(ingest_stats.get("rows_curated", -1))

        out_stats, _out_paths = build_and_publish(
            spark=spark,
            df_curated=df_curated,
            metrics_base_path=metrics_root,
        )

        # Expect out_stats like: {"M1":{"rows_written":...}, ...}
        for mid in metrics_list:
            v = out_stats.get(mid, {})
            if isinstance(v, dict):
                per_metric_rows[mid] = int(v.get("rows_written", -1))
            else:
                per_metric_rows[mid] = int(v)

        status = "success"
        return_code = 0

    except Exception as e:
        status = "failed"
        failure_reason = f"{type(e).__name__}: {str(e)}"
        return_code = 1

        # Rule: no publish on failure (delete any partially written metrics for this run).
        if ms_exists(ms, metrics_root):
            best_effort_rm(ms, metrics_root)

    finally:
        # Log must contain ONLY the fields you asked for, in a sensible order.
        log_obj: Dict[str, Any] = {
            "run_id": run_id,
            "input_path": args.input_path,
            "curated_path": curated_path,
            "metrics_root": metrics_root,
            "metrics": metrics_list,
            "rows_read_raw": rows_read_raw,
            "rows_curated": rows_curated,
            "per_metric_rows": per_metric_rows,
            "status": status,
        }
        if status == "failed":
            log_obj["failure_reason"] = failure_reason

        # Write single run.log (one JSON object + newline)
        ms.fs.put(run_log_path, json.dumps(log_obj, separators=(",", ":"), sort_keys=False) + "\n", overwrite=True)

        try:
            spark.stop()
        except Exception:
            pass

        _ = int((time.perf_counter() - t0) * 1000)  # computed but intentionally not logged

    return return_code


if __name__ == "__main__":
    sys.exit(main())
