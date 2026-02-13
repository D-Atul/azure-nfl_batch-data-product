from __future__ import annotations

from typing import Tuple, Dict, Any

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F

from src.contracts.input_contract import raw_events_spark_schema, validate_raw_events_spark, ContractViolation


def ingest_validate_transform(
    spark,
    input_path: str,
    curated_path: str,
) -> Tuple[SparkDF, Dict[str, Any]]:
    """
    Azure batch v1 ingest:
    - strict read
    - fail-fast input contract
    - minimal transforms only
    - reconciliation invariant enforced (no drops / no imputation)
    - write curated to a run-scoped path
    Returns df_curated + stats for run.log.
    """
    df_raw = (
        spark.read
        .schema(raw_events_spark_schema())
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(input_path)
    )

    # Evidence: what we actually read
    rows_read_raw = int(df_raw.count())

    # Contract-first: fail fast before any business logic
    df_valid = validate_raw_events_spark(df_raw)

    # Minimal declared transforms (should not change row count)
    df_curated = (
        df_valid
        .withColumn("team_home", F.trim(F.col("team_home")))
        .withColumn("team_away", F.trim(F.col("team_away")))
        .withColumn("stadium", F.trim(F.col("stadium")))
    )

    # Reconciliation invariant: curated row count must match what we read
    rows_curated = int(df_curated.count())
    if rows_curated != rows_read_raw:
        raise ContractViolation(
            f"Reconciliation invariant failed: rows_read_raw={rows_read_raw} != rows_curated={rows_curated}"
        )

    # Write curated (run-scoped). Runner should guarantee unique run_id, but we still fail fast if it exists.
    try:
        df_curated.write.mode("errorifexists").parquet(curated_path)
    except Exception:
        # Some Synapse setups behave differently; fallback to a clear failure message.
        raise ContractViolation(f"Curated output path already exists (refusing to overwrite): {curated_path}")

    stats: Dict[str, Any] = {
        "rows_read_raw": rows_read_raw,
        "rows_curated": rows_curated,
        "row_policy": "reconcile_strict_equal",
        "declared_transforms": [
            "trim(team_home)",
            "trim(team_away)",
            "trim(stadium)",
        ],
    }

    return df_curated, stats
