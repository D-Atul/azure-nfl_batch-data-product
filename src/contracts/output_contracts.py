from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class ContractViolation(Exception):
    """Raised when a metric output violates its contract."""


def _fail(msg: str) -> None:
    raise ContractViolation(msg)


METRIC_IDS_V1: List[str] = ["M1", "M2", "M3", "M4", "M5", "M6"]


@dataclass(frozen=True)
class OutputContract:
    metric_id: str
    description: str
    schema: StructType
    grain_keys: List[str]
    non_null_cols: List[str]
    non_negative_cols: List[str]


def contracts_v1() -> Dict[str, OutputContract]:
    return {
        "M1": OutputContract(
            metric_id="M1",
            description="Games played per team per season (played games only).",
            schema=StructType([
                StructField("team", StringType(), False),
                StructField("season", IntegerType(), False),
                StructField("games_played", IntegerType(), False),
            ]),
            grain_keys=["team", "season"],
            non_null_cols=["team", "season", "games_played"],
            non_negative_cols=["games_played"],
        ),
        "M2": OutputContract(
            metric_id="M2",
            description="Win/loss/tie counts per team per season (played games only).",
            schema=StructType([
                StructField("team", StringType(), False),
                StructField("season", IntegerType(), False),
                StructField("wins", IntegerType(), False),
                StructField("losses", IntegerType(), False),
                StructField("ties", IntegerType(), False),
            ]),
            grain_keys=["team", "season"],
            non_null_cols=["team", "season", "wins", "losses", "ties"],
            non_negative_cols=["wins", "losses", "ties"],
        ),
        "M3": OutputContract(
            metric_id="M3",
            description="Points for/against per team per season (played games only).",
            schema=StructType([
                StructField("team", StringType(), False),
                StructField("season", IntegerType(), False),
                StructField("points_for", IntegerType(), False),
                StructField("points_against", IntegerType(), False),
            ]),
            grain_keys=["team", "season"],
            non_null_cols=["team", "season", "points_for", "points_against"],
            non_negative_cols=["points_for", "points_against"],
        ),
        "M4": OutputContract(
            metric_id="M4",
            description="Total games per season (played games only).",
            schema=StructType([
                StructField("season", IntegerType(), False),
                StructField("games_total", IntegerType(), False),
            ]),
            grain_keys=["season"],
            non_null_cols=["season", "games_total"],
            non_negative_cols=["games_total"],
        ),
        "M5": OutputContract(
            metric_id="M5",
            description="Regular vs playoff game counts per season (played games only).",
            schema=StructType([
                StructField("season", IntegerType(), False),
                StructField("regular_games", IntegerType(), False),
                StructField("playoff_games", IntegerType(), False),
            ]),
            grain_keys=["season"],
            non_null_cols=["season", "regular_games", "playoff_games"],
            non_negative_cols=["regular_games", "playoff_games"],
        ),
        "M6": OutputContract(
            metric_id="M6",
            description="Neutral-venue game count per season (played games only).",
            schema=StructType([
                StructField("season", IntegerType(), False),
                StructField("neutral_games", IntegerType(), False),
            ]),
            grain_keys=["season"],
            non_null_cols=["season", "neutral_games"],
            non_negative_cols=["neutral_games"],
        ),
    }


def validate_metric_df(metric_id: str, df: SparkDF) -> Dict[str, int]:
    all_contracts = contracts_v1()
    if metric_id not in all_contracts:
        _fail(f"Unknown metric_id '{metric_id}'. Expected one of {sorted(all_contracts.keys())}.")

    c = all_contracts[metric_id]
    expected_cols = [f.name for f in c.schema.fields]
    actual_cols = df.columns

    missing = [col for col in expected_cols if col not in actual_cols]
    extra = [col for col in actual_cols if col not in expected_cols]
    if missing:
        _fail(f"{metric_id} contract failed: missing columns: {missing}")
    if extra:
        _fail(f"{metric_id} contract failed: extra columns present: {extra}")

    df = df.select(*expected_cols)

    # Type check (exact simpleString match)
    expected_types = {f.name: f.dataType.simpleString() for f in c.schema.fields}
    actual_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    mismatched = {
        col: (expected_types[col], actual_types.get(col))
        for col in expected_cols
        if actual_types.get(col) != expected_types[col]
    }
    if mismatched:
        _fail(f"{metric_id} contract failed: type mismatches: {mismatched}")

    row_count = int(df.count())

    # Non-null checks
    null_counts = (
        df.agg(*[F.sum(F.col(col).isNull().cast("int")).alias(col) for col in c.non_null_cols])
        .collect()[0]
        .asDict()
    )
    bad_nulls = {k: int(v) for k, v in null_counts.items() if v and int(v) > 0}
    if bad_nulls:
        _fail(f"{metric_id} contract failed: nulls in required cols: {bad_nulls}")

    # Non-negative checks
    negative_cols = []
    for col in c.non_negative_cols:
        if df.filter(F.col(col) < 0).limit(1).count() > 0:
            negative_cols.append(col)
    if negative_cols:
        _fail(f"{metric_id} contract failed: negative values found in: {negative_cols}")

    # Grain uniqueness (no dedupe policy)
    if (
        df.groupBy(*c.grain_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        _fail(f"{metric_id} contract failed: grain key not unique for keys={c.grain_keys}")

    return {"row_count": row_count}


def validate_all_metrics(outputs: Dict[str, SparkDF]) -> Dict[str, Dict[str, int]]:
    expected = set(METRIC_IDS_V1)
    actual = set(outputs.keys())

    missing = sorted(list(expected - actual))
    extra = sorted(list(actual - expected))
    if missing:
        _fail(f"Output set contract failed: missing metrics: {missing}")
    if extra:
        _fail(f"Output set contract failed: unexpected metrics present: {extra}")

    return {mid: validate_metric_df(mid, outputs[mid]) for mid in METRIC_IDS_V1}
