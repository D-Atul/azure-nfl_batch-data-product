from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
)


def raw_events_spark_schema() -> StructType:
    """
    Strict read-time schema for spreadspoke_scores.csv.

    Note: score fields are nullable on purpose because the dataset includes
    scheduled/future games. Validation enforces that scores are either:
      - both present (played game), or
      - both null (scheduled game).
    """
    return StructType(
        [
            StructField("schedule_date", StringType(), False),
            StructField("schedule_season", IntegerType(), False),
            StructField("schedule_week", StringType(), True),
            StructField("schedule_playoff", BooleanType(), False),
            StructField("team_home", StringType(), False),
            StructField("score_home", DoubleType(), True),
            StructField("score_away", DoubleType(), True),
            StructField("team_away", StringType(), False),
            StructField("team_favorite_id", StringType(), True),
            StructField("spread_favorite", DoubleType(), True),
            StructField("over_under_line", StringType(), True),
            StructField("stadium", StringType(), False),
            StructField("stadium_neutral", BooleanType(), False),
            StructField("weather_temperature", DoubleType(), True),
            StructField("weather_wind_mph", DoubleType(), True),
            StructField("weather_humidity", DoubleType(), True),
            StructField("weather_detail", StringType(), True),
        ]
    )


def raw_events_expected_columns() -> List[str]:
    return [f.name for f in raw_events_spark_schema().fields]


class ContractViolation(Exception):
    """Raised when input data violates the declared contract."""


def validate_raw_events_spark(df: SparkDF) -> SparkDF:
    """
    Fail-fast validation for raw events.

    Rules:
    - strict columns (no missing, no extras)
    - required fields non-null
    - score consistency: both present OR both null (never one-sided)
    - basic domain sanity
    - duplicate natural keys are a hard failure (we do NOT dedupe)
    """
    expected = raw_events_expected_columns()
    actual = df.columns

    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    if missing:
        raise ContractViolation(f"Input contract failed: missing columns: {missing}")
    if extra:
        raise ContractViolation(f"Input contract failed: extra columns present: {extra}")

    # Canonical column order (helps deterministic downstream behavior)
    df = df.select(*expected)

    required_non_null = [
        "schedule_date",
        "schedule_season",
        "schedule_playoff",
        "team_home",
        "team_away",
        "stadium",
        "stadium_neutral",
    ]

    null_counts = (
        df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required_non_null])
        .collect()[0]
        .asDict()
    )

    bad_nulls = {k: v for k, v in null_counts.items() if v and v > 0}
    if bad_nulls:
        raise ContractViolation(f"Input contract failed: nulls in required fields: {bad_nulls}")

    # Scores must be consistent:
    # - played game: both scores present
    # - scheduled game: both scores null
    one_sided_score = (
        (F.col("score_home").isNull() & F.col("score_away").isNotNull())
        | (F.col("score_home").isNotNull() & F.col("score_away").isNull())
    )
    if df.filter(one_sided_score).limit(1).count() > 0:
        raise ContractViolation(
            "Input contract failed: inconsistent scores (one of score_home/score_away is null, the other is not)."
        )

    if df.filter(F.col("schedule_season") < 1900).limit(1).count() > 0:
        raise ContractViolation("Input contract failed: schedule_season contains values < 1900.")

    # Natural key uniqueness: fail fast (no dedupe policy)
    key_cols = ["schedule_season", "schedule_date", "team_home", "team_away"]
    dup_exists = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )
    if dup_exists:
        raise ContractViolation(f"Input contract failed: duplicate game keys detected for {key_cols}.")

    return df
