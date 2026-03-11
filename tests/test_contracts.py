from __future__ import annotations

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.contracts.input_contract import (
    ContractViolation as InputContractViolation,
    raw_events_expected_columns,
    validate_raw_events_spark,
)
from src.contracts.output_contracts import (
    ContractViolation as OutputContractViolation,
    METRIC_IDS_V1,
    validate_all_metrics,
    validate_metric_df,
)


RAW_SCHEMA = StructType([
    StructField("schedule_date", StringType(), False),
    StructField("schedule_season", IntegerType(), False),
    StructField("schedule_week", StringType(), False),
    StructField("schedule_playoff", BooleanType(), False),
    StructField("team_home", StringType(), False),
    StructField("score_home", DoubleType(), True),
    StructField("score_away", DoubleType(), True),
    StructField("team_away", StringType(), False),
    StructField("team_favorite_id", StringType(), True),
    StructField("spread_favorite", DoubleType(), True),
    StructField("over_under_line", StringType(), True),
    StructField("stadium", StringType(), True),
    StructField("stadium_neutral", BooleanType(), True),
    StructField("weather_temperature", DoubleType(), True),
    StructField("weather_wind_mph", DoubleType(), True),
    StructField("weather_humidity", DoubleType(), True),
    StructField("weather_detail", StringType(), True),
])


def _raw_rows():
    return [
        {
            "schedule_date": "2020-09-10",
            "schedule_season": 2020,
            "schedule_week": "1",
            "schedule_playoff": False,
            "team_home": " Chiefs ",
            "score_home": 34.0,
            "score_away": 20.0,
            "team_away": "Texans ",
            "team_favorite_id": "KC",
            "spread_favorite": -9.5,
            "over_under_line": "54.5",
            "stadium": " Arrowhead ",
            "stadium_neutral": False,
            "weather_temperature": 70.0,
            "weather_wind_mph": 5.0,
            "weather_humidity": 55.0,
            "weather_detail": "Clear",
        }
    ]


def test_validate_raw_events_accepts_valid_dataframe_and_enforces_column_order(spark):
    df = spark.createDataFrame(_raw_rows(), schema=RAW_SCHEMA)

    validated = validate_raw_events_spark(df)

    assert validated.columns == raw_events_expected_columns()
    assert validated.count() == 1


def test_validate_raw_events_rejects_one_sided_scores(spark):
    rows = _raw_rows()
    rows[0]["score_away"] = None
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)

    with pytest.raises(InputContractViolation, match="inconsistent scores"):
        validate_raw_events_spark(df)


def test_validate_raw_events_rejects_duplicate_natural_keys(spark):
    rows = _raw_rows() * 2
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)

    with pytest.raises(InputContractViolation, match="duplicate game keys"):
        validate_raw_events_spark(df)


def test_validate_metric_df_accepts_valid_m1_output(spark):
    df = spark.createDataFrame(
        [("Chiefs", 2020, 1)],
        schema="team string, season int, games_played int",
    )

    stats = validate_metric_df("M1", df)

    assert stats == {"row_count": 1}


def test_validate_metric_df_rejects_negative_values(spark):
    df = spark.createDataFrame(
        [("Chiefs", 2020, -1)],
        schema="team string, season int, games_played int",
    )

    with pytest.raises(OutputContractViolation, match="negative values found"):
        validate_metric_df("M1", df)


def test_validate_all_metrics_rejects_missing_metric(spark):
    outputs = {
        "M1": spark.createDataFrame(
            [("Chiefs", 2020, 1)],
            "team string, season int, games_played int",
        ),
        "M2": spark.createDataFrame(
            [("Chiefs", 2020, 1, 0, 0)],
            "team string, season int, wins int, losses int, ties int",
        ),
        "M3": spark.createDataFrame(
            [("Chiefs", 2020, 34, 20)],
            "team string, season int, points_for int, points_against int",
        ),
        "M4": spark.createDataFrame(
            [(2020, 1)],
            "season int, games_total int",
        ),
        "M5": spark.createDataFrame(
            [(2020, 1, 0)],
            "season int, regular_games int, playoff_games int",
        ),
        # M6 intentionally omitted
    }

    with pytest.raises(OutputContractViolation, match="missing metrics"):
        validate_all_metrics(outputs)

    assert sorted(METRIC_IDS_V1) == ["M1", "M2", "M3", "M4", "M5", "M6"]