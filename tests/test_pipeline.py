# tests/test_ingest_build_integration.py

from __future__ import annotations

import csv
from pathlib import Path

from src.jobs.build_and_publish import build_and_publish
from src.jobs.ingest_validate_transform import ingest_validate_transform


def test_ingest_then_build_and_publish_end_to_end(spark, tmp_path: Path):
    input_csv = tmp_path / "spreadspoke_scores.csv"
    curated_path = tmp_path / "curated"
    metrics_base = tmp_path / "metrics"

    rows = [
        {
            "schedule_date": "2020-09-10",
            "schedule_season": 2020,
            "schedule_week": "1",
            "schedule_playoff": "false",
            "team_home": " Team A ",
            "score_home": 10,
            "score_away": 7,
            "team_away": " Team B ",
            "team_favorite_id": "A",
            "spread_favorite": -3.5,
            "over_under_line": "42.5",
            "stadium": " Stadium One ",
            "stadium_neutral": "false",
            "weather_temperature": 70,
            "weather_wind_mph": 5,
            "weather_humidity": 50,
            "weather_detail": "Clear",
        },
        {
            "schedule_date": "2020-09-17",
            "schedule_season": 2020,
            "schedule_week": "2",
            "schedule_playoff": "true",
            "team_home": "Team B",
            "score_home": 3,
            "score_away": 3,
            "team_away": "Team A",
            "team_favorite_id": "B",
            "spread_favorite": -1.5,
            "over_under_line": "39.5",
            "stadium": "Stadium Two",
            "stadium_neutral": "true",
            "weather_temperature": 60,
            "weather_wind_mph": 8,
            "weather_humidity": 45,
            "weather_detail": "Cloudy",
        },
        {
            "schedule_date": "2020-09-24",
            "schedule_season": 2020,
            "schedule_week": "3",
            "schedule_playoff": "false",
            "team_home": "Team C",
            "score_home": "",
            "score_away": "",
            "team_away": "Team D",
            "team_favorite_id": "",
            "spread_favorite": "",
            "over_under_line": "",
            "stadium": "Stadium Three",
            "stadium_neutral": "false",
            "weather_temperature": "",
            "weather_wind_mph": "",
            "weather_humidity": "",
            "weather_detail": "",
        },
    ]

    with input_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    df_curated, ingest_stats = ingest_validate_transform(
        spark=spark,
        input_path=str(input_csv),
        curated_path=str(curated_path),
    )

    assert ingest_stats["rows_read_raw"] == 3
    assert ingest_stats["rows_curated"] == 3
    assert df_curated.count() == 3

    out_stats, output_paths = build_and_publish(
        spark=spark,
        df_curated=df_curated,
        metrics_base_path=str(metrics_base),
    )

    assert out_stats["metrics_input"] == {
        "total_rows": 3,
        "played_rows": 2,
        "excluded_missing_scores": 1,
    }
    assert sorted(output_paths.keys()) == ["M1", "M2", "M3", "M4", "M5", "M6"]

    m1 = spark.read.parquet(output_paths["M1"]).orderBy("team").collect()
    assert [(r["team"], r["season"], r["games_played"]) for r in m1] == [
        ("Team A", 2020, 2),
        ("Team B", 2020, 2),
    ]

    m2 = spark.read.parquet(output_paths["M2"]).orderBy("team").collect()
    assert [(r["team"], r["wins"], r["losses"], r["ties"]) for r in m2] == [
        ("Team A", 1, 0, 1),
        ("Team B", 0, 1, 1),
    ]

    m3 = spark.read.parquet(output_paths["M3"]).orderBy("team").collect()
    assert [(r["team"], r["points_for"], r["points_against"]) for r in m3] == [
        ("Team A", 13, 10),
        ("Team B", 10, 13),
    ]

    m4 = spark.read.parquet(output_paths["M4"]).collect()
    assert [(r["season"], r["games_total"]) for r in m4] == [(2020, 2)]

    m5 = spark.read.parquet(output_paths["M5"]).collect()
    assert [(r["season"], r["regular_games"], r["playoff_games"]) for r in m5] == [(2020, 1, 1)]

    m6 = spark.read.parquet(output_paths["M6"]).collect()
    assert [(r["season"], r["neutral_games"]) for r in m6] == [(2020, 1)]