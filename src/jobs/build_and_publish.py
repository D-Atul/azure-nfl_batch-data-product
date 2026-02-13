from __future__ import annotations

from typing import Dict, Tuple, Any

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.contracts.output_contracts import validate_all_metrics

EXPECTED_METRICS = ("M1", "M2", "M3", "M4", "M5", "M6")


def build_and_publish(
    spark,
    df_curated: SparkDF,
    metrics_base_path: str,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Azure batch v1:
    - build M1..M6 from curated events
    - validate outputs (schema/grain)
    - write each metric to metrics_base_path/<Mi>

    Rule: if anything fails, this function leaves no partial metrics behind.
    (Runner also enforces this, but the job is self-cleaning.)
    """
    base = metrics_base_path.rstrip("/")

    def _rm_tree_best_effort(path: str) -> None:
        # Spark/Hadoop delete works for ADLS paths in Synapse.
        try:
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).delete(spark._jvm.org.apache.hadoop.fs.Path(path), True)
        except Exception:
            # Best-effort only; runner will also cleanup on failure.
            pass

    g = df_curated.select(
        F.col("schedule_season").alias("season"),
        F.col("schedule_playoff").alias("is_playoff"),
        F.col("stadium_neutral").alias("is_neutral"),
        F.col("team_home").alias("home_team"),
        F.col("team_away").alias("away_team"),
        F.col("score_home").alias("home_score"),
        F.col("score_away").alias("away_score"),
    )

    # Score-dependent metrics use played games only (both scores present).
    played = g.filter(F.col("home_score").isNotNull() & F.col("away_score").isNotNull())

    # Keep results stable: reject fractional scores before casting
    if (
        played.filter(
            (F.col("home_score") != F.floor(F.col("home_score")))
            | (F.col("away_score") != F.floor(F.col("away_score")))
        )
        .limit(1)
        .count()
        > 0
    ):
        raise RuntimeError("Scores contain fractional values; refusing to cast to int.")

    played_i = (
        played.withColumn("home_score_i", F.col("home_score").cast(T.IntegerType()))
        .withColumn("away_score_i", F.col("away_score").cast(T.IntegerType()))
    )

    # Season-level metrics (played games)
    m4 = (
        played.groupBy("season")
        .agg(F.count(F.lit(1)).cast("int").alias("games_total"))
        .orderBy("season")
    )

    m5 = (
        played.groupBy("season")
        .agg(
            F.sum(F.when(F.col("is_playoff") == F.lit(True), 1).otherwise(0)).cast("int").alias("playoff_games"),
            F.sum(F.when(F.col("is_playoff") == F.lit(False), 1).otherwise(0)).cast("int").alias("regular_games"),
        )
        .select("season", "regular_games", "playoff_games")
        .orderBy("season")
    )

    m6 = (
        played.groupBy("season")
        .agg(F.sum(F.when(F.col("is_neutral") == F.lit(True), 1).otherwise(0)).cast("int").alias("neutral_games"))
        .orderBy("season")
    )

    # Team expansion for team metrics (played games)
    home_rows = played_i.select(
        "season",
        F.col("home_team").alias("team"),
        F.col("home_score_i").alias("points_for"),
        F.col("away_score_i").alias("points_against"),
        F.when(F.col("home_score_i") > F.col("away_score_i"), F.lit("W"))
        .when(F.col("home_score_i") < F.col("away_score_i"), F.lit("L"))
        .otherwise(F.lit("T"))
        .alias("result"),
    )

    away_rows = played_i.select(
        "season",
        F.col("away_team").alias("team"),
        F.col("away_score_i").alias("points_for"),
        F.col("home_score_i").alias("points_against"),
        F.when(F.col("away_score_i") > F.col("home_score_i"), F.lit("W"))
        .when(F.col("away_score_i") < F.col("home_score_i"), F.lit("L"))
        .otherwise(F.lit("T"))
        .alias("result"),
    )

    team_games = home_rows.unionByName(away_rows)

    m1 = (
        team_games.groupBy("team", "season")
        .agg(F.count(F.lit(1)).cast("int").alias("games_played"))
        .orderBy("season", "team")
    )

    m2 = (
        team_games.groupBy("team", "season")
        .agg(
            F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).cast("int").alias("wins"),
            F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).cast("int").alias("losses"),
            F.sum(F.when(F.col("result") == "T", 1).otherwise(0)).cast("int").alias("ties"),
        )
        .orderBy("season", "team")
    )

    m3 = (
        team_games.groupBy("team", "season")
        .agg(
            F.sum(F.col("points_for")).cast("int").alias("points_for"),
            F.sum(F.col("points_against")).cast("int").alias("points_against"),
        )
        .orderBy("season", "team")
    )

    outputs: Dict[str, SparkDF] = {"M1": m1, "M2": m2, "M3": m3, "M4": m4, "M5": m5, "M6": m6}

    if tuple(sorted(outputs.keys())) != tuple(sorted(EXPECTED_METRICS)):
        raise RuntimeError(f"Metric set mismatch. Expected {EXPECTED_METRICS}, got {tuple(sorted(outputs.keys()))}")

    # Validate before any write
    validate_all_metrics(outputs)

    output_paths: Dict[str, str] = {}
    out_stats: Dict[str, Any] = {}

    # Evidence stats for exclusions (computed once)
    total_rows = int(g.count())
    played_rows = int(played.count())

    try:
        for metric_id in EXPECTED_METRICS:
            df = outputs[metric_id]
            path = f"{base}/{metric_id}"

            # No overwrites in a run-scoped folder
            df.write.mode("errorifexists").parquet(path)

            rows_written = int(df.count())
            output_paths[metric_id] = path
            out_stats[metric_id] = {"rows_written": rows_written}

    except Exception:
        # Framework rule: no partial publish on failure.
        _rm_tree_best_effort(base)
        raise

    out_stats["metrics_input"] = {
        "total_rows": total_rows,
        "played_rows": played_rows,
        "excluded_missing_scores": total_rows - played_rows,
    }

    return out_stats, output_paths
