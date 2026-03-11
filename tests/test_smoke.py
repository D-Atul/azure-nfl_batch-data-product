# tests/test_run_azure_batch_smoke.py

from __future__ import annotations

import json
import sys

import src.runner.run_azure_batch as runner


class FakeFS:
    def __init__(self):
        self.put_calls = []
        self.removed = []

    def exists(self, path: str) -> bool:
        return False

    def rm(self, path: str, recurse: bool = True):
        self.removed.append((path, recurse))

    def ls(self, path: str):
        return []

    def put(self, path: str, content: str, overwrite: bool = True):
        self.put_calls.append((path, content, overwrite))


class FakeMS:
    def __init__(self):
        self.fs = FakeFS()


class FakeSpark:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeBuilder:
    def __init__(self, spark):
        self.spark = spark
        self.app_name = None

    def appName(self, name: str):
        self.app_name = name
        return self

    def getOrCreate(self):
        return self.spark


class FakeSparkSession:
    def __init__(self, spark):
        self.builder = FakeBuilder(spark)


def test_runner_smoke_success(monkeypatch):
    fake_ms = FakeMS()
    fake_spark = FakeSpark()

    monkeypatch.setattr(runner, "_mssparkutils", lambda: fake_ms)
    monkeypatch.setattr(runner, "SparkSession", FakeSparkSession(fake_spark))

    def fake_ingest_validate_transform(*, spark, input_path, curated_path):
        assert spark is fake_spark
        assert input_path == "abfss://container/raw.csv"
        assert curated_path.startswith("abfss://container/curated/run_id=")
        return object(), {"rows_read_raw": 3, "rows_curated": 3}

    def fake_build_and_publish(*, spark, df_curated, metrics_base_path):
        assert spark is fake_spark
        assert metrics_base_path.startswith("abfss://container/metrics/run_id=")
        return (
            {
                "M1": {"rows_written": 2},
                "M2": {"rows_written": 2},
                "M3": {"rows_written": 2},
                "M4": {"rows_written": 1},
                "M5": {"rows_written": 1},
                "M6": {"rows_written": 1},
            },
            {mid: f"{metrics_base_path}/{mid}" for mid in ["M1", "M2", "M3", "M4", "M5", "M6"]},
        )

    monkeypatch.setattr(runner, "ingest_validate_transform", fake_ingest_validate_transform)
    monkeypatch.setattr(runner, "build_and_publish", fake_build_and_publish)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_azure_batch",
            "--input_path", "abfss://container/raw.csv",
            "--curated_base", "abfss://container/curated",
            "--metrics_base", "abfss://container/metrics",
            "--logs_base", "abfss://container/logs",
        ],
    )

    rc = runner.main()

    assert rc == 0
    assert fake_spark.stopped is True
    assert len(fake_ms.fs.put_calls) == 1

    log_path, content, overwrite = fake_ms.fs.put_calls[0]
    assert log_path.startswith("abfss://container/logs/run_id=")
    assert overwrite is True

    payload = json.loads(content)
    assert payload["input_path"] == "abfss://container/raw.csv"
    assert payload["status"] == "success"
    assert payload["rows_read_raw"] == 3
    assert payload["rows_curated"] == 3
    assert payload["per_metric_rows"] == {
        "M1": 2,
        "M2": 2,
        "M3": 2,
        "M4": 1,
        "M5": 1,
        "M6": 1,
    }
    assert "failure_reason" not in payload