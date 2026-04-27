#!/usr/bin/env python3
"""Write the same small dataset with different `coalesce` values and compare output part-file count."""

from __future__ import annotations

import os
import shutil
import tempfile

from pyspark.sql import SparkSession


def count_parquet_files(path: str) -> int:
    n = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet") and not f.startswith("_"):
                n += 1
    return n


def main() -> None:
    base = tempfile.mkdtemp(prefix="spark-out-")
    try:
        spark = (
            SparkSession.builder.appName("output-file-count")
            .master(os.environ.get("SPARK_LOCAL", "local[*]"))
            .getOrCreate()
        )
        try:
            df = spark.range(0, 2000, 1, 20).toDF("id")
            for n, name in ((8, "eight-parts"), (1, "one-part")):
                out = os.path.join(base, name)
                df.coalesce(n).write.mode("overwrite").parquet(out)
                c = count_parquet_files(out)
                print(f"coalesce({n}) -> {c} data parquet files under {out}")
        finally:
            spark.stop()
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
