#!/usr/bin/env python3
"""Show Spark partition count before/after `repartition` and `coalesce` (no shuffle vs full shuffle)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("repartition-vs-coalesce")
        .master(os.environ.get("SPARK_LOCAL", "local[*]"))
        .getOrCreate()
    )
    try:
        df = spark.range(0, 1000, 1, 1).toDF("id")
        print("initial partitions", df.rdd.getNumPartitions())
        r4 = df.repartition(4)
        print("after repartition(4)", r4.rdd.getNumPartitions())
        c2 = r4.coalesce(2)
        print("after coalesce(2) from 4", c2.rdd.getNumPartitions())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
