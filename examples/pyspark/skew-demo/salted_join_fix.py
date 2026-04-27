#!/usr/bin/env python3
"""
Skewed join with salting (toy data).

Pattern: the **right** side of the join is replicated per **salt bucket**; the **left** side
carries a matching `(key, salt)` so work spreads across more tasks than a single hot key.
Tradeoff: more rows to shuffle; only worth it if skew dominates runtime.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def main() -> None:
    spark = (
        SparkSession.builder.appName("salted-join-demo")
        .master(os.environ.get("SPARK_LOCAL", "local[*]"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        left = spark.createDataFrame(
            [("A", 1), ("A", 2), ("A", 3), ("A", 4), ("B", 1), ("C", 1)],
            ["k", "v"],
        )
        right = spark.createDataFrame(
            [("A", 10), ("B", 20), ("C", 30)],
            ["k", "w"],
        )
        n_salt = 4
        # Left: for each row, pick a salt bucket 0..n_salt-1 (deterministic for demo)
        left_s = left.withColumn("salt", F.pmod(F.hash("k", "v"), F.lit(n_salt))).withColumn(
            "join_key", F.concat_ws(":", F.col("k"), F.col("salt").cast("string"))
        )
        # Right: copy each right row n_salt times with matching join_key
        buckets = ", ".join(str(i) for i in range(n_salt))
        right_s = (
            right.withColumn("salt", F.explode(F.expr(f"array({buckets})")))
            .withColumn("join_key", F.concat_ws(":", F.col("k"), F.col("salt").cast("string")))
        )

        joined = left_s.join(broadcast(right_s), on="join_key", how="inner").select("k", "v", "w")

        print("=== EXPLAIN (salted, broadcast right) ===")
        joined.explain("formatted")
        print("=== row count ===", joined.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
