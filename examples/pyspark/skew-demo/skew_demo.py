#!/usr/bin/env python3
"""Skew demo: show uneven key distribution before an expensive shuffle."""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# `skew_detector` lives in the parent `examples/pyspark` directory
_PYSPARK_EXAMPLES = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PYSPARK_EXAMPLES not in sys.path:
    sys.path.insert(0, _PYSPARK_EXAMPLES)

from skew_detector import detect_skew, print_report  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Skew demo on customer_id (local sample CSV).")
    p.add_argument(
        "--input",
        default=os.path.normpath(
            os.path.join(_PYSPARK_EXAMPLES, "..", "local", "data", "events_sample.csv")
        ),
        help="Path to events CSV",
    )
    args = p.parse_args()

    spark = (
        SparkSession.builder.appName("skew-demo")
        .master(os.environ.get("SPARK_LOCAL", "local[*]"))
        .getOrCreate()
    )
    try:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(args.input)
        )
        # Artificially skew: duplicate one customer to simulate a hot key
        hot = df.filter(F.col("customer_id") == "cust_001")
        skewed = df.unionByName(hot).unionByName(hot)
        rep = detect_skew(skewed, "customer_id", top_n=5)
        print_report(rep, key_col="customer_id")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
