"""
Detect key skew on a DataFrame before it shows up as a long-tail stage.

WHAT THIS DEMONSTRATES
    A small, reusable skew detector. Given a DataFrame and a key column,
    it reports:
      * Top-N keys and their share of total rows.
      * Distribution percentiles of rows-per-key (p50, p95, p99, max).
      * The max-to-median ratio (the single best one-number skew indicator).
      * An approximate distinct count of the key, so you can sanity-check
        shuffle partition count vs key cardinality.

WHY IT MATTERS
    AQE skew handling is reactive: it kicks in after the shuffle has
    already started. This detector is proactive: run it on the join or
    group-by key and you'll know whether to expect a long tail before
    you launch the expensive job.

    A good production pattern: run this as a guardrail before every
    aggregation/join over a date partition. If the top-1 key share
    exceeds a threshold, alert.

WHAT TO LOOK FOR IN SPARK UI
    * If this script reports a high max-to-median ratio (>50), the next
      shuffle stage on this key will show:
        - max task duration far above the 75th percentile in the Stages tab,
        - one or two tasks with disproportionate shuffle read,
        - one executor with disproportionate shuffle read in the
          Executors tab.

PHYSICAL PLAN OPERATORS THAT MATTER
    * Exchange hashpartitioning(<key>, N) -> the redistribution that will
      become hot if this script reports skew.
    * AQE adaptive node showing skew handling at runtime, when AQE is on.

PRODUCTION ISSUES THIS HELPS DIAGNOSE
    * Long-tail aggregations and joins.
    * Single-task OOM during shuffle.
    * Output file size skew when partitioning by a key with hot values.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class SkewReport:
    distinct_keys: int
    total_rows: int
    top_keys: List[tuple]
    p50: float
    p95: float
    p99: float
    max_rows: int
    max_to_median_ratio: Optional[float]


def detect_skew(df: DataFrame, key_col: str, *, top_n: int = 20) -> SkewReport:
    """Compute a skew report for a single key column."""

    if key_col not in df.columns:
        raise ValueError(f"Column '{key_col}' not found in DataFrame schema")

    counts = (
        df.groupBy(key_col)
        .agg(F.count(F.lit(1)).alias("n"))
        .cache()
    )

    total_rows = counts.agg(F.sum("n")).first()[0] or 0

    distinct_keys = counts.count()

    percentiles = (
        counts.agg(
            F.expr("percentile_approx(n, 0.50)").alias("p50"),
            F.expr("percentile_approx(n, 0.95)").alias("p95"),
            F.expr("percentile_approx(n, 0.99)").alias("p99"),
            F.max("n").alias("max_n"),
        )
        .first()
    )

    top_rows = (
        counts.orderBy(F.desc("n"))
        .limit(top_n)
        .collect()
    )
    top_keys = [(row[key_col], row["n"]) for row in top_rows]

    counts.unpersist()

    p50 = float(percentiles["p50"]) if percentiles["p50"] is not None else 0.0
    p95 = float(percentiles["p95"]) if percentiles["p95"] is not None else 0.0
    p99 = float(percentiles["p99"]) if percentiles["p99"] is not None else 0.0
    max_n = int(percentiles["max_n"] or 0)

    ratio = (max_n / p50) if p50 > 0 else None

    return SkewReport(
        distinct_keys=distinct_keys,
        total_rows=int(total_rows),
        top_keys=top_keys,
        p50=p50,
        p95=p95,
        p99=p99,
        max_rows=max_n,
        max_to_median_ratio=ratio,
    )


def classify(ratio: Optional[float]) -> str:
    """A coarse, opinionated label that's useful in alerts."""
    if ratio is None:
        return "no-data"
    if ratio < 5:
        return "balanced"
    if ratio < 50:
        return "moderate-skew"
    if ratio < 500:
        return "severe-skew"
    return "pathological-skew"


def print_report(report: SkewReport, key_col: str) -> None:
    print(f"=== Skew report for key: {key_col} ===")
    print(f"  total rows           : {report.total_rows:,}")
    print(f"  distinct keys        : {report.distinct_keys:,}")
    print(f"  rows/key p50         : {report.p50:,.0f}")
    print(f"  rows/key p95         : {report.p95:,.0f}")
    print(f"  rows/key p99         : {report.p99:,.0f}")
    print(f"  rows/key max         : {report.max_rows:,}")
    if report.max_to_median_ratio is not None:
        print(f"  max/median ratio     : {report.max_to_median_ratio:,.1f}")
    print(f"  classification       : {classify(report.max_to_median_ratio)}")
    print()
    print("  top keys:")
    total = max(report.total_rows, 1)
    for key, n in report.top_keys:
        pct = 100.0 * n / total
        print(f"    {str(key)[:40]:40s} {n:>12,}  {pct:5.2f}%")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", help="Table name (e.g. db.events).")
    parser.add_argument(
        "--input",
        help="Path to read instead of a table. Use with --format.",
    )
    parser.add_argument(
        "--format",
        default="parquet",
        help="Reader format when --input is set: parquet, csv, json, orc.",
    )
    parser.add_argument(
        "--header",
        action="store_true",
        help="For CSV: treat the first line as a header.",
    )
    parser.add_argument("--key", required=True, help="Key column to analyze.")
    parser.add_argument("--filter", help="Optional WHERE clause (without WHERE).")
    parser.add_argument("--top", "--top-n", dest="top", type=int, default=20)
    args = parser.parse_args()

    if not args.table and not args.input:
        parser.error("either --table or --input is required")

    spark = SparkSession.builder.appName("skew_detector").getOrCreate()

    if args.input:
        reader = spark.read.format(args.format)
        if args.format == "csv":
            reader = reader.option("header", "true" if args.header else "false")
            reader = reader.option("inferSchema", "true")
        df = reader.load(args.input)
    else:
        df = spark.table(args.table)

    if args.filter:
        df = df.filter(args.filter)

    report = detect_skew(df, args.key, top_n=args.top)
    print_report(report, args.key)


if __name__ == "__main__":
    main()
