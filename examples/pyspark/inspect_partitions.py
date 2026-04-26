"""
Inspect Spark execution partitions of a DataFrame.

WHAT THIS DEMONSTRATES
    How to look at the per-partition row distribution of a DataFrame, which
    is the most direct way to confirm whether a stage is balanced or skewed
    before you tune anything.

    The script:
      * Reports the number of execution partitions.
      * Reports row counts per partition.
      * Reports min / median / p95 / max rows per partition and the
        max-to-median ratio.
      * Optionally prints the physical plan so you can correlate the
        partitioning to a specific Exchange node.

WHY IT MATTERS
    "How many partitions does this DataFrame have, and how evenly is the
    data distributed across them?" is the question behind almost every
    skew, OOM, and small-files investigation. The Spark UI shows you this
    after a stage runs; this script lets you check before.

WHAT TO LOOK FOR IN SPARK UI
    * After running this against a DataFrame that feeds a downstream stage,
      the Stages tab for that downstream stage should show a task count
      equal to the partition count reported here.
    * If the per-partition row counts here are uneven (max >> median), the
      downstream stage will show a long-tail task distribution.

PHYSICAL PLAN OPERATORS THAT MATTER
    * Exchange hashpartitioning(...) -> shuffle that produced these
      partitions. Hot keys produce uneven partitions.
    * RoundRobinPartitioning -> repartition(n) without a key. Should be
      even unless data is tiny.

PRODUCTION ISSUES THIS HELPS DIAGNOSE
    * "Why is one task always slow?"
    * "Why did my repartition(N) before write produce N skewed files?"
    * "Why does my groupBy aggregation OOM on one executor?"
"""

from __future__ import annotations

import argparse
import statistics
from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def partition_row_counts(df: DataFrame) -> List[int]:
    """Return the row count per execution partition, in partition order."""

    def count_rows(rows: Iterable) -> Iterable[int]:
        n = 0
        for _ in rows:
            n += 1
        yield n

    return df.rdd.mapPartitions(count_rows).collect()


def summarize(counts: List[int]) -> dict:
    if not counts:
        return {"partitions": 0}

    sorted_counts = sorted(counts)
    median = statistics.median(sorted_counts)
    p95_index = max(0, int(len(sorted_counts) * 0.95) - 1)

    return {
        "partitions": len(counts),
        "total_rows": sum(counts),
        "min": sorted_counts[0],
        "median": median,
        "p95": sorted_counts[p95_index],
        "max": sorted_counts[-1],
        "max_to_median_ratio": (sorted_counts[-1] / median) if median > 0 else None,
        "empty_partitions": sum(1 for c in counts if c == 0),
    }


def inspect(df: DataFrame, *, top_n: int = 10, show_plan: bool = True) -> None:
    if show_plan:
        print("=== Physical plan ===")
        df.explain(mode="formatted")
        print()

    counts = partition_row_counts(df)
    summary = summarize(counts)

    print("=== Partition summary ===")
    for key, value in summary.items():
        print(f"  {key:24s}: {value}")
    print()

    print(f"=== Top {top_n} partitions by row count ===")
    indexed = sorted(enumerate(counts), key=lambda kv: kv[1], reverse=True)[:top_n]
    for partition_id, n in indexed:
        print(f"  partition {partition_id:6d}: {n:>12,} rows")


def _example_df(spark: SparkSession) -> DataFrame:
    """A small synthetic DataFrame with deliberate skew, for offline demos."""
    return (
        spark.range(0, 1_000_000)
        .withColumn(
            "customer_id",
            F.when(F.col("id") % 1000 == 0, F.lit("HOT_KEY"))  # ~0.1% rows -> hot
            .otherwise(F.concat(F.lit("c_"), (F.col("id") % 5000).cast("string"))),
        )
        .withColumn("order_total", (F.col("id") % 977).cast("double"))
        .repartition(200, "customer_id")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", help="Table name to inspect (e.g. db.events).")
    parser.add_argument("--filter", help="Optional WHERE clause (without the WHERE).")
    parser.add_argument("--top", type=int, default=10, help="Top-N partitions to print.")
    parser.add_argument(
        "--no-plan", action="store_true", help="Skip printing the physical plan."
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("inspect_partitions").getOrCreate()

    if args.table:
        df = spark.table(args.table)
        if args.filter:
            df = df.filter(args.filter)
    else:
        df = _example_df(spark)

    inspect(df, top_n=args.top, show_plan=not args.no_plan)


if __name__ == "__main__":
    main()
