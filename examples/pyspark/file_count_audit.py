"""
Audit output file count and average file size for a Spark-written dataset.

WHAT THIS DEMONSTRATES
    A small auditor that walks a path (S3 or local) and reports:
      * Total number of data files.
      * Total size in bytes.
      * Average / median / p95 / max file size.
      * Number of files smaller than a "small file" threshold.
      * Per-partition (top level) file count and total size, when the
        layout uses Hive-style partitioning (e.g. event_date=2026-04-25/).

WHY IT MATTERS
    Small files are one of the most expensive long-running problems in a
    data platform: scan planning, S3 listing cost, executor task overhead,
    and memory pressure all scale with file count. Most "the job got slow
    over time" tickets are small-file tickets.

    On the other end, oversized files (1 file / 10 GB) prevent parallelism
    on read and tend to OOM single tasks during downstream wide
    transformations.

    A healthy production target on Parquet for analytics is roughly
    128-512 MB per file, with no partition having only a handful of tiny
    files unless that partition is genuinely small.

WHAT TO LOOK FOR IN SPARK UI
    * Stages tab on a downstream scan: a huge task count with each task
      reading a few MB is the small-file signature.
    * Spark UI Storage / Environment will show
      spark.sql.files.maxPartitionBytes (default 128 MiB), which is the
      target read partition size; small files are coalesced up to this
      size, large files are split below it.

PHYSICAL PLAN OPERATORS THAT MATTER
    * FileScan parquet ... -> the scan node. Its task count is driven by
      file count, file sizes, and maxPartitionBytes.

PRODUCTION ISSUES THIS HELPS DIAGNOSE
    * "Listing this prefix takes 4 minutes" -> small files / too many
      partitions.
    * "Why did my last write produce 20,000 files?" -> shuffle partition
      count or partitionBy column choice.
    * "S3 cost is going up" -> request count from listings + opens.

USAGE NOTES
    * For S3, run with the appropriate AWS credentials. The script uses the
      Hadoop FileSystem via Spark, so paths like s3://bucket/key and
      s3a://bucket/key both work as long as the cluster's hadoop-aws JARs
      are present (true by default on EMR).
    * For very large prefixes (millions of files) this script is itself a
      listing job and will be slow; that is informative on its own.
"""

from __future__ import annotations

import argparse
import statistics
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import SparkSession


SMALL_FILE_THRESHOLD_DEFAULT = 32 * 1024 * 1024  # 32 MiB


@dataclass
class FileInfo:
    path: str
    size: int
    partition: Optional[str]


def list_files(spark: SparkSession, root_path: str) -> List[FileInfo]:
    """List data files under root_path using the Hadoop FileSystem API.

    Skips hidden files (starting with _ or .) and Spark/Hive metadata files
    like _SUCCESS or _committed_*.
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    root = Path(root_path)
    fs = FileSystem.get(root.toUri(), hadoop_conf)

    results: List[FileInfo] = []

    def is_data_file(name: str) -> bool:
        return not (name.startswith("_") or name.startswith("."))

    def partition_label(parent: str, root: str) -> Optional[str]:
        if not parent.startswith(root):
            return None
        rel = parent[len(root):].strip("/")
        return rel or None

    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        path = status.getPath()
        name = path.getName()
        if not is_data_file(name):
            continue
        parent = path.getParent().toString()
        label = partition_label(parent, root_path.rstrip("/"))
        results.append(
            FileInfo(
                path=path.toString(),
                size=int(status.getLen()),
                partition=label,
            )
        )
    return results


def summarize(files: List[FileInfo], small_threshold: int) -> dict:
    if not files:
        return {"file_count": 0}

    sizes = sorted(f.size for f in files)
    p95_index = max(0, int(len(sizes) * 0.95) - 1)

    return {
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "min_bytes": sizes[0],
        "median_bytes": int(statistics.median(sizes)),
        "p95_bytes": sizes[p95_index],
        "max_bytes": sizes[-1],
        "avg_bytes": int(sum(sizes) / len(sizes)),
        "small_files": sum(1 for s in sizes if s < small_threshold),
        "small_files_pct": 100.0 * sum(1 for s in sizes if s < small_threshold) / len(sizes),
    }


def per_partition_summary(files: List[FileInfo]) -> List[dict]:
    by_partition: dict = {}
    for f in files:
        key = f.partition or "<root>"
        bucket = by_partition.setdefault(key, {"file_count": 0, "total_bytes": 0})
        bucket["file_count"] += 1
        bucket["total_bytes"] += f.size

    rows = [
        {
            "partition": key,
            "file_count": v["file_count"],
            "total_bytes": v["total_bytes"],
            "avg_bytes": int(v["total_bytes"] / v["file_count"]),
        }
        for key, v in by_partition.items()
    ]
    rows.sort(key=lambda r: r["file_count"], reverse=True)
    return rows


def fmt_bytes(n: int) -> str:
    units = [("TiB", 1024**4), ("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)]
    for label, factor in units:
        if n >= factor:
            return f"{n / factor:,.2f} {label}"
    return f"{n} B"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="Root path to audit (s3://... or local).")
    parser.add_argument(
        "--small-bytes",
        type=int,
        default=SMALL_FILE_THRESHOLD_DEFAULT,
        help="Threshold in bytes for the 'small file' label (default 32 MiB).",
    )
    parser.add_argument(
        "--top-partitions",
        type=int,
        default=20,
        help="How many top partitions (by file count) to print.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("file_count_audit").getOrCreate()

    files = list_files(spark, args.path)
    summary = summarize(files, args.small_bytes)

    print(f"=== File audit for {args.path} ===")
    if summary.get("file_count", 0) == 0:
        print("  no data files found")
        return

    print(f"  file_count       : {summary['file_count']:,}")
    print(f"  total_size       : {fmt_bytes(summary['total_bytes'])}")
    print(f"  min file size    : {fmt_bytes(summary['min_bytes'])}")
    print(f"  median file size : {fmt_bytes(summary['median_bytes'])}")
    print(f"  p95 file size    : {fmt_bytes(summary['p95_bytes'])}")
    print(f"  max file size    : {fmt_bytes(summary['max_bytes'])}")
    print(f"  avg file size    : {fmt_bytes(summary['avg_bytes'])}")
    print(
        f"  small files (<{fmt_bytes(args.small_bytes)}): "
        f"{summary['small_files']:,} ({summary['small_files_pct']:.1f}%)"
    )
    print()

    rows = per_partition_summary(files)
    print(f"=== Top {args.top_partitions} partitions by file count ===")
    print(f"  {'partition':60s} {'files':>10s}  {'size':>12s}  {'avg':>10s}")
    for row in rows[: args.top_partitions]:
        print(
            f"  {row['partition'][:60]:60s} {row['file_count']:>10,}  "
            f"{fmt_bytes(row['total_bytes']):>12s}  {fmt_bytes(row['avg_bytes']):>10s}"
        )


if __name__ == "__main__":
    main()
