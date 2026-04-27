# Troubleshooting: small output files

**Problem:** The write produces thousands of tiny files, S3 costs explode, or downstream reads are slow from metadata load.

## Symptoms

- Output directory with **huge file count** and small average size (MB or KB per file when GB+ is target).
- **ListObjects** and Spark **planning** time increase over time.
- “Works in dev, slow in prod” when prod has more **shuffle partitions** or more executors.

## What to check first

1. **How many tasks wrote?** ≈ file count for Parquet/CSV if one file per task (common). Map **output task count** to `coalesce` / `repartition` / `shuffle.partitions` before `write`.
2. **Partition columns** — dynamic insert can multiply directories × files.
3. **Idempotent or merge write** — some paths write extra files per attempt; check Iceberg commit history.

## Spark UI signals

- Last **stage** has **many succeeded tasks** with small **Output** records/bytes each.
- **Input** to the write is already split into thousands of tiny tasks — trace upstream to `repartition(10000)` or high default shuffle partitions.

## Logs and metrics

- File count and average size: shell / `aws s3 ls`, Iceberg `files` metadata, or a small PySpark count script — see [`examples/pyspark/file_count_audit.py`](../../examples/pyspark/file_count_audit.py) and [partitioning-demo](../../examples/pyspark/partitioning-demo/README.md).

## Likely causes

- **`repartition` default** from a wide transformation → thousands of **shuffle** partitions into **write** tasks.
- **Executor count × partitions** — each run creates `partitions` files unless coalesced.
- **Dynamic partition insert** with high-cardinality key → many output partitions each with N files.
- **Streaming** checkpoint + trigger producing micro-files.

## Fix options

- **Coalesce** or **repartition to a target number** (or target bytes where supported) *before* write — balance against task size and skew.
- **AQE** `coalescing` and shuffle partition sizing — reduce count when data is small.
- **Table-level compaction** (Iceberg `rewrite_data_files`, Delta `OPTIMIZE`) for files already written.
- **Layout:** partition table by a low- or medium-cardinality column; avoid over-partitioning.
- For **incremental** pipelines: **merge** and scheduled compaction instead of raw append of micro-batches.

## Tradeoffs

- `coalesce(1)`**:** single file, single task — **skew** and OOM risk; bad for large data.
- **Too few files** — under-parallel read on some engines; under-utilizes cluster on write.
- **Compaction** — extra job cost and write amplification; must be budgeted.

## Example final diagnosis

*Symptoms:* 40k Parquet files for a 200 GB table. **Cause:** `spark.sql.shuffle.partitions=8000` with full-cluster parallelism and no `coalesce` before `write`. **Fix:** `repartition(400)` by `dt` before write; average file size near target. **Ongoing:** weekly Iceberg `rewrite_data_files` for stragglers.

## Prevention checklist

- [ ] Target **output file size** (e.g. 128–512 MB) documented per pipeline class.
- [ ] `coalesce`/`repartition` before write in the golden path for batch writes.
- [ ] Alert on **file count** and **list time** in the output prefix.
- [ ] Streaming: appropriate **trigger** and **repartition** before table write.

**See also:** [`../book/17-spark-write-path-and-output-files.md`](../book/17-spark-write-path-and-output-files.md), [`../field-guides/small-files-playbook.md`](../field-guides/small-files-playbook.md).
