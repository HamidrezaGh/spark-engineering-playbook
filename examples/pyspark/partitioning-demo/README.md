# Partitioning and output file count (PySpark)

## How to run

```bash
export SPARK_LOCAL=local[2]   # optional
python3 examples/pyspark/partitioning-demo/repartition_vs_coalesce.py
python3 examples/pyspark/partitioning-demo/output_file_count_demo.py
```

## What to observe

- **`repartition_vs_coalesce.py`** — `repartition` increases partitions (full shuffle in real
  data); `coalesce` can **reduce** without shuffle when `new < old` (narrow only when Spark can
  pipeline — here from 4 → 2).
- **`output_file_count_demo.py`** — same row count, different **file** count in Parquet output.
  `coalesce(1)` is not always “bad,” but for large data it creates one huge task and one file.

**Production lesson:** match output **partitions** to a **file size** target, not a magic
number. Too many **tiny** files hurt reads; one **giant** file under-uses parallelism and risks
OOM on a single task.

**Common mistake:** using `repartition(200)` before every `write` “because 200 is the default
shuffle partitions” without measuring bytes per file.

**See:** [`../../../docs/book/03-partitioning.md`](../../../docs/book/03-partitioning.md), [`../../../docs/troubleshooting/small-files.md`](../../../docs/troubleshooting/small-files.md)

## Sample output

[`sample_output.md`](sample_output.md)
