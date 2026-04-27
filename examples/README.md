# Examples

Short, annotated examples that support the handbook. Each example is paired with the chapters it
illustrates and explains, in comments, what to look for in the Spark UI and which production issue
it helps diagnose.

These are not benchmarks or end-to-end pipelines. They are the kind of small diagnostic snippets
you keep in a scratchpad and paste into a notebook during incident triage.

## Directories

| Directory | Contents |
| --- | --- |
| [`sql/`](sql/README.md) | Spark SQL: `EXPLAIN`, join strategy, window vs `GROUP BY`, pruning, Iceberg. |
| [`sql/join-strategies/`](sql/join-strategies/README.md) | Focused **broadcast** vs **sort-merge** `EXPLAIN` pair. |
| [`sql/iceberg-write-path/`](sql/iceberg-write-path/README.md) | Iceberg **MERGE** / compaction **templates** (needs Iceberg runtime). |
| [`pyspark/`](pyspark/README.md) | Diagnostic scripts: partition inspector, skew detector, file audit. |
| [`pyspark/skew-demo/`](pyspark/skew-demo/README.md) | Skewed join / salting **demos** on toy or sample data. |
| [`pyspark/partitioning-demo/`](pyspark/partitioning-demo/README.md) | `repartition` vs `coalesce`, output file counts. |
| [`local/`](local/README.md) | Runnable harness with sample CSVs and a driver script for the SQL and PySpark examples. |
| [`configs/`](configs/README.md) | Spark configuration examples annotated with the workload shape they target. |

## Reading order (paired with the book)

1. **Chapter 1 (Execution) + 2 (Shuffle):** `sql/01-explain-shuffle.sql`, `pyspark/inspect_partitions.py`
2. **Chapters 4–5 (Joins, skew):** `sql/02-broadcast-vs-sort-merge-join.sql`, `sql/join-strategies/`,
   `sql/03-skew-detection.sql`, `pyspark/skew_detector.py`, `pyspark/skew-demo/`
3. **Chapter 3 (Partitioning) + 17 (Write):** `pyspark/partitioning-demo/`, `pyspark/file_count_audit.py`
4. **Chapter 9 (Catalyst / SQL):** `sql/04-window-vs-groupby.sql`
5. **Chapters 13 + 17 (Iceberg, write):** `sql/iceberg-write-path/`, `sql/07-iceberg-merge.sql` (when
   your session has Iceberg + tables)

**Harness:** `cd examples/local && ./run_examples.sh` runs the CSV-backed SQL and a subset of
PySpark. See [`local/README.md`](local/README.md).
