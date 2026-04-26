# Examples

Short, annotated examples that support the handbook. Each example is paired with the chapters it illustrates and explains, in comments, what to look for in the Spark UI and which production issue it helps diagnose.

These are not benchmarks or end-to-end pipelines. They are the kind of small diagnostic snippets a staff engineer keeps in a scratchpad and pastes into a notebook during incident triage.

## Directories

| Directory | Contents |
| --- | --- |
| [`sql/`](sql/README.md) | Spark SQL examples: reading physical plans, join strategy, skew detection, window vs `GROUP BY`. |
| [`pyspark/`](pyspark/README.md) | PySpark diagnostic scripts: partition inspector, skew detector, file count auditor. |
| [`configs/`](configs/README.md) | Spark configuration examples annotated with the workload shape they target. |

## Reading Order

If you are going through the book sequentially, the natural pairing is:

1. After Chapter 1 (Execution Model): `sql/01-explain-shuffle.sql`, `pyspark/inspect_partitions.py`.
2. After Chapter 4 (Joins) and Chapter 5 (Data Skew): `sql/02-broadcast-vs-sort-merge-join.sql`, `sql/03-skew-detection.sql`, `pyspark/skew_detector.py`.
3. After Chapter 9 (Spark SQL And Catalyst): `sql/04-window-vs-groupby.sql`.
4. After Chapter 17 (Write Path) and Chapter 18 (S3): `pyspark/file_count_audit.py`.
