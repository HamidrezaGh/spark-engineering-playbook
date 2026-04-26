# PySpark Examples

These scripts are short, single-purpose tools that you would actually use in a production debugging session. Each one is designed to answer one specific question that comes up in real Spark operations.

The point of these examples is not "look how to use the API." The point is to give you reusable diagnostic snippets that you can drop into a notebook or a Spark shell when investigating a real incident.

## Files

| File | Purpose | Pairs With |
| --- | --- | --- |
| [`inspect_partitions.py`](inspect_partitions.py) | Show per-partition row counts and skew indicators for any DataFrame. | Chapter 1 (Execution Model), Chapter 3 (Partitioning). |
| [`skew_detector.py`](skew_detector.py) | Compute distribution stats and top keys for a candidate join/group-by key. | Chapter 5 (Data Skew), Chapter 6 (AQE). |
| [`file_count_audit.py`](file_count_audit.py) | Walk an output prefix and report file counts, sizes, and small-file ratios per partition. | Chapter 17 (Write Path), Chapter 18 (S3), `docs/field-guides/small-files-playbook.md`. |

## How To Use These

Each script is a standalone CLI:

```bash
spark-submit examples/pyspark/inspect_partitions.py --table db.events --filter "event_date='2026-04-25'"
spark-submit examples/pyspark/skew_detector.py     --table db.events --key customer_id --filter "event_date='2026-04-25'"
spark-submit examples/pyspark/file_count_audit.py  --path s3://my-lake/events/
```

They are deliberately small. Adapt them rather than try to import them as a library; the goal is to keep the diagnostic logic visible in front of you, not hidden behind a wrapper.

## Conventions

- All three scripts annotate, in the docstring, what to look for in the Spark UI and which production issue they help diagnose. Read the docstring before running.
- None of them write data. They are read-only diagnostic tools.
- They prefer Spark SQL primitives (`groupBy`, `agg`, `percentile_approx`) over Python loops, because that's what the rest of the handbook recommends.
