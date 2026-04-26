# S3 On EMR

Status: Draft

## Knob

S3 performance tuning is not one knob. It is a combination of file sizing, table layout, request concurrency, commit behavior, EMR release behavior, S3A/EMRFS configuration, KMS permissions, and Spark parallelism.

## When It Helps

Tune S3 behavior when Spark shows low CPU utilization but slow scans, slow planning, slow commits, frequent storage retries, or high small-file overhead.

Common high-impact levers:

- Compact small files.
- Reduce unnecessary partition listing.
- Use Iceberg/Delta/Hudi metadata pruning instead of raw directory discovery.
- Control final write partition count.
- Avoid rename-heavy write patterns.
- Preserve Spark event logs to S3 for after-the-fact analysis.

## When It Hurts

Adding more executors can make S3 pressure worse if the bottleneck is request rate, listing, KMS throttling, or small files. More parallelism is useful only when S3 and the table layout can support it.

## Validation

Validate with:

- Spark scan time and task count.
- Number of files scanned.
- Average file size.
- S3 request errors, retries, and throttling.
- Commit time for write jobs.
- CloudWatch metrics where enabled.
- Executor CPU utilization compared with storage wait symptoms.

## Real Use Case

An EMR job scans a table with 900,000 small Parquet files and shows low CPU usage. Increasing executors increases S3 requests but does not fix runtime. Compaction, better table metadata pruning, and final write partition control reduce both runtime and S3 request cost.
