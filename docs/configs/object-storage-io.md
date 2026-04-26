# S3 / Object Storage IO Configs


## Core Idea

Many “Spark is slow with low CPU” incidents are actually remote storage or metadata overhead (listing many files, slow reads, throttling), not lack of executors.

## What To Validate First (Before Changing Configs)

- File counts and file sizes (small-file risk).
- Whether partition pruning and column pruning are working (SQL physical plan).
- Whether the job is spending time in scan stages with lots of tiny tasks.

## Common Failure Modes

- Excessive LIST/HEAD calls due to too many files and partitions.
- Slow planning and slow scans due to metadata overhead.
- Throttling or intermittent request failures amplifying runtime.

## UI-First Validation

- Stages: scan stages with huge task counts and many short tasks.
- SQL: scans missing pruning/pushdown.
- Executors: low CPU while wall time grows.

Note: object storage tuning is platform- and version-specific; treat it as a last-mile optimization after file layout and query shape are correct.
