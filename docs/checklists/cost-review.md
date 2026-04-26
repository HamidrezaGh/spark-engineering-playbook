# Cost Review Checklist

## Compute

- What is the steady-state runtime and how variable is it day-to-day?
- Is the workload CPU-bound, shuffle-bound, or IO-bound?
- Are you paying for idle executors (low CPU, long wall time)?
- Are there obvious opportunities to reduce work:
  - filter/project earlier
  - remove expensive UDFs
  - avoid unnecessary shuffles
- Is the cluster right-sized (instance types, executor sizing, concurrency)?

## Storage

- Are you creating too many files (small-file explosion) that increase request costs and planning time?
- Is data retention/TTL defined for intermediate and output datasets?
- Are table partitions/layout aligned with common query patterns (pruning works)?
- Are compaction/optimize jobs needed and budgeted?

## Shuffle And IO

- What are the top stages by shuffle read/write?
- Is shuffle volume proportional to input size, or inflated by wide rows/unneeded columns?
- Is spill high (disk IO cost + runtime) and why?
- Is remote storage (S3) a bottleneck:
  - high listing/HEAD volume
  - throttling/errors
  - slow scan stages with low CPU

## Workload Shape

- Is the job incremental or reprocessing large historical ranges frequently?
- Are backfills controlled (throttled, partition-scoped) or do they rewrite entire tables?
- Does the pipeline create amplification:
  - exploding joins
  - wide intermediate datasets
  - repeated scans of the same data (missing caching or materialization)
- Do you have guardrails:
  - shuffle bytes
  - spill bytes
  - file count
  - input bytes/rows per run
