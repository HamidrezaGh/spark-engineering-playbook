# Large Iceberg Merge

## Problem

You need to apply large upserts (MERGE semantics) into an Iceberg table. Naive merges can be extremely expensive: they can trigger huge shuffles, rewrite many files, create small files, and cause long runtimes or failures.

## Pattern

Make merges predictable and bounded:

- Reduce merge scope:
  - filter incremental changes to the minimum necessary key/date range
  - target only affected partitions where possible
- Reduce merge payload:
  - project only columns needed for the merge and output
  - pre-deduplicate change records (latest per key)
- Control file rewrite behavior:
  - aim for sane target file sizes
  - follow merges with controlled compaction when necessary
- Validate join strategy and shuffle sizing in Spark UI before scaling up.

## Tradeoffs

- MERGE provides correctness but can be costly compared to append-only designs.
- Limiting scope improves performance but requires careful incremental design.
- Compaction improves read performance and reduces small files, but adds extra compute cost.

## Failure Modes

- Merge rewrites far more data than expected due to broad predicates or missing partition pruning.
- Skewed keys create long-tail tasks and spill/OOM during join/aggregation steps.
- Small-file explosion after merge degrades downstream scans.
- Metadata growth (manifests/snapshots) increases planning overhead.

## Operational Checks

- Before running at full scale:
  - estimate change volume and target partitions
  - validate plan (`Exchange`, join type) and stage metrics on a sample
- During runs:
  - track shuffle bytes, spill bytes, max task duration
  - track output file count and file size distribution
- After runs:
  - validate table health (file counts per partition, query latency)
  - schedule compaction/optimize if needed
  - confirm snapshot/metadata retention policies are appropriate

## Real Use Case

A table receives daily CDC updates requiring upserts by primary key.

- The pipeline pre-deduplicates changes to the latest record per key.
- It filters merges to affected date partitions and projects only necessary columns.
- Spark UI validation focuses on the merge join stage for skew and spill.
- A follow-up compaction job rewrites small files into target-sized files to protect downstream performance.
