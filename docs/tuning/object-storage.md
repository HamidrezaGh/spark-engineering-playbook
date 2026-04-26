# S3 On EMR

Status: Draft

## Knob

S3 performance tuning is not one knob. It is a combination of file sizing, table layout, request concurrency, commit behavior, EMR release behavior, S3A/EMRFS configuration, KMS permissions, and Spark parallelism.

In practice, you should treat “S3 tuning” as three separate problems:

- **Metadata and listing**: how many files and partitions Spark has to discover and plan.
- **Scan throughput**: how quickly tasks can read bytes once work starts.
- **Commit behavior**: how quickly and safely output files are committed (especially for partitioned writes).

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

Over-tuning low-level S3A/EMRFS settings is also risky: it can mask the real problem (file layout) and create instability when workload shape changes.

## Validation

Validate with:

- Spark scan time and task count.
- Number of files scanned.
- Average file size.
- S3 request errors, retries, and throttling.
- Commit time for write jobs.
- CloudWatch metrics where enabled.
- Executor CPU utilization compared with storage wait symptoms.

Spark UI hints that you are S3/metadata-bound:

- **Stages**: scan stages with extremely large task counts and many very short tasks.
- **Stages**: long wall time with low CPU and little shuffle/spill.
- **SQL**: missing partition pruning or filter pushdown (Spark reads far more data than expected).
- **Executors**: low CPU utilization across executors while the application is slow.

What “good” looks like after fixes:

- Fewer files scanned for the same query (or larger average file sizes).
- Lower scan stage overhead and fewer tiny tasks.
- Lower end-to-end runtime with similar or lower compute.
- More stable runtimes across days (less sensitivity to file-count fluctuations).

## Real Use Case

An EMR job scans a table with 900,000 small Parquet files and shows low CPU usage. Increasing executors increases S3 requests but does not fix runtime. Compaction, better table metadata pruning, and final write partition control reduce both runtime and S3 request cost.

More detail:

- Root cause: a backfill produced extremely small files across many partitions.
- Evidence: Spark UI showed scan stages dominated by task overhead with low CPU.
- Fix:
  - rewrite/compact affected partitions to a sane target file size
  - add a guardrail on file count per partition and total files added per run
  - ensure future writes use a partition count that matches target file sizing
