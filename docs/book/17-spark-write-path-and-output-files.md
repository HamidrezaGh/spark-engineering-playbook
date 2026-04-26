# Spark Write Path And Output Files

Status: First Draft
Level: Senior to Staff
Covers: file writers, task outputs, commit protocols, object-store writes, output file sizing

## Core Idea

Spark writes are distributed. Each task writes output for its partition, so final file count is tied to final partition count, table partitioning, retries, and writer behavior.

## Key Takeaways

- **Final partition count strongly influences output file count**.
- **Commit behavior matters more on S3 than on HDFS**.
- **Task retries and speculation can create orphan or duplicate attempt files** if commit handling is wrong.
- **Table formats reduce raw path write risks** by committing metadata snapshots.

## Mental Model

A write is not one file operation. Spark schedules tasks, each task writes data files, and a commit protocol coordinates which files become visible. Table formats add a metadata commit layer.

```text
Final DataFrame partitions
  -> write tasks
  -> task attempt files
  -> task commit
  -> job commit
  -> visible output files or table snapshot
```

| Lever | Affects | Risk |
| --- | --- | --- |
| Final partition count | Number of writer tasks | Too many or too few files |
| Table partitioning | Directory/table layout | High-cardinality small files |
| Commit protocol | Retry safety | Orphan or duplicate files if wrong |
| Compaction | Read efficiency | Extra maintenance cost |

## What Spark Does Internally

For file writes, tasks write temporary or task-attempt outputs. On successful completion, Spark commits task outputs and then commits the job. On object stores, commit behavior is more complex because rename is not a cheap atomic metadata operation like on HDFS.

Speculative execution and retries can create multiple attempts for the same task. Correct commit protocols ensure only the winning attempt becomes part of the committed output, but misconfigured sinks or raw path logic can leave orphan files.

## Why It Matters In Production

Write path controls:

- Output file count.
- File size.
- Atomicity.
- Retry safety.
- Downstream read performance.
- Object-store cost.

Small output files often come from too many final partitions, high-cardinality table partitions, frequent streaming micro-batches, or writing after repartitioning poorly.

## Common Failure Modes

- 50,000 small files from high task count or over-partitioned writes.
- Partial raw-path overwrite after job failure.
- Duplicate attempt files from failed or speculative tasks.
- Slow commits on S3.
- Downstream query slowdown from many tiny files.

## Tuning And Configuration

Control output files by:

- Repartitioning or coalescing before write.
- Targeting reasonable file sizes.
- Avoiding high-cardinality table partition columns.
- Using table-format compaction.
- Configuring writer options where available.
- Separating write parallelism from long-term table layout.

## Spark UI Signals

Check:

- Final write stage task count.
- Output bytes and records.
- Write duration.
- Number of files produced.
- Failures in task commit or job commit.

## Best Practices

- Write through table APIs for managed tables.
- Treat raw path overwrites as risky for critical data.
- Monitor output file count and average file size.
- Compact small files as part of table maintenance.
- Ensure production writes are retry-safe.

## Anti-Patterns

- `coalesce(1)` for production single-file output.
- Partitioning output by high-cardinality user or event IDs.
- Ignoring failed write attempts in S3.
- Using append-only writes for rerunnable pipelines.

## Example

```python
(
    daily.repartition(200, "event_date")
         .write
         .mode("overwrite")
         .partitionBy("event_date")
         .parquet("s3://lake/events_daily/")
)
```

This can still create too many files if `event_date` has many values or if 200 final partitions are not aligned with target file sizes.

## Interview-Style Questions Covered

- What happens internally when Spark writes files?
- Why does each task usually write one or more output files?
- What is a commit protocol?
- Why are object stores different from HDFS for Spark writes?
- What causes duplicate, temporary, or orphan files?
- How can speculative execution and task retries affect writes?
- How do you control output file size?
- How do you safely overwrite partitions?
- What is the difference between writing a DataFrame and writing to a table format like Iceberg or Delta?
- How do you debug a job that writes far more files than expected?

## Real Use Case

A streaming job writes to a data lake every 30 seconds and creates millions of tiny files. The fix is to increase trigger interval, write to a transactional table, compact small files, and enforce output file count checks as part of pipeline observability.
