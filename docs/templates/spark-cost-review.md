# Spark Cost Review Template

Use this template to review the cost of a Spark job — quarterly for SLA-critical jobs, opportunistically for any job whose cost has surprised the team. The intent is to catch waste (over-provisioning, over-partitioning, missing pruning, accumulated small files) before the finance team does.

Cost reviews are not "make it cheaper at all costs." They are "is the cost proportional to the value, and what is the cheapest version that still meets the SLA?"

## Job Identity

- **Job name**:
- **Owning team**:
- **Last cost review date**:
- **Runtime environment** (EMR, Databricks, Kubernetes):
- **Linked design review**:

## Cluster Sizing

- What is the cluster shape (instance types, master / core / task counts, memory per executor)?
- What was the sizing rationale, and when was it last validated?
- What is the daily cluster cost?
- Is the cluster ephemeral (per-job) or persistent (shared)?
- Is auto-scaling enabled? Is it actually scaling, or is it pinned at the maximum?

## Executor Sizing

- `spark.executor.cores` =
- `spark.executor.memory` =
- `spark.executor.memoryOverhead` =
- `spark.driver.memory` =
- `spark.driver.cores` =
- Are these values defaults, tuned, or copied from another job?
- Has memory been bumped repeatedly without root cause? (a debugging giveaway)

## Shuffle Cost

- What is the largest stage's shuffle write/read bytes?
- What is the total shuffle bytes per run?
- Is `spark.sql.shuffle.partitions` set explicitly? Why?
- Is AQE coalescing enabled and effective?
- Are there shuffles that could be eliminated (broadcast where appropriate, semi-join to narrow)?

## File Count

- What is the typical files-per-partition on the source?
- What is the typical files-per-partition on the destination?
- Is there a compaction job? Is it succeeding?
- What is the trend on file count over the past 90 days?
- Are output files in the 128 MB – 1 GB target range?

## Storage Layout

- Is the destination table partitioned appropriately for the query patterns that read it?
- Is there over-partitioning that creates small files?
- Is there a clustering / bucketing strategy?
- For Iceberg/Delta: are snapshots being expired?
- For Iceberg/Delta: are orphan files being removed?
- What is the storage growth rate, and is it sustainable?

## Runtime Trend

- What is the trailing 30-day median runtime?
- What is the trailing 30-day max runtime?
- Is runtime trending up, down, or flat?
- Is runtime variance high (5×+ between runs)? If yes, what causes it?

## Wasted Parallelism

- Are there stages with many tasks but each task takes <10 seconds? (over-partitioning)
- Are there stages with few tasks but each task takes >30 minutes? (under-partitioning)
- Is the cluster's CPU utilization aligned with the job's task count?
- Are executors idle for significant fractions of the job?

## Over-Partitioning

- Does the destination table have a partition column with high cardinality?
- Does the job produce many output files per run?
- Is `spark.sql.shuffle.partitions` higher than the workload requires?
- Has the partitioning grown organically over time (each engineer added another column)?

## Under-Partitioning

- Are there stages with one or two huge tasks?
- Is shuffle volume per partition above 1 GB (suggests too few partitions)?
- Is there a single global aggregation where partial aggregation could parallelize the work?

## Maintenance Jobs

- Is there a compaction / table-maintenance job for any tables this job reads or writes?
- Does that maintenance job run successfully?
- Does it have monitoring and paging?
- Is its cost proportional to the value (i.e., it actually reduces query cost downstream)?

## Cost Per Unit

A useful normalized metric: cost per million input rows, or cost per TB processed. Tracking this trend is more useful than tracking absolute cost.

- Cost per million input rows:
- Cost per TB processed:
- Cost per output partition produced:
- 90-day trend on each:

## Spot vs On-Demand

- What percentage of the cluster runs on Spot?
- Are SLA-critical shuffle stages on Spot? (Generally a bad idea.)
- Has Spot reclamation caused any incidents in the past 90 days?
- Is the Spot strategy documented?

## Cost-Reduction Levers Considered

| Lever | Estimated Savings | Implementation Cost | Decision |
| --- | --- | --- | --- |
| Smaller cluster | | | |
| Different instance type | | | |
| Move shuffle stages off Spot (if currently on) | | | |
| Pre-filter inputs before joins | | | |
| Add or fix table compaction | | | |
| Reduce shuffle partition count | | | |
| Enable / tune AQE | | | |
| Combine multiple jobs that share inputs | | | |
| Move to a different runtime (Databricks, EMR Serverless) | | | |
| Reduce job frequency | | | |

## Reviewer Sign-Off

- [ ] Cluster sizing is justified by current workload, not historical assumptions.
- [ ] Executor sizing is intentional, not copied or accumulated through past incidents.
- [ ] Shuffle volume is bounded; AQE is enabled where appropriate.
- [ ] Output file count is in target range; compaction is in place where needed.
- [ ] Storage layout matches the query patterns that read the table.
- [ ] Runtime trend is flat or declining; variance is acceptable.
- [ ] No obvious wasted parallelism.
- [ ] Cost per unit (rows or TB) is tracked.
- [ ] Spot strategy is documented and SLA-aware.
- [ ] Cost-reduction levers are considered with explicit rationale.

Reviewer name and date:
