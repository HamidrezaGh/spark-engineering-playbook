# Case Study — Large Iceberg Merge On EMR: Memory Spill And Oversized Scope

This is an anonymized post-incident review of a recurring failure on a large daily merge job running on AWS EMR. Numbers are illustrative; the failure shape is real and common.

## Problem

A daily pipeline merged the previous day's CDC events into a multi-terabyte Iceberg fact table on S3. The job had been stable for ~9 months. Over a few weeks it became progressively slower, and then started failing.

- Source: ~250 GB / day of CDC events, partitioned by `event_date`.
- Target: ~6 TB Iceberg table partitioned by `event_date`, clustered by `customer_id`.
- Operation: `MERGE INTO target USING staging ON target.event_id = staging.event_id WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`.

By the time it became an incident, the job ran for 8+ hours when it succeeded and was OOM-ing two runs out of three.

## Symptoms

- Wall-clock time went from ~50 minutes to 8+ hours over six weeks.
- Multiple `ExecutorLostFailure` events per run, clustered around the merge stage.
- Some runs failed with `FetchFailedException` cascades after Spot task node loss.
- One run failed with executor OOM during a sort-merge join stage.
- EMR cluster cost tripled because the job was scaled up reactively each time it failed.

The on-call response to that point had been "add more memory and re-run." That stopped working.

## Evidence From Spark UI And Logs

Following the [Spark UI reading guide](../field-guides/spark-ui-reading-guide.md) workflow:

### Stages

- The slowest stage was the shuffle stage feeding the `MERGE` join — a sort-merge join between the staging dataset and the entire target table for the affected partitions.
- That stage was 92% of total runtime.
- Task duration distribution: median ~45 seconds, max ~58 minutes. Long tail.
- The slowest task: shuffle read ~14 GB; spill memory ~22 GB; spill disk ~9 GB.
- 30 of ~2000 tasks accounted for ~70% of stage runtime.

### SQL Tab

- Two `Exchange hashpartitioning(event_id, 200)` nodes, one for staging and one for the target side. Both sides shuffled fully.
- No broadcast — target was multi-TB so this was correct.
- AQE was enabled but did not help much: skew join handling triggered only on a subset of partitions and the working set per task remained large.
- The scan on the target side showed `PartitionFilters: [event_date >= ...]` but pulled in ~3 weeks of partitions, not just the day being merged. The merge condition included a small window of historical updates.

### Executors

- Executors were lost during the long-tail tasks. Three executors lost in one run, all on Spot task nodes.
- One executor consistently took ~5x more shuffle read than peers — the host of the skewed task.
- Driver was healthy; this was an executor-side problem.

### YARN / EMR Logs

- Container kill messages on the lost executors: physical memory limit exceeded.
- `spark.executor.memoryOverhead` had been bumped from 2 GB to 6 GB over the previous weeks; the kills continued.
- EMR step logs confirmed the job was running on a fleet that was 70% Spot for task nodes.

## Root Cause

There were three problems compounding into one incident:

1. **Merge scope had silently grown.** The original `MERGE` predicate matched a 1-day window. A change six months earlier extended this to 21 days to handle late-arriving CDC. The shuffle volume on the target side scaled with that window, not with the daily input. The job was no longer a "merge yesterday" job; it was a "join staging against three weeks of target" job.

2. **Per-task working set exceeded executor memory budget.** The shuffle volume per reduce partition had crossed the memory budget. Tasks were spilling tens of GB to disk, and the long-tail tasks were spilling more than that. Bumping `memoryOverhead` did not help because the limit being exceeded was the per-task working set, not heap.

3. **Spot task nodes amplified the failure.** Once the merge stage took ~6 hours, the probability of losing at least one Spot task node mid-stage approached 1. Each loss caused a `FetchFailedException` cascade and re-execution of upstream map output, which made the next attempt longer and more likely to lose another node.

The "obvious" fix (more memory) didn't work because the real problem was scope and shape, not capacity.

## Fix

The fix applied three changes in order, validating each in the Spark UI before adding the next.

### 1. Bound the merge scope explicitly

The merge query was rewritten to make the time window explicit and small, with a separate "late updates" path for the older window:

```sql
-- Daily fast path: yesterday only.
MERGE INTO fact_events t
USING (
  SELECT * FROM staging_events
  WHERE event_date = DATE '<run_date>'
) s
ON  t.event_id = s.event_id
AND t.event_date = s.event_date     -- partition pruning hint to the merge
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

The 21-day "late updates" merge moved to its own weekly job. After this change:

- Target-side scan dropped from 21 partitions to 1.
- Shuffle volume on the merge stage dropped ~14× on a typical day.
- Long-tail tasks went away because the per-task working set fit comfortably in memory.

### 2. Right-size the executors instead of overprovisioning

The cluster had been scaled vertically through repeated incidents. After fix #1, the working set per task was small enough that the original sizing was already adequate. The team reverted:

- `spark.executor.memoryOverhead` from 6 GB to 3 GB (PySpark workload, kept some headroom).
- Executor instance type from a memory-heavy variant back to the standard analytics fleet.
- Removed an ad-hoc `spark.sql.shuffle.partitions` override and let AQE choose at runtime.

This dropped cluster cost back to roughly the pre-incident level.

### 3. Move SLA-critical shuffle off Spot task nodes

The merge stage was kept on on-demand core nodes. Spot was reintroduced only for the read-heavy staging preparation, which is cheap to retry. After this:

- No more `FetchFailedException` cascades on the merge stage.
- The job became deterministic in runtime within ±15%, instead of varying 2× run-to-run.

## Result

| Metric | Before | After |
| --- | --- | --- |
| Daily merge runtime | 5–8+ hours, sometimes failed | ~50 minutes, deterministic |
| Failed runs / week | 2–3 | 0 |
| Cluster cost / day | ~3× baseline | ~1.0× baseline |
| Number of Spot reclamations affecting the job | Multiple per run | 0 (on the merge stage) |
| Spark UI long-tail ratio (max / median task duration) | ~80× | ~3× |

A guardrail metric was added: the merge stage's shuffle read total and max-to-median task duration ratio are now logged to a central metrics table after every run. A regression in either fires an alert before the SLA is at risk.

## Staff-Level Lesson

The local lesson is "scope your merges." The platform-level lessons are more important.

1. **A `MERGE` predicate is a contract, not an implementation detail.** Quietly widening a window from one day to 21 days is a 14× shuffle change. The platform should require predicate-scope review for incremental jobs over large tables, not leave it to whoever is writing the query.

2. **Adding memory is a diagnostic giveaway.** When a Spark job has had `executor.memoryOverhead` bumped multiple times and is still failing, the bottleneck is almost never heap. It is shape: per-task working set, skew, or scope. The platform should treat repeated memory escalations as a signal to open the Spark UI, not to provision more memory.

3. **Spot capacity is fine for compute. It is dangerous for shuffle.** A long shuffle stage on Spot is a probabilistic failure. The platform should provide guardrails or templates that keep shuffle-heavy SLA-critical stages on stable capacity by default, and require explicit opt-in for Spot on those stages.

4. **Every incident is a candidate platform improvement.** This incident produced one query rewrite, one cluster template change, and one new guardrail metric. Forty teams running similar jobs benefit from the metric and the template; only one team benefits from the query rewrite. Staff-level work is recognizing which lessons generalize and turning them into standards.

5. **Validate one change at a time.** It is tempting to apply all three fixes simultaneously. Doing so makes the post-mortem useless and removes the evidence needed to defend the changes against future "let's revert this" pressure. Each change above was validated in isolation in the Spark UI before the next was applied.

The runbook outcome wasn't "this team knows how to fix this." The outcome was "the platform now prevents this for everyone."
