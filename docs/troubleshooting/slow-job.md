# Troubleshooting: slow Spark job

**Problem:** A Spark job is slower than expected (SLA miss, runtime regression, or “it used to be fine”).

## Symptoms

- Wall-clock runtime much higher than historical baseline.
- Stages or jobs that previously finished in minutes now take much longer.
- No obvious error — the job eventually completes (or is still running).

## What to check first

1. **Is this one stage or many?** Open **Stages** → sort by duration; one dominant stage narrows the problem.
2. **Did input size or schedule change?** More data, late-arriving partitions, or a new upstream feed can change cost without a code change.
3. **Config or cluster change?** Compare **Environment** to a known-good run (shuffle partitions, AQE, executors, Spark version).
4. **Data quality shift?** Skew, null explosion, or duplicate keys often show as one slow stage.

## Spark UI signals

- **One stage dominates total time** — focus there; the rest is noise.
- **Max task time ≫ median** — skew, bad files, or hot keys (see [skew-and-stragglers](skew-and-stragglers.md)).
- **Even tasks, all slow** — often shuffle volume, scan volume, or CPU-heavy operators (UDFs, JSON).
- **High spill** — memory pressure (see [memory-spill-oom](memory-spill-oom.md)).
- **High shuffle read/write** — wide transformations or large joins (see [shuffle-heavy-job](shuffle-heavy-job.md)).

## Logs and metrics

- YARN/EMR: container logs for **FetchFailedException**, preemption, disk full.
- Driver logs: planning time, `AnalysisException`, OOM.
- If event logs are enabled: **History Server** replay to compare two runs’ stage lists.

## Likely causes (decision tree)

- **Is one task much slower than others?**
  - Likely **skew** or a single huge input split → [skew-and-stragglers](skew-and-stragglers.md), profile keys / files.
- **Are all tasks in the slow stage slow?**
  - **Input too large** for the parallelism → more partitions, filter earlier, or bigger cluster.
  - **Shuffle too large** → reduce data before shuffle, better join order, broadcast if eligible.
  - **Memory / spill** → tune partitions, reduce working set, fix join strategy.
- **CPU low, shuffle or fetch wait high?**
  - **Shuffle/network bound** — reduce shuffle bytes, check cluster health, avoid unnecessary repartition.
- **Many tiny output files / tiny tasks?**
  - **Over-partitioning** at write → [small-files](small-files.md), coalesce/repartition to target file size.

## Fix options (examples)

- Push filters and column pruning earlier in the plan.
- Tune `spark.sql.shuffle.partitions` and AQE; validate with a second run.
- Replace broadcast-unsafe patterns or add hints only after EXPLAIN review.
- Fix upstream layout (file sizes, table partitioning) instead of only adding executors.

## Tradeoffs

- More executors: faster up to a point, then scheduling overhead and cost.
- More shuffle partitions: smaller tasks but more tasks and more output files.
- Caching: helps only on **repeated** reads of the same DataFrame; can steal memory and slow everything else.

## Example final diagnosis

*Symptoms:* Runtime 2h vs 30m baseline. **Stages:** one reduce stage is 80% of time. **Tasks:** max ≈ 50× median, one task with huge **shuffle read**. **SQL:** `Exchange` + `SortMergeJoin` on `customer_id`. **Root cause:** a new “celebrity” `customer_id` in the day’s data. **Fix:** AQE skew join enabled + data-side guardrail on top-1 key concentration. **Result:** back to ~35m; no memory increase.

## Prevention checklist

- [ ] Event logs retained for before/after comparisons.
- [ ] Key concentration or row-count checks on fact tables for daily jobs.
- [ ] `EXPLAIN` or plan capture in CI for high-cost jobs.
- [ ] Alert on stage duration p95/p99, not just job success.
- [ ] Document expected input cardinality and cluster shape per job class.

**See also:** [`../book/12-production-debugging.md`](../book/12-production-debugging.md), [`../field-guides/debugging-slow-jobs.md`](../field-guides/debugging-slow-jobs.md).
