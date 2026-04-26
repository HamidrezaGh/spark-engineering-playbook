# Spark UI Reading Guide

A practical, production-oriented guide to reading the Spark UI under time pressure.

The Spark UI is the source of truth when a job is slow, expensive, or failing. Logs lie about timing, ticketing systems lie about scope, and tribal knowledge ages badly. The UI is what actually happened. This guide is the workflow you should run every time you open it.

## The 90-Second Triage Workflow

When you open the Spark UI for an incident, you have one job: reduce the surface area to a single stage and a single root-cause class within ~90 seconds. Speed comes from following the same loop every time.

### The Loop

1. **Stages** — sort by duration. Open the slowest stage. Look at:
   - Task duration distribution (median vs max).
   - Shuffle read / shuffle write totals.
   - Spill (memory + disk).
   - GC time.
   - Failed tasks count.
2. **SQL** (DataFrame / Spark SQL workloads) — find the matching query. Look at:
   - `Exchange` nodes (shuffle boundaries).
   - Join strategy (`BroadcastHashJoin` vs `SortMergeJoin`).
   - Scan filters (`PartitionFilters`, `PushedFilters`, `ReadSchema`).
   - AQE adaptive nodes (coalesce, skew handling, dynamic strategy).
3. **Executors** — confirm cluster health.
   - Lost executors.
   - GC time per executor.
   - Per-executor shuffle read/write (uneven => skew or locality issue).

If you remember nothing else: **Stages → SQL → Executors**. Three tabs, three questions, ~30 seconds each.

### What Each Tab Is For

| Tab | Use It To Answer |
| --- | --- |
| **Jobs** | "What ran when?" Useful for orientation, rarely the fastest root-cause path. |
| **Stages** | "Which stage is the bottleneck and what does its task distribution look like?" The primary debugging surface. |
| **SQL** | "Which operators caused the expensive stage?" Best place for plan-level diagnosis. |
| **Executors** | "Was the cluster healthy during the bad stage?" Cluster-level reality check. |
| **Storage** | "Is caching helping or hurting?" Useful when caches are involved. |
| **Environment** | "Did a config change?" Useful for diffing against a known-good run. |

## Classifying The Bottleneck

Once you have the slowest stage open, the next decision is what *kind* of bottleneck you are looking at. Most production Spark issues fall into one of five classes. Each class has a distinctive Spark UI signature and a smallest-safe-fix path.

| Class | Signature In The UI | Likely Causes | Smallest Safe Fix |
| --- | --- | --- | --- |
| **CPU-bound** | High task CPU time. Low shuffle read. Low spill. Low GC. Tasks are running, just slowly. | UDFs, JSON parsing, expensive projections, regex, decompression. | Replace UDF with built-in expression. Push computation into Spark SQL. Reduce per-row work. |
| **Shuffle-bound** | High shuffle read / shuffle write. CPU is low while tasks wait on the network. Long fetch wait time. | Wide transformations on large data, missing pre-filter / pre-projection, fact-on-fact joins. | Push filters/projections before the shuffle. Convert to broadcast join if the small side allows. Tune `spark.sql.shuffle.partitions` and AQE. |
| **GC-bound / memory-bound** | High GC time. Heap usage close to limit. Tasks failing intermittently. Possible YARN container kills. | Wide rows, big windows, oversized broadcast, caching too aggressively, Python overhead. | Remove unnecessary cache. Project earlier. Increase memory overhead (especially on PySpark). Avoid unsafe broadcast. |
| **Spill-bound** | High spill memory + spill disk during sort/aggregate/join. CPU and shuffle look reasonable; tasks slow because they keep flushing to disk. | Per-task working set too large; too few shuffle partitions; bad join strategy; skew. | Increase shuffle partitions. Pre-aggregate. Switch to a smaller join. Reduce columns/rows entering the operator. |
| **Skew-bound** | Max task time ≫ median (often 10×–100×). Most tasks finish quickly, a few drag on. One task with anomalous shuffle read/input. | Hot key in join or `groupBy`. Hot partition column. One oversized input file. | Enable AQE skew join handling. Salt the hot key. Pre-aggregate before the skewed point. Fix upstream file/partition layout. |

Two quick rules to keep these straight:

- **Long tail (max ≫ median) ⇒ skew.** Don't tune memory; find the hot key.
- **Even tasks but slow ⇒ ask which resource is saturated.** CPU, shuffle, GC, or spill — only one of these is the answer at any time.

## Stages Tab — Deep Dive

This is where you spend most of the time during a real incident.

### What To Scan First (in order)

1. **Status / failed tasks.** Red flags here usually explain the incident faster than performance tuning. A handful of fetch failures often means executor loss; a stage with many task retries means instability, not a tuning problem.
2. **Stage duration.** Sort by duration. Most "slow job" incidents are dominated by one or two stages.
3. **Summary metrics** on the stage detail page:
   - **Input** — bytes/records read by the stage. Big number on a scan stage means you're scan-bound.
   - **Shuffle read / write** — bytes moved. Big numbers mean you're shuffle-bound.
   - **Spill (memory + disk)** — evidence of memory pressure during sort/aggregate/join.
   - **Task time distribution** — the long tail. Compare 75th percentile to max.
   - **GC time** — high GC suggests memory pressure or oversized partitions/objects.

### Per-Task Metrics

Different Spark versions show slightly different columns, but the high-value ones are:

- **Duration** — the headline.
- **Scheduler delay** — high values indicate too many tasks, queue pressure, or driver scheduling overhead.
- **Task deserialization time** — high values usually mean large closures, heavy UDFs, or oversized row objects.
- **Shuffle read time / bytes** — shuffle pressure.
- **Fetch wait time** — high with low CPU points to network bottleneck or shuffle service issues.
- **Spill (memory / disk)** — per-task working set is too large.
- **GC time** — memory pressure.
- **Input size / records** — one task with anomalous input size is the file-skew or split-skew signature.

### Common Pattern Recognition

- **Skew** — one or a few tasks run far longer than peers; those tasks have larger shuffle read, input, or spill than the median.
- **Shuffle bottleneck** — large shuffle read; tasks spend much of their time in fetch wait; CPU is low.
- **Over-partitioning** — huge task count, tiny per-task input, high scheduler delay, low CPU utilization.
- **Under-partitioning** — small task count, very large per-task input, long tasks, high spill, GC, and OOM risk.
- **Instability, not slowness** — many failed tasks, lost executors clustered around one stage. Often disk pressure, network issues, or Spot reclamation. Stop tuning Spark and look at the cluster.

## SQL Tab — Plan-Level Diagnosis

The SQL tab is where you confirm *why* a stage is expensive, not just *that* it is.

### What To Look For In The Physical Plan

- **`Exchange`** — shuffle boundary. One per stage transition.
- **Join type** — `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`. Strategy regressions are silent and brutal.
- **Scan pruning** — `PartitionFilters`, `PushedFilters`, `ReadSchema`. Missing pruning is a very common optimization gap.
- **Sorts and windows** — global sorts and windows force shuffles plus per-partition sorts.
- **AQE adaptive nodes** — when AQE is enabled, the plan node is annotated with `AdaptiveSparkPlan` or coalesce/skew markers. Verify what AQE actually did at runtime.

### Mapping Stage To Operator

The Spark SQL UI graph nodes link back to stage IDs. Use this to:

- Find which `Exchange` corresponds to the slow stage.
- Identify whether the stage is dominated by a join, an aggregation, a sort, or the write.

This is the reverse of the Chapter 1 workflow: instead of predicting stage boundaries from the plan, you are reading them off a real run.

## Executors Tab — Cluster Health Reality Check

Before you tune Spark, confirm the runtime wasn't simply unhealthy.

### What To Scan First

- **Lost executors / failed tasks per executor** — node instability, container preemption, disk failures.
- **GC time** — if a few executors have extreme GC, it usually correlates with skew or hot partitions.
- **Shuffle read/write per executor** — one executor doing far more shuffle than others is a skew or locality signature.
- **Disk used / spill** — confirms the memory pressure pattern seen in the Stages tab.
- **Driver row** — high driver heap or driver GC almost always means `collect()`, huge plan, or huge file listing.

### Common Interpretations

- Many lost executors during a shuffle-heavy stage → local disk pressure, network issues, or shuffle service problems.
- One executor doing far more shuffle read/write than peers → skew or locality imbalance.
- Driver GC + slow stage start → driver-side planning overhead, often from huge file listings or oversized plans.

## Storage Tab — Caching Reality

Caching is only valuable when it avoids real recomputation and actually fits.

Use Storage to confirm:

- Which RDDs/DataFrames are cached.
- Memory consumption of each cache.
- Whether data is being evicted or spilling.

### Anti-Pattern Signals

- A multi-hundred-GB cache that "might be useful" — almost always slows the job by stealing executor memory.
- A cache being repeatedly evicted and rebuilt — worse than no cache at all.
- A serialized storage level applied without measuring the CPU overhead.

## Environment Tab — Configuration Diffing

Most useful for two questions:

- Did `spark.sql.shuffle.partitions`, AQE settings, broadcast threshold, executor sizing, or S3 settings change between runs?
- Are you on the expected Spark version / EMR release?

For incidents, this tab is most useful for diffing against a known-good run.

## Symptom → Likely Cause → Smallest Fix

A consolidated table you can keep open during incidents.

| Symptom | Likely Cause | Smallest Safe Fix |
| --- | --- | --- |
| One slow stage, max task time ≫ median | Key skew | AQE skew handling; salt or pre-aggregate the hot key |
| Slow stage, even task times, high shuffle read | Shuffle volume too large | Push filters/projections earlier; consider broadcast join; tune shuffle partitions |
| Slow stage, even task times, low CPU, high fetch wait | Shuffle network bottleneck | Investigate executor/disk health; reduce shuffle volume |
| Slow stage, high spill | Per-task working set too large | More partitions; smaller join; remove caches; pre-aggregate |
| Slow stage, high GC | Memory pressure | Project fewer columns; remove caches; raise memory overhead (especially PySpark) |
| Slow stage, lots of tiny tasks | Over-partitioning | Coalesce; reduce shuffle partitions or post-shuffle target size |
| Slow stage, very few huge tasks | Under-partitioning | More shuffle partitions; check whether AQE coalesced too aggressively |
| Many failed/retried tasks, lost executors | Instability | Check disk, network, Spot reclamation; fix cluster before tuning Spark |
| Driver OOM during planning | Huge plan / huge listing | Reduce plan size; avoid massive `union` chains; fix small-files upstream |
| Driver OOM after success | `collect()` / `toPandas()` | Stop collecting; write to a sink and inspect there |
| Stage starts very late | Driver-side planning / scheduler delay | Listing cost on S3; many small files; complex plan |
| `FetchFailedException` in reduce stage | Lost shuffle output | Investigate executor loss; reduce Spot exposure on shuffle-heavy stages |
| Output has too many tiny files | Too many shuffle partitions feeding write | Coalesce or repartition by the right key before write |
| Output has one huge file | Too few partitions or accidental `coalesce(1)` | Increase partitions; remove unnecessary `coalesce(1)` |

## Real Use Case

A daily aggregation job regressed from 25 minutes to 2 hours.

- In **Stages**, one reduce-side stage dominated runtime and had a long-tail task distribution: max ≈ 60× median.
- The slowest task had ~30× the **shuffle read** of the median and heavy **spill**.
- In **SQL**, the plan showed an `Exchange hashpartitioning(customer_id, 200)` before an aggregation. The slow stage mapped to that exchange.
- A quick data profile (see [`examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql)) found one new `customer_id` accounting for ~38% of rows that day.

The fix was AQE skew join handling on the aggregation plus a guardrail metric on top-1 key concentration. The slow stage went back to ~30 minutes; the guardrail caught a similar shift two weeks later before the SLA breached. Total tuning effort: one configuration change and one new metric. No code change. No memory increase.

That's the goal of this guide: arrive at the smallest fix, supported by Spark UI evidence, in under an hour.
