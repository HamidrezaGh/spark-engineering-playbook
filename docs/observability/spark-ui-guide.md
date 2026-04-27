# Spark UI — production reading guide

A practical reference for the Spark Web UI. Use it to connect **what you see** in the UI to
**what you change** in the query, layout, or cluster.

The UI is the best record of a run: plan shape, per-stage cost, and task-level skew. For a
branching workflow by symptom, see [`../troubleshooting/README.md`](../troubleshooting/README.md).

> **Narrow duplicate:** a shorter, incident-oriented version of this page used to live only under
> [`../field-guides/spark-ui-reading-guide.md`](../field-guides/spark-ui-reading-guide.md). The
> field guide now points here so the playbook has one place to update.

## The 90-second triage workflow

**Goal:** Land on one dominant stage and one resource class (CPU, shuffle, memory/spill, skew, or
cluster instability) in about 90 seconds.

**Loop: Stages → SQL → Executors**

1. **Stages** — sort by duration; open the slowest stage. Check task duration (median vs max),
   shuffle read/write, spill, GC, failed tasks.
2. **SQL** — find the query; note `Exchange`, join type, `PushedFilters` / `PartitionFilters`,
   `AdaptiveSparkPlan` changes.
3. **Executors** — lost executors, per-executor shuffle imbalance, driver row.

## Tabs at a glance

| Tab | Primary question | Production notes |
| --- | --- | --- |
| **Jobs** | What actions ran, in what order? | One row per *action*; orients you when a driver runs many jobs. |
| **Stages** | Which stage and what task shape? | Main surface for time, skew, shuffle, and spill. |
| **Storage** | What is cached, and is it evicting? | Cache is often negative-sum; use to prove reuse. |
| **Environment** | What config and Spark version? | Diff against a known-good run during regressions. |
| **SQL** | Why is that stage expensive? | Physical plan, join pick, and scan pruning evidence. |
| **Executors** | Was the floor solid? | Lost executors, GC, shuffle imbalance, driver health. |

## Jobs tab

- Each **job** is one **action** (`write`, `count`, `foreach`, etc.).
- Use Jobs when a session runs multiple actions: find which action matches the time window a user
  cares about, then open its stages.
- A failed **job** often points to the last **stage** that completed before failure; still confirm
  in Stages to see retries.

## Stages tab

- **Duration** list — start here; long jobs are often one or two stages.
- **Summary metrics (stage detail):**
  - **Input** — large on scan-dominant stages; compare to expectations after filters.
  - **Shuffle read / write** — large values mean a wide transformation or big join/aggregate.
  - **Spill** — in-memory sort/agg/hash exceeded capacity; per-task work may be too big or skewed.
  - **Task time** — long tail: skew; even spread: look at **which** metric (CPU, shuffle, GC) is high.

## Tasks table (inside a stage)

- Sort by **Duration** to find stragglers.
- Compare **Input**, **Shuffle Read**, and **Spill** to the median task:
  - **Outlier with huge input** — file split skew or a hot partition in the read.
  - **Outlier with huge shuffle read** — key skew on the join/aggregate.
  - **Spill** on many tasks — under-partitioning or wide rows through a hash/sort.
- **Scheduler delay** high — may indicate too many tiny tasks, driver overload, or cluster contention.
- **Fetch wait time** high — shuffle or network; pair with **Executors** to rule out node loss.

## SQL tab

- Maps **query plan** nodes to **stages** (node links show stage id where applicable).
- Read **EXPLAIN**-style tree: `FileScan` + filters, `Exchange`, `BroadcastHashJoin` vs
  `SortMergeJoin`, `HashAggregate`, `Window`, and `Write`.
- AQE: look for `AdaptiveSparkPlan` and changed join or partition counts in the *final* plan
  (Spark 3+).

**See also (plan reading):** [`physical-plans.md`](physical-plans.md).

## Executors tab

- **Task time / shuffle / GC** by executor: uneven shuffle often tracks **skew** (one executor
  holds the hot task).
- **Failed tasks** across many executors during shuffle → environment / Spot / disk / network, not
  one key.
- **Driver** row: high memory or GC often from `collect()`, huge in-memory plan, or massive file
  listing (common with small files on object storage).

## Storage tab

- Lists cached Datasets: **memory** used, **deserialized** vs **serialized**, **disk** spill to
  block storage.
- Red flags: cache too large, constant eviction, or cache on a one-shot read path.

## Environment tab

- Confirms `spark.*` and runtime: **version**, **shuffle partitions**, **AQE**, **broadcast
  threshold**, classpath-relevant settings (in some UIs as separate sections).
- Use this tab to **prove** a config delta between prod and your notebook.

## Classifying the bottleneck (five common patterns)

| Class | What you usually see | First moves |
| --- | --- | --- |
| **CPU** | High task CPU, modest shuffle, modest spill; even tasks. | Cheaper expressions; fewer UDFs; push work into SQL; check compression/decode. |
| **Shuffle** | High shuffle read/write; **fetch wait**; low CPU. | Filter/project earlier; right join; tune partitions/AQE. |
| **Memory / GC** | High GC, heap near limits, container kills. | Narrow columns; shrink broadcast; increase overhead (PySpark); un-cache. |
| **Spill** | Spill to disk, tasks slow, shuffle may look “ok.” | More shuffle partitions; reduce rows into sort/hash; fix skew. |
| **Skew** | **Max** task time ≫ **median**; a few outlier tasks. | Skew join / salt / isolate key; not “add more memory” first. |
| **Instability** | Many failed tasks, lost executors, fetch failures across nodes. | Cluster / Spot / disk / network; reduce shuffle *while* you stabilize. |

## Symptom → likely cause → small fix (quick list)

| Symptom | Likely cause | Smallest useful fix |
| --- | --- | --- |
| One slow stage, max ≫ median | Skew or bad split | AQE skew; profile keys; file layout |
| Even tasks, high shuffle | Too much data across shuffle | Filter early; broadcast if valid |
| High fetch wait, low CPU | Network / shuffle / unhealthy nodes | Cluster triage; reduce shuffle |
| High spill | Working set / partitions | Repartition; less data into operator |
| High GC | Memory pressure, wide rows | Project early; clear cache; PySpark overhead |
| Tiny tasks everywhere | Over-partitioning | Coalesce; AQE coalescing; fewer shuffle parts |
| Few huge tasks | Under-partitioning | More shuffle partitions; check AQE |
| Lost executors in shuffle | Spot / disk / YARN | See [`../troubleshooting/emr-yarn-failures.md`](../troubleshooting/emr-yarn-failures.md) |

## Reference: signal → where to look → fix

| Signal | What it usually means | Where to look | Possible fix |
| --- | --- | --- | --- |
| **Max task time ≫ median** | Skew or one huge split | Stage **Tasks** table; compare input/shuffle per task | Skew join, salt, isolate key; fix file layout |
| **Even tasks, all slow, high input** | Scan or UDF on big read | **SQL** `FileScan`, **Stages** input bytes | Prune partitions; filter early; remove UDF |
| **High shuffle read/write, even tasks** | Large join/aggregate shuffles | **SQL** `Exchange` nodes | Narrow inputs; better join; broadcast if safe |
| **High fetch wait** | Shuffle or network | **Tasks**; **Executors**; host metrics | Less shuffle; cluster health; avoid Spot on shuffle |
| **Spill** | Sort/agg/hash too big per task | **Stages** summary; **Tasks** spill columns | More partitions; fewer rows; fix skew if skewed |
| **GC time** | Memory pressure | **Executors**; **Tasks** | Project; cache; overhead; downsize broadcast |
| **Driver memory high** | `collect` / plan / file listing | **Executors** tab driver row; driver logs | Stop collect; fix small files; smaller plan |
| **Many task failures, shuffle stage** | Fetch failure / node loss | Logs; **Jobs**/**Stages** error | EMR/YARN; retry after stability |
| **Output = huge file count** | Too many write tasks | Last stage task count; **SQL** write node | `coalesce` / `repartition` to target size |

## Real use case (abbreviated)

A daily job regressed from 25 minutes to 2 hours. **Stages** showed one reduce stage with max
≈ 60× median; **SQL** showed `Exchange` on `customer_id` before an aggregate. Key profiling
found one new hot key.

**Smallest fix:** AQE skew handling plus a data-quality check on key concentration. No driver
change. Full narrative: see “Real use case” in the [field guide
appendix](#historical-narrative-from-field-guide).

## Historical narrative (from field guide)

A daily aggregation job regressed from 25 minutes to 2 hours.

- In **Stages**, one reduce-side stage dominated runtime and had a long-tail task distribution:
  max ≈ 60× median.
- The slowest task had ~30× the **shuffle read** of the median and heavy **spill**.
- In **SQL**, the plan showed an `Exchange hashpartitioning(customer_id, 200)` before an
  aggregation. The slow stage mapped to that exchange.
- A quick data profile (see
  [`../../examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql)) found one
  new `customer_id` accounting for ~38% of rows that day.

The fix was AQE skew join handling on the aggregation plus a guardrail metric on top-1 key
concentration. The slow stage went back to ~30 minutes; the guardrail caught a similar shift two
weeks later before the SLA breached.

**Related:** [`../troubleshooting/slow-job.md`](../troubleshooting/slow-job.md),
[`../book/12-production-debugging.md`](../book/12-production-debugging.md),
[`../field-guides/debugging-slow-jobs.md`](../field-guides/debugging-slow-jobs.md).
