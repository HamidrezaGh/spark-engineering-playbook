# Spark UI Reading Guide


## The 90-Second Workflow

When you open a Spark application, you usually want to answer four questions quickly:

1. What is the slowest or failed **stage**?
2. Is the bottleneck **CPU**, **shuffle**, **GC/memory**, **disk**, or **remote storage**?
3. Is it a **plan problem** (join strategy, shuffle boundary, missing pruning) or a **data shape problem** (skew, wide rows, too many files)?
4. What is the smallest safe fix you can validate?

If you only remember one flow:

- Go to **Stages** → sort by duration → open the slowest stage → check **task time distribution**, **shuffle read/write**, **spill**, **GC**, and **failed tasks**.
- Then go to **SQL** → open the matching query → read the **physical plan** and find shuffle boundaries (`Exchange`), joins, aggregates, and scans.
- Then go to **Executors** → confirm whether the cluster was healthy (GC, lost executors, memory, disk).

## Core Tabs (What Each Is For)

- **Jobs**: high-level actions and their stage DAG. Useful for “what ran when?” but rarely the fastest root-cause path.
- **Stages**: where most performance and failure diagnosis happens (skew, spill, shuffle, fetch failures).
- **SQL** (Spark SQL / DataFrame): maps user queries to operators; best place to spot `Exchange`, join type, scan pruning, and adaptive plan changes.
- **Executors**: cluster health (GC, memory, shuffle read/write per executor, task time, failures).
- **Storage**: cache/persist behavior; helps answer “is caching helping or hurting?”
- **Environment**: effective Spark configs and classpath; good for “did a config change?” debugging.

## Stages Tab (Primary Debugging Surface)

### What to scan first

- **Status / failed tasks**: any red flags usually explain the incident faster than performance tuning.
- **Stage duration**: find the longest stage(s). Most “slow job” incidents are one or two stages.
- **Summary metrics** (on the stage details page):
  - **Input**: bytes/records read by the stage (scan stages).
  - **Shuffle read / write**: bytes moved (shuffle stages).
  - **Spill (memory + disk)**: evidence of memory pressure during sort/agg/join.
  - **Task time distribution**: skew shows up as long tails.
  - **GC time**: high GC suggests memory pressure or too-large partitions/objects.

### How to recognize common patterns

- **Skew**
  - Signal: one or a few tasks run much longer than peers.
  - Corroboration: those tasks often have much larger **shuffle read**, **input size**, or **spill**.
- **Shuffle bottleneck**
  - Signal: big **shuffle read** and tasks spend lots of time waiting (often low CPU).
  - Corroboration: fetch failures (`FetchFailedException`) or long reduce-side stages.
- **Too many tiny tasks (over-partitioning)**
  - Signal: huge task count with small per-task input; stage time dominated by scheduler overhead.
  - Corroboration: low CPU utilization, lots of short tasks.
- **Too few huge tasks (under-partitioning)**
  - Signal: small task count with very large per-task input; long tasks, spill, GC, OOM risk.
  - Corroboration: high spill and uneven executor utilization.

### Useful per-task breakdown columns (when available)

Different Spark versions show slightly different columns, but the high-value ones are:

- **Duration**
- **Scheduler delay**
- **Task deserialization time**
- **Shuffle read time / bytes**
- **Fetch wait time**
- **Spill (memory/disk)**
- **GC time**
- **Input size / records**
- **Output size / records**

Interpretation shortcut:

- High **scheduler delay**: too many tasks, queue pressure, or driver scheduling overhead.
- High **fetch wait time** with low CPU: shuffle/network bottleneck or remote shuffle service issues.
- High **GC time**: memory pressure, big objects, wide rows, bad partition sizing, Python overhead.
- High **deserialization time**: large closures, heavy UDFs, or overly complex row objects.

## SQL Tab (Plan + Operator-Level Diagnosis)

Use SQL to answer “what operators caused the expensive stage?”

### What to look for in the physical plan

- **`Exchange`**: almost always indicates a shuffle boundary.
- **Join type**: broadcast hash join vs sort-merge join vs shuffled hash join.
- **Scan pruning**: partition filters, pushed filters, read schema vs table schema.
- **Sorts / windows**: global sorts and window functions often force shuffles/sorts.
- **AQE changes** (when enabled): plan nodes marked as adaptive; coalesced partitions; skew handling.

### Fast mapping from stage to operator

On Spark SQL UI, the query page usually shows a graph where nodes link to stage IDs. Use this to:

- Find which `Exchange` corresponds to the slow stage.
- Identify whether the stage is dominated by join/aggregation/sort/write.

## Executors Tab (Cluster Health Reality Check)

Before “tuning Spark,” confirm the runtime wasn’t simply unhealthy.

### What to scan first

- **Lost executors / failed tasks per executor**: node instability, preemption, disk failures.
- **GC time**: if a few executors have extreme GC, it often correlates with skew/hot partitions.
- **Shuffle read/write**: hotspot executors can indicate skew or uneven scheduling.
- **Input / disk spill / memory spill**: confirms memory pressure patterns seen in stages.

### Common interpretations

- Many lost executors during a shuffle-heavy stage: local disk pressure, network issues, or shuffle service issues.
- One executor doing far more shuffle read/write than others: skew or locality imbalance.

## Storage Tab (Caching Reality)

Caching is only good when it avoids expensive recomputation and actually fits.

Use Storage to confirm:

- What RDD/DataFrame caches exist.
- How much memory they consume.
- Whether data is being evicted or spilling.

Anti-pattern signals:

- Caching a very large dataset “just in case,” causing GC/spill and slowing the whole job.
- Persisting with a storage level that forces heavy serialization/CPU without benefits.

## Environment Tab (Configuration Diffing)

Use Environment to answer:

- Did `spark.sql.shuffle.partitions`, AQE, broadcast threshold, executor sizing, or S3 settings change?
- Are you running the expected Spark version / EMR release?

For incidents, this tab is most useful for comparing to a known-good run.

## Real Use Case

A daily aggregation job regressed from 25 minutes to 2 hours.

- In **Stages**, one reduce-side stage dominated runtime and had a long-tail task distribution.
- The slowest task had far higher **shuffle read** and heavy **spill**.
- In **SQL**, the plan showed an `Exchange` before an aggregation; the stage mapped to that exchange.
- A quick data profile found one key with a huge fraction of rows (new skew).
- Fix: add skew handling (salting / AQE skew join handling depending on operator) and add a guardrail metric for top-key concentration.
