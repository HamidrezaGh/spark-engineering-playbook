# Case Study: Fixing A Skewed Spark Merge On EMR

Anonymized post-incident review of a recurring failure on a large daily merge job on AWS EMR against Iceberg on S3.

> **Numbers in this write-up are illustrative** — rounded for readability and to avoid leaking customer scale. The failure *shape* (scope creep, shuffle pressure, Spot during long shuffle) is common in production.

## Situation

A daily pipeline merged the previous day’s CDC events into a multi-terabyte Iceberg fact table on S3.

The job had been stable for roughly nine months, then degraded over several weeks: longer wall times, then intermittent failures.

- **Source:** CDC events for one logical day, stored with a partition column such as `event_date`.
- **Target:** a large Iceberg fact table partitioned by `event_date`, with clustering on a high-cardinality business key.
- **Operation:** `MERGE INTO target USING staging ON target.event_id = staging.event_id WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`.

By the time it became an incident, successful runs could exceed eight hours; roughly two out of three runs failed.

## Symptoms

- Wall-clock time grew from roughly an hour to many hours over about six weeks.
- Repeated `ExecutorLostFailure` around the merge stage; occasional `FetchFailedException` cascades after Spot task loss.
- At least one run failed with executor OOM during a sort-merge join stage.
- EMR cost rose sharply because the team scaled clusters reactively after each failure.

The on-call pattern had been “add more memory and re-run,” which eventually stopped helping.

## Investigation

Following the [Spark UI reading guide](../field-guides/spark-ui-reading-guide.md):

### Stages

- The dominant stage was the shuffle stage feeding the `MERGE` join — a sort-merge join between staging and the affected target partitions.
- That stage accounted for the vast majority of total runtime.
- Task duration was heavily skewed: median task duration was modest, but the slowest tasks ran tens of minutes.
- A small fraction of tasks accounted for most of the stage time.
- Shuffle read and spill metrics on the worst tasks were far above peers.

### SQL tab

- Two `Exchange hashpartitioning(event_id, …)` nodes — staging and target both shuffled end-to-end for the working set Spark chose.
- No broadcast on the target side (expected at multi-terabyte scale).
- AQE was enabled but could not fully absorb the problem: skew handling fired for some partitions, yet per-task working sets on hot buckets remained huge.
- Target scan showed partition filters, but the **effective** partition window was wider than “one day” because the merge predicate allowed a rolling history window for late-arriving updates.

### Executors and cluster shape

- Executors were lost during long-tail tasks; losses clustered on Spot-backed task nodes.
- One executor repeatedly showed several times the shuffle read of its peers — the host running the hot reduce work.
- The driver stayed healthy: this was executor-side pressure, not `collect()`.

### YARN / EMR logs

- Container kills cited physical memory limits.
- `spark.executor.memoryOverhead` had been raised multiple times in prior weeks; kills continued.
- Step logs showed a large fraction of task capacity on Spot.

## Root Cause

Three compounding issues:

1. **Merge scope crept without a capacity review.** A predicate change widened the target partition window from roughly one day to multiple weeks to absorb late-arriving CDC. Shuffle volume on the target side scaled with that window, not with “one day of staging.” The job was no longer architecturally a small merge; it was a large repeated join against a sliding target slice.

2. **Per-task working set exceeded the safe memory envelope.** Shuffle bytes per hot partition drove sort and merge structures past what executor heap and overhead could hold without heavy spill. Raising overhead did not shrink the working set — it only changed how YARN classified the failure.

3. **Spot on long shuffle stages raised the probability of fetch failures.** Once the merge stage ran for many hours, losing a task node mid-stage became likely. Each loss triggered shuffle recompute and stretched the next attempt.

## What Did Not Work

- **Repeated vertical scaling and memoryOverhead bumps** without changing join scope: costs rose, runtime variance stayed high, failures continued.
- **Treating the problem as “Iceberg is slow”** without opening the SQL plan: the plan showed the true partition read window and the exchanges.
- **Applying several fixes at once** on some reruns: made attribution noisy and slowed down the real post-mortem.

## Fix

Changes were rolled out **one at a time**, each validated in the Spark UI before the next.

### 1. Bound merge scope and split late updates

The daily merge was rewritten so the fast path touched **one** target partition (explicit alignment on the partition column in the `ON` clause where the engine can prune). A separate, lower-frequency job absorbed the wider late-arrival window.

Effects:

- Target-side scan dropped from many partitions to one for the steady path.
- Shuffle volume on the merge stage dropped by an order of magnitude on typical days.
- Long-tail tasks collapsed once per-task shuffle read returned to a sane range.

### 2. Right-size executors after scope was fixed

After the working set shrank, the overgrown executor shape from firefighting was rolled back: memory overhead returned toward normal for the workload, instance types moved back toward the standard analytics fleet, and ad-hoc shuffle partition overrides were removed so AQE could size partitions from observed traffic.

### 3. Keep SLA-critical shuffle off Spot

Merge stages were pinned to on-demand core capacity; Spot remained for earlier, retry-friendly stages. Fetch-failure cascades on the merge stage stopped being a routine outcome.

## Result

| Metric | Before (incident period) | After (steady state) |
| --- | --- | --- |
| Daily merge runtime | Many hours; frequent failures | Sub-hour class; stable week over week |
| Failed runs | Multiple per week | None in the tracked window |
| Cluster cost | Several times baseline | Back to baseline band |
| Merge-stage Spot losses | Common | None on the merge stage |
| Long-tail ratio (max / median task time) | Very high | Healthy band for the workload |

A guardrail was added: total shuffle read for the merge stage plus max-to-median task duration are logged after each run; regression in either pages on-call before the SLA burns.

## Lessons

The local fix was “scope the merge.” The durable contribution was **turning the incident into an operational pattern**.

1. **`MERGE` predicates that widen time windows are capacity changes** — they deserve the same review as doubling partition count or cluster size.
2. **Repeated memory-overhead escalation without UI evidence is a smell** — treat it as a trigger to inspect shuffle bytes, spill, and partition windows, not as a capacity knob to keep turning.
3. **Spot is a fine tool for elastic compute and a poor default for hours-long shuffle** — platform templates should encode that tradeoff so product teams do not rediscover it under pager load.
4. **One query fix helps one team; metrics and templates help every team** — platform follow-through packages the lesson so the next merge job inherits the guardrail.

The runbook outcome was not “this team is heroically good at Spark.” It was “the platform makes this failure class hard to repeat.”
