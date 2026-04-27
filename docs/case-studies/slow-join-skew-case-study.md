# Case Study — Slow Fact-To-Dimension Join From Key Skew

This is an anonymized post-incident review of a daily batch job whose dominant stage was a
**sort-merge join** between a large fact table and a shared dimension. Runtime jumped when one join
**key** began carrying a much larger share of traffic than the model assumed. Numbers are
illustrative; the **failure shape** (long tail on a shuffle join, one hot key) is common in
production.

## Situation

A nightly ETL job joined **events** (fact) to **accounts** (dimension) on `account_id`, then ran a
`GROUP BY` on several attributes from the dimension for reporting. The job had been stable for about
a year at ~18 minutes. Over three weeks, wall time grew to 55–90 minutes with the same code and
roughly the same *total* event volume (±5%).

- **Source fact:** one day of events, partitioned and pruned to `event_date = yesterday`.
- **Dimension:** a slowly changing **accounts** table (dozens of millions of rows; a small fraction
  of the fact per day, but not broadcast-eligible in practice).
- **Join:** `INNER` on `account_id` — the planner correctly chose `SortMergeJoin` (two large shuffled
  sides, stats showed both above the broadcast threshold).

The team’s first response was to raise `spark.sql.shuffle.partitions` and add executors. Runtime
moved a little, then **stopped improving**; max-to-median task time stayed bad.

## Symptoms

- **One** stage took ~80% of total job time (shuffle-heavy join/aggregate region).
- **Max** task duration in that stage was **~45× the median** (750 ms vs 34 s illustrative).
- A **single** task in the task table showed **~22× the median shuffle read** of other tasks in the
  same stage.
- No OOM: the problem was straggling, not memory failure. GC was elevated only on the executor that
  held the straggler.
- **EXPLAIN** still showed a plain `SortMergeJoin` and two `Exchange hashpartitioning(account_id,
  400)` — nothing “obviously broken” in the static plan.
- A **profile query** (see [`../../examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql))
  on the fact side for the day’s data showed that **one** `account_id` accounted for **~14% of
  rows** (up from about **0.5%** historically). That `account_id` was a new **integration partner** that
  sent high-volume traffic through a **single** business account for routing reasons.

## Evidence From Spark UI And SQL

### Stages and tasks

- The slow stage was the one containing the **post-shuffle** join and downstream partial aggregate
  feeding the `GROUP BY` — a classic shuffle-boundary stage.
- Sorting the tasks by **Duration** made the outlier obvious: one task, one executor, long runtime.
- That task was not “slow CPU across the board”; it was **slow because it was huge** (shuffle
  read + sort working set) relative to its peers.

### SQL tab

- Plan shape matched expectations: two exchanges on `account_id`, `SortMergeJoin`, then
  `HashAggregate` partials. **AQE** was on; it adjusted partition counts in some runs but did not
  eliminate the straggler — the skew was in **one** partition’s row mass after the hash partitioner.

## Root Cause

1. **Business change without data-model review** — a high-volume feed was **aggregated in the
   product** under one `account_id`, turning that key into a “celebrity” key. The join/aggregate
   **per key** was no longer a uniform distribution; one reducer partition held an outsized share of
   the work.

2. **Tuning without profiling** — raising shuffle partitions and cluster size did not fix **one**
  partition with most of the **rows for a key**; it only redistributed *other* work.

3. **No guardrail** on join-key concentration on this daily path, so the shift was detected only
   when the SLA was missed.

## What Did Not Work (Or Was Incomplete Alone)

- **Blindly increasing** `spark.sql.shuffle.partitions` — helped tail latency slightly, did not change
  the max/median ratio in a stable way.
- **Broadcasting the dimension** — rejected: dimension too large; also would not help when the
  skew is on the **fact** side for one key.
- **Salting in the first attempt without isolating the hot key** — risked incorrect aggregates if
  applied before the product team defined consistent semantics for the hot account.

## Fix

The fix sequence was **one change at a time**, each validated in the UI.

### 1. Data-side guardrail and visibility

- Added a **daily** report: top **N** `account_id` by row share on the fact. Alert when any key
  exceeds a agreed threshold (e.g. **1% of daily rows** for this product).

### 2. Query-side mitigation

- For the hot `account_id`, **pre-aggregated** the fact to one row per remaining group key **before**
  the main join, then **unioned** that small branch with the “everyone else” branch where keys were
  not in the top-N set. (Exact pattern depends on required dimensions; the principle is **isolate
  the hot key** so the main join returns to a near-uniform key distribution.)
- In parallel, enabled **AQE skew join** handling where supported, as a second line of defense after
  the key split; still kept the **business** report because operators want to know *which* key
  moved.

### 3. Longer-term product fix

- The partner’s traffic was **split** across several internal `account_id`s so no single key carried
  the entire integration volume. That restored a more even daily distribution and reduced the need
  for special-casing in SQL over time.

## Result

| Metric | Before (incident) | After (mitigation + product fix) |
| --- | --- | --- |
| Wall time | 55–90 min | ~20–25 min |
| Max / median task time in join stage | Very high (≈40×) | Back to a healthy band (under 8×) |
| Top key row share (fact) | ~14% on one `account_id` | under 1% after traffic split |
| Pager load | Frequent | None in 90-day window |

## Lessons (Platform Lessons)

1. **Skew is often a *data* change, not a Spark bug.** The plan was “correct”; the key distribution
   was not. Profile keys on **every** high-volume join when the business can change who sends data
   and how it keys.

2. **“Tune shuffle partitions” is not a substitute for a histogram.** If max ≫ median, open the
   **task table** and a **key profile** before turning knobs.

3. **AQE helps many skew shapes but is not a product requirement.** A hard celebrity key can still
   need **application or modeling** work.

4. **A cheap daily concentration metric** on critical join keys pays for itself in on-call and
   capacity planning. Treat it like table **file count** and **row count** monitoring.

5. **Isolate-then-union** is a standard pattern for hot keys; it must be **correct by construction**
  for your aggregations, not a copy-paste from another team’s job.

## Guardrails Added

- **Daily** top-key concentration report and threshold alert on the fact table feeding this job.
- **Design review** checklist item: any new “aggregator” or partner integration must state expected
  **key cardinality** and max share of daily volume per key.
- **Runbook** entry: “long tail in sort-merge join” → [skew troubleshooting](../troubleshooting/skew-and-stragglers.md)
  and [join performance tree](../troubleshooting/join-performance.md).

**See also:** [`emr-merge-memory-spill.md`](emr-merge-memory-spill.md) (a different join-heavy story
  on Iceberg `MERGE`), [`../book/05-data-skew.md`](../book/05-data-skew.md), and
[`../../examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql).
