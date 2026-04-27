# Troubleshooting: skew and stragglers

**Problem:** A few tasks run much longer than the rest, or the job’s wall time is dominated by a long tail.

## Symptoms

- **Max** task duration 10×–100× **median** in the Stages **Tasks** table.
- One or a few executors with far more **shuffle read** or **input bytes** than peers.
- A stage that “finishes” only when the last straggler completes.
- Occasional OOM on a single task while others are healthy.

## What to check first

1. **Which stage?** The skew is almost always in a shuffle-backed stage: join, `groupBy`, `distinct`, or window.
2. **Join or aggregate key?** Profile key frequency (see [`examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql)).
3. **Input file skew?** One huge file or one HDFS block can straggle even without key skew.
4. **AQE** — confirm `spark.sql.adaptive.enabled=true` and skew join settings for your Spark version.

## Spark UI signals

| Signal | What it usually means |
| --- | --- |
| Max task time ≫ median | Data or partition skew, not “needs more executors” |
| One task high shuffle read | Hot join or aggregate key, or one heavy shuffle partition |
| One task high input | Large file or split skew on scan |
| Spill on a few tasks only | Skewed partition working set in sort/agg/join |
| Skewed executors tab | Same executor may hold hot task — secondary signal |

![Placeholder: Spark UI task list — one task with duration far above the rest](../assets/screenshots/placeholder-spark-ui-skewed-stage.png)

<!-- Screenshot placeholder: `placeholder-spark-ui-skewed-stage.png` — skewed stage / tasks. Caption: sort by duration; match outlier to shuffle read vs input vs spill. -->

Caption: **Skew** is one **task** (or a few) at the end of a stage, not a uniformly slow stage.
Compare **per-task** shuffle read and **input** to the median, then profile join / group keys
([`../../examples/sql/03-skew-detection.sql`](../../examples/sql/03-skew-detection.sql)).

## Logs and metrics

- Task-level metrics in Spark UI: **Shuffle Read**, **Input Size**, **Spill**.
- Optional: sample key counts in a notebook or SQL (`GROUP BY` key with `count` order by desc).

## Likely causes

- **Hot key** in join / `groupBy` / window partition.
- **Null or default key** co-locating many rows.
- **Time-based skew** (all “today” in one partition).
- **File skew** — one file much larger than others on read.
- **Dynamic partition** write skew — one partition gets most rows.

## Fix options

- **AQE skew join** (if applicable to your operator and Spark version).
- **Salting** the hot key (with duplication on the other side) — see [`examples/pyspark/skew-demo`](../../examples/pyspark/skew-demo/README.md).
- **Isolate** the hot key: filter → process separately → union.
- **Pre-aggregate** to reduce row count before the expensive join.
- **Fix layout** — partition or cluster so large keys are splittable (Iceberg, table design).

## Tradeoffs

- Salting increases data volume and join complexity; test row counts and correctness.
- Isolating the hot key adds maintenance and must stay idempotent.
- Random salt without adjusting both sides of a join can **break** results — always verify semantics.

## Example final diagnosis

*Symptoms:* 45 min in one stage, max/median ≈ 40. **UI:** one task with **shuffle read** 30× median. **SQL:** `Exchange` on `user_id` before aggregate. **Data:** one `user_id` has 12% of daily rows. **Fix:** AQE skew handling + business rule to cap per-user processing batch size. **Prevention:** daily top-key report.

## Prevention checklist

- [ ] Skew called out in design review for high-cardinality join keys.
- [ ] AQE and skew join flags aligned with platform defaults.
- [ ] Metrics for max/median task duration ratio on critical jobs.
- [ ] Documented playbook for “celebrity key” products or customers.

**See also:** [`../book/05-data-skew.md`](../book/05-data-skew.md), [`../field-guides/debugging-skew.md`](../field-guides/debugging-skew.md).
