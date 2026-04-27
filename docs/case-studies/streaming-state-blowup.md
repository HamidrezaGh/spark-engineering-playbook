# Case Study — Structured Streaming State Blowup

This is an anonymized post-incident review of a Structured Streaming job whose state store grew without bound, micro-batch durations slowly degraded, and end-to-end freshness eventually missed its SLA. Numbers are illustrative; the failure shape is real and common.

## Problem

A Structured Streaming job consumed user activity events from a Kafka topic and produced two outputs:

- A windowed aggregation: per-user event counts over 5-minute tumbling windows, written to an Iceberg table.
- A near-real-time enrichment: a stateful join between user events and a slowly-changing user profile stream, written to a second Iceberg table.

Trigger: `processingTime='30 seconds'`. Target end-to-end freshness: 2 minutes. Steady-state input volume: ~12k events/second.

The job had been stable for ~5 months. Over a 6-week period, micro-batch durations increased from a steady ~6 seconds to ~45 seconds. Checkpoint sizes grew from ~120 MB to over 18 GB. End-to-end freshness moved from ~90 seconds to ~7 minutes. The SLA started to miss daily during peak hours.

## Symptoms

- Spark UI Streaming Query tab showed batch duration trending up linearly over weeks.
- The `inputRowsPerSecond` was flat (workload unchanged) while `processedRowsPerSecond` was declining.
- Driver memory rose slowly and started GC-thrashing during peak hours.
- The state store directory in the checkpoint location on S3 grew from ~120 MB to over 18 GB.
- Restarting the streaming query from the latest checkpoint took >25 minutes — the state was being rebuilt into the executors.
- Two production incidents in the 6-week window: once when an executor was lost during peak and the job took 45 minutes to recover, and once when the driver heap exceeded its limit during a checkpoint commit.
- Watermark progress in the Streaming Query tab was lagging real-time by 12+ hours, and growing.

The on-call response had been to restart the job and increase driver memory. The state kept growing.

## Evidence From Spark UI / Streaming Query Progress / Logs

### Streaming Query tab

The `StreamingQueryProgress` records (printed every batch) showed the shape of the problem:

```json
{
  "batchId": 487120,
  "numInputRows": 360123,
  "inputRowsPerSecond": 12004.1,
  "processedRowsPerSecond": 7980.3,
  "durationMs": {
    "addBatch": 38400,
    "queryPlanning": 220,
    "walCommit": 110,
    "triggerExecution": 41200
  },
  "stateOperators": [
    {
      "operatorName": "stateStoreSave",
      "numRowsTotal": 84129044,
      "numRowsUpdated": 360123,
      "numRowsRemoved": 0,
      "memoryUsedBytes": 18234567890,
      "customMetrics": {
        "loadedMapCacheHitCount": 980,
        "loadedMapCacheMissCount": 4220
      }
    }
  ],
  "eventTime": {
    "watermark": "2026-04-13T11:18:42.000Z",
    "max":       "2026-04-25T08:14:30.000Z"
  }
}
```

What jumps out:

- `numRowsTotal` in the state store: 84 million entries, growing daily.
- `numRowsRemoved`: zero. The state operator never expired anything.
- `memoryUsedBytes`: ~18 GB on a single state operator.
- `watermark` was 12 days behind `max` event time.

Watermark 12 days behind real time is a smoking gun. The watermark advances only when no event with an earlier event time can still arrive — and Spark uses it to expire state. A stuck watermark means state never expires.

### SQL tab

The SQL tab for the job showed the streaming aggregation operator with a `stateStoreSave` node and a `flatMapGroupsWithState`-equivalent operator (used for the enrichment join's session logic). Both operators reported state size, both growing.

### Stages and Executors

- The slow batches' stages were dominated by `stateStoreSave` and `stateStoreRestore` operators, not by Kafka read or output write.
- Executor memory pressure peaked during state checkpointing. GC time per executor was 18% of total task time, vs ~2% in healthy steady state.
- Driver heap usage trended up as the streaming query metadata accumulated; the driver was tracking metadata for an unbounded number of state-key entries during planning.

### Logs

- Executor logs included `RocksDB` warnings about WAL size on the state store.
- Driver logs showed slow `commit` operations on the checkpoint directory; S3 list operations on the state store path were taking 30+ seconds because the directory had ~25,000 small files.
- No application errors — just slow batches that eventually cascaded.

## Root Cause

There were three compounding problems:

1. **No watermark on the windowed aggregation.** The original developer had used `groupBy(window(...), "user_id")` for the 5-minute aggregation but had not specified `withWatermark("event_time", "...")` on the source. Without a watermark, Spark cannot decide when a window is "done" and never expires its state. Every 5-minute window for every user since the job started was still in the state store.

2. **Watermark on the enrichment join was technically present but ineffective.**
   The enrichment used a `flatMapGroupsWithState` keyed by `user_id`.
   The watermark was set on the events stream but the enrichment state never used it
   for expiration; the developer had assumed Spark would expire on the watermark
   automatically for `mapGroupsWithState`, which is not how it works.
   State expiration in `flatMapGroupsWithState` requires explicit
   `state.setTimeoutTimestamp(...)` and a TTL strategy. The original code had neither.

3. **High-cardinality state key.** `user_id` had ~120 million distinct values across the lifetime of the job. Even with correct watermarking, state size scales with the number of active keys. The original design did not account for keys that are never seen again — long-tail users who appeared once and never returned. With no expiration, those keys lived in state forever.

The "obvious" fix (more memory) had been masking the underlying state-growth problem. State size was not bounded by anything in the code.

## Fix

The fix applied four changes in order, validating each in the Streaming Query tab before adding the next.

### 1. Add explicit watermark on the source

The source DataFrame was rebuilt with an explicit watermark and a documented late-data tolerance:

```python
events = (
    spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS raw",
            "timestamp AS kafka_timestamp"
        )
        .select(from_json("raw", schema).alias("e"), "kafka_timestamp")
        .select("e.*", "kafka_timestamp")
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .withWatermark("event_time", "10 minutes")
)
```

After this, the windowed aggregation could expire windows older than `current_watermark - 10 minutes`. The state store's `numRowsRemoved` started incrementing on every batch.

### 2. Use proper state TTL on the enrichment join

The enrichment was rewritten to use a `flatMapGroupsWithState` with explicit timeout, plus `GroupStateTimeout.EventTimeTimeout()`:

```python
def enrich(key, events_iter, state):
    if state.hasTimedOut:
        state.remove()
        return iter([])

    profile = current_profile_for(key)
    out = []
    for e in events_iter:
        out.append(enrich_row(e, profile))

    state.update(profile)
    state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 60 * 60 * 1000)  # 1h TTL
    return iter(out)
```

After this, users who had not appeared in events for an hour past the watermark had their state cleaned up. The state size dropped from 84 million entries to around 8 million within a day.

### 3. Reduce state key cardinality where possible

The team found that ~40% of distinct `user_id` values in the windowed aggregation were synthetic IDs from a logging path that was supposed to be filtered upstream. The filter was added, dropping the live key cardinality by ~40% with no business impact.

This is the single most underrated streaming optimization: state size scales with key cardinality, and key cardinality is often inflated by upstream noise that nobody is monitoring.

### 4. Resize state store and adjust checkpointing

After fixes 1–3, state size dropped to ~600 MB and the operational issues went away. The team also:

- Switched to `RocksDBStateStoreProvider` (already the default in modern Spark, but worth verifying); RocksDB scales better than the default in-memory state store for state >1 GB.
- Configured `spark.sql.streaming.minBatchesToRetain` to keep recent checkpoints for replay but avoid unbounded retention on S3.
- Moved the checkpoint location to an S3 prefix with versioning disabled (S3 versioning on a high-write checkpoint path produces a graveyard of delete markers and inflates listing cost).
- Added a cleanup job that periodically deletes orphaned files from old query restarts.

## Result

| Metric | Before | After |
| --- | --- | --- |
| Steady-state batch duration | ~45 seconds and rising | ~5 seconds, steady |
| End-to-end freshness | ~7 minutes | ~75 seconds |
| State store size | ~18 GB and growing | ~600 MB, bounded |
| State store entries | 84 million | ~8 million |
| Driver heap usage | 85% peak, GC-thrashing | 35% peak, stable |
| Time to recover from executor loss | 45 minutes | ~3 minutes |
| Restart-from-checkpoint time | 25+ minutes | ~2 minutes |
| Watermark lag from real time | 12 days | <30 seconds |

The streaming query has now been stable for over six months with no on-call pages.

## Lessons

The local lesson is "watermark your streaming aggregations." The platform lessons are more important.

1. **State store size is a streaming SLA.** Treat it like any other SLA: monitor, alert, set a budget, and act on a regression. A streaming job whose state size is not graphed is a streaming job that will eventually fail in this exact shape.

2. **Watermark behavior is per-operator, not per-query.** Adding `withWatermark` on the source affects time-based aggregations but not arbitrary stateful operators (`flatMapGroupsWithState`, custom state). Each stateful operator needs its own expiration strategy. This is a common bug that does not show up in tests.

3. **Key cardinality is the streaming equivalent of partition count.** For batch, partition count drives task count. For streaming, distinct key count drives state size, GC pressure, and recovery time. Audit it like you audit partition count.

4. **Slow drift is harder to diagnose than fast failure.** A streaming job that fails immediately is easier to fix than one that gets 1% slower per day. Add metrics that detect linear drift over weeks: batch duration, state size, watermark lag, checkpoint size. A regression alert on the slope is more useful than an alert on the absolute value.

5. **Restart-from-checkpoint time is a recovery SLA.** A 25-minute restart on a streaming job means a 25-minute outage during a node loss. State size dominates restart time. Bounded state means bounded recovery; unbounded state means unbounded outage.

6. **"Add memory" is the wrong response to state growth.** It buys time against an exponentially failing strategy. The right response is to find what state is not being expired and why.

## Guardrails Added

- **State size metric.** Per-operator state size emitted by the streaming query is captured every batch and persisted to a metrics table. Alert fires if state grows by more than 10% week-over-week with no input volume change.
- **Watermark lag metric.** Difference between `max event_time` and `watermark` timestamp, sampled every batch. Alert fires if the lag exceeds 2× the configured watermark delay tolerance.
- **Distinct key cardinality metric.** Approximate count of distinct state keys, computed per checkpoint. Alert fires on rapid growth.
- **Restart drill.** A monthly drill restarts the streaming job from checkpoint to verify recovery time stays under the SLA budget.
- **Streaming review template.** Any streaming PR must answer: which operators are stateful, what is the watermark, what is the state TTL strategy, and what is the expected steady-state state size. Linked from the design review template.
- **Default RocksDB state store provider.** Cluster default for streaming jobs, with explicit opt-out required.
