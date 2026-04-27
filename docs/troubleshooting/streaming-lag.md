# Troubleshooting: streaming lag and delivery delays

**Problem:** Micro-batches fall behind, processing time > trigger interval, or “watermark” and checkpoint issues.

## Symptoms

- **Input rate** is higher than **processing rate** in Structured Streaming (sustained).
- **Lag** in offsets (Kafka) or **event time** vs processing time **gap** growing.
- **Checkpoint** path errors, “multiple queries on checkpoint,” or state store corruption.
- **Batch** duration spikes when **state** grows.

## What to check first

1. **Trigger interval** — is the query trying to do more work per micro-batch than the interval allows?
2. **Checkpoint location** — unique per query, **durable** (S3), not deleted between deploys.
3. **State store** size — exploding keys, never-expiring state (missing **watermark** on event time with append mode).
4. **Sink** back-pressure — is Iceberg/JDBC/foreachBatch the bottleneck?

## Spark UI signals

- **Streaming** tab (if available): **batch** time vs **interval**; **scheduling** delay.
- **Batch** job details mirror batch Spark: if each micro-batch has a **huge** shuffle, tune like a batch job.
- **State** operations — **dedupe**, **sessionization**, `dropDuplicates` with watermark — show as stateful nodes.

## Logs and metrics

- Driver logs: **offset commit**, `StreamingQueryException`, `IllegalStateException` for state.
- Kafka: **consumer lag** per partition.
- Custom **metrics** on `foreachBatch` duration and rows written.

## Likely causes

- **Data volume** spike without **horizontal** scale.
- **Complex** state (large keys, `mapGroupsWithState` with unbounded state).
- **Small cluster** for chosen **shuffle** and **output** work per batch.
- **Bad sink** (slow JDBC, throttled S3) blocking micro-batch commit.
- **Checkpoint** on a lossy or wrong path; **schema** change without a new checkpoint when required.

## Fix options

- **Scale** out executors, increase `maxOffsetsPerTrigger` *carefully* (bigger micro-batches).
- **Simplify** state — pre-aggregate in batch layer; narrow **watermark**; avoid unbounded `groupBy` keys.
- **Repartition** before heavy **foreachBatch**; tune **output** to avoid small files [small-files](small-files.md).
- **Sink tuning** — Iceberg `write` options, connection pools, idempotent `MERGE` in batch.
- **Schema evolution** — follow Spark guidance for stateful queries; new checkpoint on incompatible change.
- **Trigger** = `Once` for catch-up, then return to **fixed interval** with validated capacity.

## Tradeoffs

- **Larger** micro-batches: better throughput, worse **latency** to sink.
- **Watermarks** drop late data — a product decision, not only a technical one.
- **idempotent** sinks + **dedup** cost CPU but prevent duplicates on retry.

## Example final diagnosis

*Symptoms:* Micro-batch p95 **&gt; 2×** trigger; Kafka lag 10M. **UI:** `foreachBatch` **Iceberg** merge dominates. **Cause:** `MERGE` did full table scan (missing partition filter) each batch. **Fix:** **merge** into daily partition + match condition; batch time back under interval.

## Prevention checklist

- [ ] `foreachBatch` path reviewed for **pruning** and **idempotency**
- [ ] **Watermark** and **state** policy documented; checkpoint path in IaC
- [ ] **Alerting** on batch duration, Kafka lag, and sink success
- [ ] **Load** test streaming query at peak **x2** in staging

**See also:** [`../book/14-structured-streaming.md`](../book/14-structured-streaming.md), [`../diagrams/structured-streaming-checkpoint-state.md`](../../diagrams/structured-streaming-checkpoint-state.md), [`../checklists/spark-streaming-checklist.md`](../checklists/spark-streaming-checklist.md).
