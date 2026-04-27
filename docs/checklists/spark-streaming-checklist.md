# Structured streaming (ops checklist)

- [ ] **Checkpoint** path is **unique** per **query** and **durable** (S3 with lifecycle understood).
- [ ] **Trigger** — processing time vs **Once**; micro-batch can finish **under** the interval at peak load.
- [ ] **Watermark** — if event-time + state, **late data** policy is explicit (product + eng).
- [ ] **State** size — `groupBy` / **session** keys are bounded; `dropDuplicates` + watermark is justified.
- [ ] **foreachBatch** — sink is **idempotent**; merge predicates **prune** target rows.
- [ ] **Schema** evolution — new columns / types have a **checkpoint** migration or **new** checkpoint.
- [ ] **Output** file control — not writing **infinite** small files; **repartition** / **table** write tuned.
- [ ] **Monitoring** — batch duration, **Kafka** lag (if used), and **sink** errors are alerted.

**Chapter:** [`../book/14-structured-streaming.md`](../book/14-structured-streaming.md) ·
**Troubleshooting:** [`../troubleshooting/streaming-lag.md`](../troubleshooting/streaming-lag.md)
