# Spark job design (quick checklist)

Use when designing a new batch or **Spark SQL** pipeline before the first production run.

- [ ] **Stage / shuffle** — I can name where shuffles are (joins, `groupBy`, windows, `distinct`) and
  why each exists.
- [ ] **Partitioning** — Input partition count and **shuffle** partition target are reasonable for
  the data size (not default-only).
- [ ] **Joins** — Join keys are **selective**; `EXPLAIN` shows an expected **broadcast** or
  **sort-merge**; stats exist for cost-based choices.
- [ ] **Skew** — Hot keys or celebrity keys are considered; mitigation path exists (AQE, salt, isolate).
- [ ] **Output** — Target **file size** and **file count** are explicit; no accidental `repartition` explosion.
- [ ] **Idempotency** — Reruns and failures are safe for the **sink** contract (path, table, merge key).
- [ ] **Observability** — Event log / metrics will answer “what was shuffle, spill, and duration last run?”
- [ ] **Resource** — **Driver** work (`collect`, huge plans, listings) is bounded; no `coalesce(1)` on
  big writes without review.
- [ ] **EMR/YARN** — Client vs cluster mode, **Spot** on shuffle, and **queue** fit the SLA class.
- [ ] **Iceberg / table** — **Partition** columns match the **filter** columns for reads and merges.

**Chapters:** [`../book/01-execution-model.md`](../book/01-execution-model.md),
[`../book/04-joins.md`](../book/04-joins.md),
[`../book/17-spark-write-path-and-output-files.md`](../book/17-spark-write-path-and-output-files.md)
