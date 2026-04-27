# Spark debugging (quick checklist)

For the first 15 minutes of a **slow** or **failed** job. Pair with
[`../observability/spark-ui-guide.md`](../observability/spark-ui-guide.md).

- [ ] **Reproduce** — Same **query** and **data window**? Config diff (`Environment` tab vs last good run).
- [ ] **Jobs / Stages** — **One** dominant slow stage, or many? **Failed** tasks vs slow tasks?
- [ ] **Task spread** — **Max** vs **median** duration → **skew** vs **even** slowness.
- [ ] **Shuffle** — Shuffle read/write and **fetch wait** — volume vs network vs cluster health.
- [ ] **Spill / GC** — **Spill** or **GC** on many tasks → partitions / memory / width of rows.
- [ ] **SQL** — `Exchange` count, `BroadcastHashJoin` vs `SortMergeJoin`, `PushedFilters` / `PartitionFilters`.
- [ ] **Executors** — **Lost** executors, **driver** memory — environment vs code.
- [ ] **Logs** — `FetchFailedException`, `OOM`, YARN preemption, **container kill** (memory / disk).
- [ ] **Smallest fix** — One change, validated with a **second** run and UI check.

**Trees:** [`../troubleshooting/README.md`](../troubleshooting/README.md)
