# Write distribution and sorting (Iceberg + Spark)

When Spark writes to Iceberg, **how** you distribute rows across tasks changes:

- **File sizes** in the next snapshot
- **Read** **prune** in downstream jobs
- **Merge** and **compaction** cost later

**Common Spark write options to discuss (names vary by version):**

- **`write.distribution-mode`** — `none`, `hash`, or `range` to align Spark output partitions with
  **Iceberg** **partitioning**; misalignment creates **small** files and bad metadata.
- **Sort order** — cluster by common **filter** columns to improve scan pruning.

**Tradeoff:** stronger distribution and sorting **cost more CPU** on the **write** path. That is
often **cheaper** than months of bad read performance and endless reactive compactions.

**See:** [`../../docs/book/13-iceberg-and-spark.md`](../../docs/book/13-iceberg-and-spark.md)
