# Iceberg + Spark (ops checklist)

- [ ] **Partition spec** matches **prune** columns used in the **vast majority** of reads and merges.
- [ ] **MERGE / DELETE** predicates are **selective** — not full table scans for small updates.
- [ ] **Concurrent writers** are coordinated — no two uncoordinated `MERGE`s on the same table without a plan.
- [ ] **Snapshots** — **expire** / retain policy set; **metadata** bloat is monitored.
- [ ] **Compaction** — `rewrite_data_files` (or platform equivalent) is scheduled where small files accrue.
- [ ] **Schema** evolution — `ALTER` and reader compatibility are tested before **prod** promotion.
- [ ] **Commit / retry** — job failures and Spark retries are safe with **Iceberg** serializable isolation.
- [ ] **Write options** — `write.distribution-mode` and **sort order** (if any) match **read** access.

**Chapters:** [`../book/13-iceberg-and-spark.md`](../book/13-iceberg-and-spark.md), [`../troubleshooting/iceberg-merge-issues.md`](../troubleshooting/iceberg-merge-issues.md) ·
**Example:** [`../../examples/sql/iceberg-write-path/README.md`](../../examples/sql/iceberg-write-path/README.md)
