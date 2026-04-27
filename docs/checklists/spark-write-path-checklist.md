# Spark write path (batch checklist)

- [ ] **Partition count** into the write is tuned to a **file size** target, not the default from an
  upstream shuffle.
- [ ] **Dynamic partition** insert — cardinality of partition columns is understood (avoid millions of
  dirs if unintended).
- [ ] **Coalesce(1)** / single task — not used for large data without explicit sign-off.
- [ ] **Speculative execution** — understood for **object-store** and **table** committer semantics.
- [ ] **Retry** idempotency — the job can safely **re-run** the write path without dup or silent loss.
- [ ] **Small files** — output **file count** and average size are checked after the first run.
- [ ] **Table format** — **Iceberg/Delta** **commit** and **retention** policy fit the read SLA.
- [ ] **Sort/cluster** options — if used, they match the **read** access pattern of downstream jobs.

**Chapter:** [`../book/17-spark-write-path-and-output-files.md`](../book/17-spark-write-path-and-output-files.md) · **Example:** [`../../examples/pyspark/partitioning-demo/README.md`](../../examples/pyspark/partitioning-demo/README.md)
