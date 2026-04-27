# Spark SQL review (PR checklist)

- [ ] **Read path** — Partition or table filters present for large scans; not `SELECT *` in hot paths.
- [ ] **EXPLAIN** — `FileScan` shows `PartitionFilters` / `PushedFilters` when expected; `ReadSchema` is
  minimal.
- [ ] **Joins** — **Join** condition is **equi-join** as intended; no accidental **or**-logic explosion.
- [ ] **Subqueries** — **Correlated** subqueries and **repeated** scans are not doing surprise full scans.
- [ ] **UDFs** — **Native** expressions preferred; UDFs justified and **deterministic** for the engine.
- [ ] **Windows** — **Partition** keys for `Window` are the minimal set; **order** and **frame** are intended.
- [ ] **AQE** — **Adaptive** settings match platform default unless **documented** exception.
- [ ] **Stats** — **ANALYZE** / table stats **fresh** for large dimension/fact used in CBO.
- [ ] **Hints** — **Broadcast** / **merge** **hints** have a one-line **reason** in the PR.
- [ ] **Write** — `INSERT` / `MERGE` **predicate** is selective for **table**-level updates.

**See:** [`../observability/physical-plans.md`](../observability/physical-plans.md),
[`../book/09-spark-sql-and-catalyst.md`](../book/09-spark-sql-and-catalyst.md)
