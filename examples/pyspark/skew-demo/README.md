# Skew demo (PySpark)

## How to run

```bash
# From repo root — adds artificial duplicates so `cust_001` is hot
python3 examples/pyspark/skew-demo/skew_demo.py

# Salted join on toy data (prints EXPLAIN)
python3 examples/pyspark/skew-demo/salted_join_fix.py
```

The full **skew detector** used in production-style profiling stays at
[`../skew_detector.py`](../skew_detector.py).

## What to observe

- **`skew_demo.py`** — max/median ratio and top-key share **before** a heavy shuffle. Compare to
  [`../../../docs/observability/spark-ui-guide.md`](../../../docs/observability/spark-ui-guide.md) where
  **max** task time ≫ **median** in the Stages view.
- **`salted_join_fix.py`** — how adding a `join_key` with a salt bucket can spread work. **Tradeoff:**
  more rows, more work to **validate**; only use with clear semantics and tests.

**Production lesson:** AQE **skew** handling helps at execution time, but you still want
**proactive** key metrics for regressions and **business** changes (e.g. a new “celebrity”
customer_id).

**Common mistake:** salting the left side but forgetting to **align** the right side, which
**silently drops** or **duplicates** join matches.

**See:** [`../../../docs/troubleshooting/skew-and-stragglers.md`](../../../docs/troubleshooting/skew-and-stragglers.md), [`../../sql/03-skew-detection.sql`](../../sql/03-skew-detection.sql)

## Sample output

[`sample_output.md`](sample_output.md)
