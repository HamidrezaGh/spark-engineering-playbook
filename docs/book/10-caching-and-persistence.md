# Caching And Persistence

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- When caching helps (real reuse) vs when it only steals memory and makes jobs slower.
- What “cache is lazy” means and how to intentionally materialize it.
- How storage levels differ (memory-only vs memory+disk vs serialized) and what risks each introduces.
- How to verify caching actually happened (Storage tab / in-memory relation in the plan).
- When to `unpersist()` and why long-lived caches can cause production incidents.

## Core Idea

Caching stores a DataFrame, Dataset, or RDD after it is computed so later actions can reuse it without recomputing the full lineage. It helps only when reuse is real and the cached data is worth the memory or disk it consumes.

## Key Takeaways

- **Cache only reused expensive intermediates**.
- **Caching is lazy**; an action must materialize it.
- **Caching can make jobs slower** by stealing memory from execution.
- **Always unpersist when reuse is finished**.

## Mental Model

`cache()` is shorthand for persisting with a default storage level. `persist()` lets you choose a storage level, such as memory only, memory and disk, serialized, replicated, or disk only depending on API and Spark version.

Caching is lazy. Spark does not populate the cache until an action materializes it.

```text
Expensive DataFrame
  |
  |-- Reused by multiple actions?
  |      -> no: do not cache
  |
  |-- yes: does it fit safely in memory?
         -> yes: cache / memory storage
         -> no: persist with memory-and-disk or reconsider

After reuse:
  -> unpersist when done
```

| Cache Decision | Good Signal | Bad Signal |
| --- | --- | --- |
| Cache | Same expensive DataFrame reused | Only one action |
| Persist memory+disk | Recompute is expensive, data may not fit | Disk pressure already high |
| Do not cache | Cheap lineage or no reuse | Memory needed for joins/shuffles |

## What Spark Does Internally

Spark stores cached partitions on executors. If cached data does not fit, behavior depends on storage level. It may evict partitions, spill to disk, or recompute missing partitions later. Cached data competes with execution memory and other cached datasets.

Spark does not automatically cache every intermediate result. It recomputes lineage unless you explicitly cache, persist, checkpoint, or materialize output.

## Why It Matters In Production

Caching can remove repeated expensive scans, joins, or feature generation. It can also make jobs slower by consuming memory needed for execution, causing eviction, spill, GC, or recomputation.

Caching before one action is usually useless because there is no reuse.

## Common Failure Modes

- Cache never used because the logical plan differs between actions.
- Cache materialization cost exceeds reuse benefit.
- Cached data evicts more important data.
- Executors OOM because cache and execution compete.
- User forgets to unpersist long-lived cached data.

## Tuning And Configuration

Choose storage level based on size, reuse, and recomputation cost.

- Memory-only: fastest when it fits.
- Memory-and-disk: safer for larger data.
- Serialized: less memory, more CPU.
- Disk-only: useful when recomputation is very expensive but memory is constrained.

Materialize intentionally with an action such as `count()` only when the upfront cost is justified.

## Spark UI Signals

Use the Storage tab to verify:

- Cached RDD/DataFrame exists.
- Fraction cached.
- Memory used.
- Disk used.
- Number of cached partitions.

In SQL plans, check whether Spark reads from an in-memory relation.

## Best Practices

- Cache only reused expensive intermediates.
- Unpersist when done.
- Cache after filters/projections to reduce size.
- Measure before and after.
- Prefer checkpointing over caching when lineage truncation is the main goal.

## Anti-Patterns

- Caching every DataFrame.
- Caching before a single action.
- Caching raw wide data before filtering.
- Forgetting that cache is per application and executor lifecycle.
- Assuming cache survives application restarts.

## Example

```python
features = (
    events.filter("event_date >= '2026-01-01'")
          .select("customer_id", "event_type", "event_ts")
          .cache()
)

features.count()  # materialize intentionally

model_input = build_model_input(features)
quality_report = build_quality_report(features)

features.unpersist()
```

Caching is justified because two downstream actions reuse the same filtered feature set.

## Self-check (concept review)

- What is the difference between `cache()` and `persist()`?
- When should you cache a DataFrame?
- When should you avoid caching?
- What storage levels exist?
- What happens if cached data does not fit in memory?
- How do you unpersist?
- Why can caching make a job slower?
- Does Spark automatically cache intermediate results?
- Is caching useful before one action?
- How do you verify cache usage from Spark UI?

## Real Use Case

A churn modeling pipeline computes a customer feature table from 18 months of events, then uses it for training, profiling, and quality reports. Caching the filtered and projected feature DataFrame saves repeated scans and joins. Caching the raw event table would be wasteful because it is wider, larger, and not reused in that form.
