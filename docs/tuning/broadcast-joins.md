# Broadcast Joins

Status: Draft

## Knob

Broadcast joins are primarily controlled by:

- `spark.sql.autoBroadcastJoinThreshold`: size threshold (bytes) below which Spark may choose a broadcast hash join.
- `spark.sql.broadcastTimeout`: how long executors will wait for the broadcast to complete.

Broadcast is also affected by query shape (join type, hints) and whether Spark can estimate table sizes.

## When It Helps

- Joining a large fact table to a safely small dimension table: avoids a shuffle join and can dramatically reduce runtime.
- Reducing shuffle volume and shuffle-related failure modes (fetch failures, spill).
- Stabilizing performance when the non-broadcast side is large but the broadcast side is static and small.

## When It Hurts

- The “small” table isn’t actually small:
  - broadcast can blow executor memory or cause heavy GC
  - can fail due to timeout or memory pressure
- Broadcasting wide rows or many columns increases memory pressure.
- In PySpark-heavy workloads, extra serialization/object overhead can make broadcasts more fragile.
- For highly concurrent clusters, many large broadcasts can amplify network/memory pressure.

## Validation

Validate with Spark UI:

- **SQL tab**
  - confirm the join operator is **BroadcastHashJoin** (or similar) rather than SortMergeJoin
  - confirm which side is broadcast (build side)
- **Stages tab**
  - the join should no longer be dominated by a large shuffle stage
  - shuffle read/write should drop materially for that join path
- **Executors tab**
  - watch for increased GC time or executor loss after introducing broadcast

Safety rule: only broadcast when you can justify the broadcast-side size at runtime (or enforce it with explicit limits/filters/selection).

## Real Use Case

A fact table joins to a product dimension table.

- Before: sort-merge join created a large shuffle stage with heavy spill.
- After: projecting only needed columns and broadcasting the dimension eliminated the shuffle join and cut runtime significantly.
- Guardrail: log/metric the estimated size of the broadcast side and alert if it grows beyond a safe budget.
