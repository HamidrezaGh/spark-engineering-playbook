# Adaptive Query Execution (AQE) Configs


## Primary Configs

- `spark.sql.adaptive.enabled`
  - **Controls**: whether Spark can adapt the physical plan at runtime based on observed sizes.
  - **Validate in Spark UI**: SQL tab shows adaptive plan nodes; Stages show coalesced partitions behavior.

- `spark.sql.adaptive.coalescePartitions.enabled`
  - **Controls**: coalescing small shuffle partitions to reduce tiny tasks.

## What AQE Helps With (In Practice)

- Over-partitioned shuffles: reduces scheduler overhead by coalescing.
- Some skew scenarios (feature depends on Spark version/operator): can reduce long-tail behavior, but doesn’t eliminate the need to understand hot keys.

## Common Misunderstandings

- AQE is not a substitute for:
  - fixing skewed keys
  - fixing small file layout
  - eliminating unnecessary shuffles

## UI-First Validation

- Confirm AQE is actually applied in the SQL tab (adaptive plan shown).
- Confirm the intended stage metrics moved:
  - fewer tiny tasks
  - reduced scheduler delay
  - more stable task size distribution
