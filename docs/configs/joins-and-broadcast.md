# Joins And Broadcast Configs

## Primary Configs

- `spark.sql.autoBroadcastJoinThreshold`
  - **Controls**: when Spark may broadcast a side of a join based on estimated size.
  - **When to change**: only when you can justify runtime sizes; otherwise prefer data/plan fixes.
  - **Validate in Spark UI**: SQL tab join operator (BroadcastHashJoin vs SortMergeJoin), Stages shuffle bytes reduction, Executors GC stability.

- `spark.sql.broadcastTimeout`
  - **Controls**: how long executors wait for broadcasts.
  - **When to change**: if broadcasts are correct but timing out under load (rare; fix root causes first).

## Failure Modes

- Broadcasting a “small” table that grows → executor OOM / high GC.
- Disabling broadcast unintentionally → shuffle join regression.

## UI-First Debugging Notes

In SQL tab:

- confirm the join strategy (don’t guess)
- find the `Exchange` operators; broadcast often removes a big shuffle join stage

In Stages tab:

- shuffle-heavy join stages should shrink or disappear when broadcast is correct
