# Sample `EXPLAIN FORMATTED` shapes (annotated)

These are **illustrative** fragments from small local runs. Your operator ids and
`statistics` lines will differ; focus on the **join family** and **Exchange** count.

## Broadcast hash join

You want to see a **build side broadcast** and **no** `Exchange` that repartitions the fact
rows **for the join key** (a scan may still be distributed by input splits).

```text
+- BroadcastHashJoin [campaign_id#...], [campaign_id#...], Inner, ...
   :- Filter (isnotnull(...))
   :  +- FileScan ... events ... PartitionFilters: [event_date#... = 2026-04-25]
   +- BroadcastExchange ...
      +- ... FileScan ... campaigns ...
```

**Production lesson:** this plan is fast when the **broadcast** truly fits. If
`autoBroadcastJoinThreshold` or stats are wrong, you get a big `BroadcastExchange` and OOM risk.

## Sort-merge join (broadcast disabled)

With `spark.sql.autoBroadcastJoinThreshold = -1`, the same join shape becomes **two** shuffles
and **two** sorts (typical for large sides):

```text
+- SortMergeJoin [campaign_id#...], [campaign_id#...], Inner
   :- Sort [campaign_id ASC]
   :  +- Exchange hashpartitioning(campaign_id#..., 8)   -- or 200, etc.
   :     +- ... events ...
   +- Sort [campaign_id ASC]
      +- Exchange hashpartitioning(campaign_id#..., 8)
         +- ... campaigns ...
```

**Tradeoff:** sort-merge is the safe default for big data, but **two** shuffles can dominate
wall time when a broadcast would have been safe. Always compare against **stats** and **row**
**counts**, not only “merge sounds heavy.”

## Common mistake

Raising the broadcast **threshold** to “fix” a slow sort-merge without measuring **build side**
**bytes** in the executed plan. The smallest fix is often **fresh stats** or a **hint**, not
more memory.
