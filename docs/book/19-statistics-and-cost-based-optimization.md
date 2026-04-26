# Statistics And Cost-Based Optimization


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What decisions Spark can make better with stats (broadcast, join order/strategy, selectivity).
- Why stale stats are dangerous (plans look “intentional” but are wrong).
- What stats to care about (table size, row counts, column NDV, null counts) and where they come from.
- When to use hints (and why they need measurement + documentation).
- How to debug “Spark picked a weird join” by checking stats and the physical plan.

## Core Idea

Spark's optimizer makes better decisions when it has useful statistics. Cost-based optimization uses table size, row counts, column stats, and estimates to choose join order, join strategy, and plan shape.

## Key Takeaways

- **Missing stats can cause bad join strategies**.
- **Stale stats are worse than no mental model** because the plan looks intentional but is wrong.
- **Table-format metadata helps scan pruning**, while catalog stats help optimizer planning.
- **Hints should be measured and documented**, not used as permanent guesswork.

## Mental Model

Without stats, Spark relies on defaults and heuristics. With accurate stats, Spark can estimate whether a table is broadcastable, which join order is cheaper, and how selective filters may be.

```text
Table and column stats
  -> Catalyst / CBO
      |-- join order
      |-- join strategy
      -> physical plan

Table-format metadata
  -> scan planning
      |-- file pruning
      -> physical plan
```

| Missing Signal | Bad Plan Risk | Fix |
| --- | --- | --- |
| Table size | Missed broadcast join | Refresh table stats |
| Column cardinality | Bad join order | Column stats where supported |
| File stats | Weak pruning | Compact/sort/maintain table metadata |
| Runtime stats | Poor static estimate | AQE plus plan verification |

## What Spark Does Internally

Catalyst creates candidate plans and applies rules. When cost-based optimization is available and stats exist, Spark can compare plan costs. Runtime AQE can improve decisions after shuffle statistics are available, but the initial plan still matters.

Table formats may expose metadata statistics that help file pruning and scan planning. Catalog statistics help the Spark optimizer at the logical planning level.

## Why It Matters In Production

Missing or stale stats can cause:

- Sort-merge join instead of broadcast.
- Wrong build side for hash joins.
- Bad join order.
- Excessive shuffle.
- Scanning files that could be pruned.

## Common Failure Modes

- Table grows but stats still show old size.
- Column distribution changes and selectivity estimates become wrong.
- Optimizer misses broadcast opportunity.
- Hint fixes one case but breaks another.
- File-level stats exist but query predicates do not align with layout.

## Tuning And Configuration

Maintain statistics for important tables where Spark uses them. Refresh stats after major loads, compactions, or backfills. Use hints when:

- Stats are unavailable or known wrong.
- You have measured the desired strategy.
- The hint is documented and covered by regression checks.

Do not use hints as a substitute for understanding the plan.

## Spark UI Signals

Check:

- Physical join strategy.
- Estimated vs actual row counts where available.
- Broadcast decisions.
- Scan file counts.
- AQE changes between initial and final plan.

## Best Practices

- Keep stats current for large shared tables.
- Validate join strategy for critical queries.
- Use table-format metadata pruning where available.
- Document optimizer hints.
- Compare planned vs actual data sizes.

## Anti-Patterns

- Assuming the optimizer knows table sizes without stats.
- Leaving stale stats after large backfills.
- Overusing hints globally.
- Ignoring AQE final plans.

## Example

```sql
ANALYZE TABLE sales COMPUTE STATISTICS;
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS customer_id, country;
```

The exact support and behavior depends on catalog and table format, but the principle is to give the optimizer trustworthy information.

## Interview-Style Questions Covered

- What statistics can Spark use during query planning?
- What is cost-based optimization?
- When can missing or stale statistics cause a bad join strategy?
- How do table size and column statistics affect broadcast joins?
- How do statistics affect join order?
- How do you collect or refresh table statistics?
- What is the difference between Spark catalog statistics and table-format metadata statistics?
- How do Iceberg or Delta statistics help with query pruning?
- How do you diagnose a bad plan caused by wrong cardinality estimates?
- When should you override the optimizer with hints?

## Real Use Case

A query joins a 5 TB fact table to a 5 MB dimension table, but Spark uses sort-merge join because the dimension table has no stats and appears large. Refreshing stats or using a documented broadcast hint avoids a large shuffle and cuts runtime dramatically.
