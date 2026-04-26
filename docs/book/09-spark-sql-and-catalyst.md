# Spark SQL And Catalyst

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What Catalyst is optimizing, and what it cannot optimize (especially around UDFs).
- How to read `explain("formatted")` and the SQL tab to find the expensive operators.
- How to spot shuffles (`Exchange`), join strategies, sorts, and scan pushdown/pruning in the plan.
- Why a query scanned “too much” data (missing pruning/pushdown, wide projections, bad layout).
- What “verify the final plan” means when AQE is enabled.

## Core Idea

Catalyst is Spark SQL's optimizer. It turns DataFrame and SQL operations into analyzed, optimized, and executable plans. Understanding these plans is the shortest path to understanding why Spark chose a join, inserted a shuffle, pushed a filter, or scanned more data than expected.

## Key Takeaways

- **Catalyst can optimize DataFrame and SQL plans** because the logic is visible to Spark.
- **`Exchange` means data movement**, usually a shuffle.
- **UDFs can hide logic from the optimizer**.
- **`explain("formatted")` is a production debugging tool**, not only a learning tool.

## Mental Model

The plan flow is:

1. Unresolved logical plan: expressions and relations before names are resolved.
2. Analyzed logical plan: tables, columns, functions, and types are resolved.
3. Optimized logical plan: Spark applies rule-based and cost-based optimizations.
4. Physical plan: Spark chooses executable operators.
5. Executed plan: runtime plan, potentially adaptive.

```text
SQL / DataFrame API
  -> unresolved logical plan
  -> analyzed logical plan
  -> optimized logical plan
  -> physical plan
  -> executed / adaptive plan
```

| Plan Layer | Question It Answers |
| --- | --- |
| Analyzed logical plan | Do columns, tables, and types resolve correctly? |
| Optimized logical plan | Did Spark simplify, prune, and push filters? |
| Physical plan | What operators will actually run? |
| Adaptive plan | What changed after runtime statistics appeared? |

## What Spark Does Internally

Catalyst applies rules such as predicate pushdown, column pruning, constant folding, projection pruning, join reordering where available, and operator simplification.

Whole-stage code generation fuses compatible operators into generated JVM code to reduce virtual function calls and improve CPU efficiency. Tungsten refers to Spark's execution engine improvements around memory management, binary processing, cache-aware execution, and code generation.

## Why It Matters In Production

DataFrame and SQL APIs can be faster than RDD APIs because Spark can inspect and optimize their logical plans. With RDDs, Spark sees opaque user functions and has fewer opportunities for column pruning, predicate pushdown, join optimization, and code generation.

## Common Failure Modes

- Python UDFs block optimization or force expensive serialization.
- Filters are not pushed because expressions are not pushdown-compatible.
- Missing stats lead to bad join strategies.
- Type casts on join keys prevent efficient planning.
- Wide projections carry unnecessary columns into shuffles.

## Tuning And Configuration

Use `explain("formatted")` for readable plans. Focus on:

- Scan operators and pushed filters.
- `Exchange` nodes for shuffles.
- Join operators.
- Sort operators.
- Aggregation strategy.
- Adaptive final plan.

Hints can guide Catalyst, but should be used sparingly and verified.

## Spark UI Signals

The SQL tab shows operator-level metrics and the physical/adaptive plan. Use it to connect code to runtime behavior. If the plan contains unexpected exchanges, sorts, or nested loop joins, investigate before tuning hardware.

## Best Practices

- Prefer DataFrame and SQL APIs for relational workloads.
- Read plans for expensive jobs.
- Project and filter early.
- Avoid UDFs when built-in functions can express the logic.
- Keep table statistics useful for optimizer decisions.

## Anti-Patterns

- Treating `explain()` as optional for production-critical queries.
- Replacing SQL with RDDs for style rather than necessity.
- Using Python UDFs for logic available in Spark SQL functions.
- Applying optimizer hints without documenting why.

## Example

```python
query = (
    orders.filter("order_date >= '2026-01-01'")
          .select("customer_id", "order_total")
          .groupBy("customer_id")
          .sum("order_total")
)

query.explain("formatted")
```

The formatted plan should show whether Spark pushed filters to the scan, pruned columns, inserted an exchange for the aggregation, and used adaptive execution.

## Interview-Style Questions Covered

- What is Catalyst Optimizer?
- What is a logical plan?
- What is an analyzed logical plan?
- What is an optimized logical plan?
- What is a physical plan?
- What is whole-stage code generation?
- What is Tungsten?
- How does Spark push filters down?
- Why can the DataFrame API be faster than the RDD API?
- How do you read an `explain("formatted")` plan?

## Real Use Case

A job reads a 200-column Parquet table but only needs six columns. The engineer verifies in `explain("formatted")` that column pruning reaches the file scan. When a Python UDF is added before filtering, scan and serialization costs increase. Rewriting the UDF using built-in Spark SQL functions restores optimizer visibility and lowers runtime.
