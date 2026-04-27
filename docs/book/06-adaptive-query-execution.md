# Adaptive Query Execution

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What AQE changes at runtime, and what evidence it uses to decide.
- How to verify AQE from the Spark UI (initial vs final adaptive plan).
- When AQE coalescing helps (and when it can reduce parallelism too far).
- When AQE skew handling helps (and why it doesn’t fix every skew pattern).
- Why you still need to reason about shuffle, joins, and output file sizing with AQE enabled.

## Core Idea

Adaptive Query Execution, or AQE, lets Spark adjust a query plan after it observes runtime statistics. Instead of relying only on estimates from the initial plan, Spark can coalesce shuffle partitions, split skewed partitions, and change join strategies while the query is running.

## Key Takeaways

- **AQE changes the plan at runtime** after Spark sees real shuffle statistics.
- **Coalescing reduces tiny shuffle tasks**.
- **Skew handling can split large shuffle partitions** in supported plans.
- **Always verify the final adaptive plan**, not only the initial plan.

## Mental Model

The initial physical plan is Spark's best plan before execution. The final adaptive plan is what Spark actually used after collecting shuffle statistics. AQE is most useful when static estimates are wrong or incomplete.

```text
Initial physical plan
  -> run query stage
  -> collect runtime stats
  -> decide whether plan can improve
       |-- coalesce small shuffle partitions
       |-- split skewed partitions
       |-- switch join strategy
  -> final adaptive plan
```

| AQE Feature | Problem It Targets | What To Verify |
| --- | --- | --- |
| Partition coalescing | Too many tiny shuffle tasks | Final task count and output files |
| Skew join handling | Long-tail shuffle partitions | Final plan and task duration spread |
| Join conversion | Bad initial join choice | Broadcast size and executor memory |

## What Spark Does Internally

AQE inserts query stages around shuffle boundaries. Once a stage completes, Spark knows actual partition sizes and row counts for that exchange. It can then:

- Coalesce many small shuffle partitions into fewer larger tasks.
- Split skewed shuffle partitions for supported joins.
- Convert a planned sort-merge join into a broadcast hash join if one side is smaller than expected.
- Avoid unnecessary work in some adaptive scenarios.

## Why It Matters In Production

AQE reduces the need to perfectly predict runtime data sizes. It helps workloads with variable daily input, imperfect statistics, and uneven shuffle output. It is not a replacement for good data modeling, correct joins, or skew-aware design.

## Common Failure Modes

- AQE does not activate because `spark.sql.adaptive.enabled` is false.
- Query shape does not include adaptive query stages where AQE can help.
- Skew thresholds are not met even though the job feels skewed.
- Broadcast conversion causes memory pressure when the runtime size is still too large for the cluster.
- AQE coalesces partitions too aggressively and reduces parallelism.

## Tuning And Configuration

`spark.sql.adaptive.enabled` enables AQE for Spark SQL/DataFrame queries. Related settings control partition coalescing, advisory partition size, skew join handling, and broadcast conversion.

Use AQE with:

- Variable input sizes.
- Joins with uncertain table size estimates.
- Large shuffles that produce many tiny partitions.
- Moderate skew in join workloads.

Validate settings through runtime evidence, not belief. Check the final adaptive plan and task distribution.

## Spark UI Signals

In the SQL tab:

- Look for `AdaptiveSparkPlan`.
- Compare initial plan and final plan.
- Check whether join strategies changed.
- Check whether shuffle partitions were coalesced.
- Check whether skew partition splitting occurred.

In the Stages tab, compare task count before and after adaptive stages.

## Best Practices

- Keep AQE enabled for most modern SQL/DataFrame workloads unless you have a measured reason not to.
- Still maintain table statistics and good layout; AQE works better with reasonable initial plans.
- Review final plans for critical jobs.
- Treat AQE as a safety net, not as the only tuning strategy.

## Anti-Patterns

- Assuming AQE fixes all skew.
- Ignoring missing filters or bad joins because AQE is enabled.
- Tuning only the initial plan and never checking the final adaptive plan.
- Disabling AQE globally because one query regressed.

## Example

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")

result = fact.join(dim, "customer_id").groupBy("country").count()
result.explain("formatted")
```

The formatted plan can show an adaptive plan. In the Spark UI, the final plan may reveal that Spark converted a join or coalesced partitions after seeing real shuffle sizes.

## Self-check (concept review)

- What is Adaptive Query Execution?
- What problems does AQE solve?
- How does AQE coalesce shuffle partitions?
- How does AQE optimize skew joins?
- How can AQE switch join strategies at runtime?
- What is `spark.sql.adaptive.enabled`?
- Can AQE make a query slower?
- Why might AQE not activate?
- How do you verify AQE from the Spark UI?
- What is the difference between the initial physical plan and final adaptive plan?

## Real Use Case

A daily revenue pipeline has input sizes ranging from 20 GB on weekdays to 800 GB after a monthly billing close. Static shuffle partition settings are either too high for small days or too low for large days. AQE coalesces small shuffle partitions on low-volume days and preserves more parallelism on large days, reducing tuning churn while engineers still monitor skew and output file sizes.
