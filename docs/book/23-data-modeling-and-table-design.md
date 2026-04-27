# Data Modeling And Table Design

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- Why table design is a long-term performance/cost lever for Spark workloads.
- How to choose partitions/clustering/bucketing based on query and write patterns.
- What “bronze/silver/gold” contracts mean operationally (correctness, SLA, maintenance).
- What anti-patterns create small-file and metadata disasters (high-cardinality partitioning).
- What to check first when a table becomes slow to read (pruning, file counts, stats, layout).

## Core Idea

Table design determines how expensive future Spark jobs will be. Good modeling balances query performance, write cost, table maintenance, correctness, and downstream usability.

## Key Takeaways

- **Table design is a long-term cost decision**.
- **Partitioning helps only when it matches query and write patterns**.
- **Bronze, silver, and gold tables should have different contracts**.
- **High-cardinality partitioning usually creates metadata and small-file problems**.

## Mental Model

Bronze tables preserve raw or lightly normalized data. Silver tables apply cleaning, deduplication, conformance, and business keys. Gold tables serve curated analytics, reporting, ML features, or product-facing data.

Partitioning is useful when it matches common filters and keeps partition sizes healthy. It is harmful when it creates tiny partitions, high metadata overhead, or poor pruning.

```text
Bronze: raw / landing
  -> Silver: cleaned / conformed
      -> Gold: curated serving tables
          |-- BI / reporting
          |-- ML features
          |-- product data
```

| Design Choice | Optimizes For | Watch Out |
| --- | --- | --- |
| Partitioning | Pruning and write organization | Small partitions |
| Clustering/sorting | Locality within files | Maintenance cost |
| SCD modeling | Historical correctness | Complex merge logic |
| Gold aggregation | Consumer speed | Rebuild/backfill complexity |

## What Spark Does Internally

Spark uses table layout, file metadata, partition metadata, and query filters to plan scans. Better layout reduces files scanned and bytes read. Poor layout forces Spark to scan too much data or manage too many files.

Slowly changing dimensions and fact tables need clear update semantics. Incremental facts often use event time, ingestion time, high-watermarks, and merge keys.

## Why It Matters In Production

Bad table design creates recurring costs:

- Slow reads.
- Expensive merges.
- Excessive compaction.
- Broken downstream contracts.
- Small-file maintenance.
- Inefficient backfills.

## Common Failure Modes

- Partitioning by high-cardinality columns.
- Partitioning by a column rarely used in filters.
- Gold tables depending directly on messy bronze data.
- SCD logic missing effective dates or current flags.
- Table evolution breaks downstream consumers.

## Design Guidance

Choose table layout based on:

- Query predicates.
- Update and merge patterns.
- Data volume per partition.
- File size targets.
- Late-arriving data.
- Retention and recovery requirements.
- Consumer expectations.

Partitioning, bucketing, clustering, sorting, and Z-ordering solve different problems. Use them when they match actual access patterns and maintenance budget.

## Operating Signals

Monitor:

- Files per table and partition.
- Average file size.
- Query pruning effectiveness.
- Merge files scanned and rewritten.
- Compaction frequency.
- Schema changes and downstream failures.

## Best Practices

- Design bronze, silver, and gold tables with different contracts.
- Keep partition transforms low-cardinality enough to manage.
- Model facts and dimensions explicitly.
- Document table ownership and SLAs.
- Evolve schemas with compatibility in mind.

## Anti-Patterns

- Partitioning by user ID.
- Creating gold tables as direct copies of raw source tables.
- Changing column meaning without versioning or communication.
- Optimizing layout for one query while breaking common workloads.

## Example

```sql
CREATE TABLE gold.daily_revenue (
  revenue_date DATE,
  country STRING,
  product_id STRING,
  gross_revenue DECIMAL(18,2)
)
USING iceberg
PARTITIONED BY (days(revenue_date));
```

Daily partitioning fits common time-window analytics and keeps partition pruning simple.

## Self-check (concept review)

- How do you choose table partition columns?
- When is partitioning harmful?
- How do you design bronze, silver, and gold tables?
- How do you model slowly changing dimensions in Spark?
- How do you design fact tables for incremental processing?
- How do you balance query performance, write cost, and maintenance cost?
- How do you choose between partitioning, bucketing, clustering, sorting, and Z-ordering?
- How do you design tables for both batch and streaming consumers?
- How do table layout decisions affect compaction and vacuuming?
- How do you evolve a table design without breaking downstream consumers?

## Real Use Case

A product analytics table is partitioned by `user_id`, creating millions of tiny partitions and slow planning. Most queries filter by event date and product area. The redesign uses date partitioning plus clustering by product and user where supported, improving pruning and reducing metadata overhead.
