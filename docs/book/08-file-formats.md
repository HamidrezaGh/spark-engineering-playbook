# File Formats

Status: First Draft
Level: Senior to Staff
Covers: Parquet, CSV, column pruning, predicate pushdown, compression, small files, schema evolution

## Core Idea

File format is a performance and correctness decision. Columnar formats such as Parquet are usually better for analytics than row-oriented text formats such as CSV because they support column pruning, predicate pushdown, compression, typed schemas, and metadata-driven reads.

## Key Takeaways

- **Parquet is usually better than CSV for analytics** because Spark can skip columns and sometimes row groups.
- **Small files are a performance and cost problem on S3**.
- **Compression is a CPU/storage tradeoff**, not a universal default.
- **Schema evolution needs compatibility discipline**, especially in shared tables.

## Mental Model

CSV is plain text. Spark must parse rows, infer or apply schema, and read full records even if a query only needs a few columns.

Parquet is columnar. Data is stored by column inside row groups and pages, with metadata such as schema, statistics, encodings, and compression. Spark can often skip columns and row groups that are irrelevant to the query.

```text
Parquet file
|-- footer metadata: schema, row groups, column stats
|-- row group 1
|   |-- column chunk: customer_id
|   |-- column chunk: event_date
|   |-- column chunk: amount
|-- row group 2
    |-- column chunk: customer_id
    |-- column chunk: event_date
    |-- column chunk: amount
```

| Format | Strength | Weakness |
| --- | --- | --- |
| CSV | Human-readable interchange | Slow parsing, weak schema, poor pruning |
| Parquet | Column pruning, compression, stats | Needs compaction and schema discipline |
| JSON | Flexible nested interchange | Expensive parsing, schema drift risk |

## What Spark Does Internally

Column pruning means Spark reads only the columns required by the query. Predicate pushdown means Spark pushes filters to the file scan so file or row-group metadata can skip data before full decoding.

Parquet stores metadata at the file level and row-group/page level. Min/max statistics and null counts can help skip work when filters align with available statistics.

## Why It Matters In Production

The same query can read dramatically different amounts of data depending on file format and layout. Reading one column from Parquet can be much faster than reading all columns because Spark avoids unnecessary column chunks. With CSV, Spark often pays more parsing and IO cost.

Small files are bad because they increase planning overhead, object-store listing, task scheduling overhead, metadata pressure, and inefficient reads.

## Common Failure Modes

- CSV schema inference changes between runs.
- Small-file explosion slows both readers and writers.
- Compression choice makes reads CPU-bound or storage-heavy.
- Schema evolution breaks readers when fields are renamed or type changes are incompatible.
- Predicate pushdown does not help because data is poorly clustered or stats are missing.

## Tuning And Configuration

Compression tradeoffs:

- Snappy: fast, splittable in columnar formats, common default for balanced analytics.
- ZSTD: better compression ratio, often good for storage and scan cost, with more CPU cost.
- Gzip: high compression for text, but often slower and less suitable for large analytic scans.

Compact small files into larger files sized for your storage and query engine. Use table-format maintenance procedures where available.

## Spark UI Signals

Check:

- Input bytes vs expected data size.
- Number of files scanned.
- Scan time in SQL plan.
- File pruning or partition pruning behavior.
- Task count for scan stages.

## Best Practices

- Prefer Parquet, ORC, or table formats using columnar files for analytics.
- Provide explicit schemas for CSV and JSON.
- Compact small files.
- Choose compression based on workload: CPU, storage cost, and read latency.
- Test schema evolution with representative readers.

## Anti-Patterns

- Using CSV as a long-term analytical storage format.
- Writing one tiny file per partition, tenant, or batch.
- Assuming partition pruning and predicate pushdown are the same thing.
- Renaming columns without checking downstream schema evolution behavior.

## Example

```python
df.select("customer_id", "event_date", "amount") \
  .write \
  .mode("overwrite") \
  .option("compression", "zstd") \
  .parquet("s3://lake/transactions_parquet/")
```

This writes typed, columnar data. Whether ZSTD is better than Snappy depends on CPU budget, storage cost, and query patterns.

## Interview-Style Questions Covered

- Why is Parquet better than CSV for analytics?
- What is column pruning?
- What is predicate pushdown?
- How does Parquet store metadata?
- What are row groups and pages in Parquet?
- Why are small files bad?
- How do you compact small files?
- What is the compression codec tradeoff between Snappy, ZSTD, and Gzip?
- Why can reading one column from Parquet be faster than reading all columns?
- How does schema evolution work with Parquet?

## Real Use Case

A reporting job reads 8 TB of raw CSV every morning to compute five columns of metrics. Converting the source to Parquet with explicit schema, compacted files, and date partitioning reduces scan bytes, removes repeated parsing cost, and lets Spark read only the columns required by each report.
