# Advanced Spark Questions

This question set defines the first roadmap for the Spark Engineering Playbook. The goal is not to memorize short answers. Each answer should explain the execution model, production tradeoffs, performance and tuning implications, debugging signals, best practices, and a realistic use case.

## 1. Spark Execution Model

- Explain the difference between job, stage, and task in Spark.
- What causes Spark to create a new stage?
- What is a wide transformation vs a narrow transformation?
- Why do `groupBy`, `join`, and `distinct` usually cause a shuffle?
- What happens internally when you call an action like `count()`?
- How does Spark decide the number of tasks for a stage?
- What is the role of the driver?
- What is the role of the executor?
- What happens if the driver dies?
- What happens if one executor dies during a job?

## 2. Shuffle And Performance

- What is a Spark shuffle?
- Why is shuffle expensive?
- What files are created during shuffle?
- How does Spark handle shuffle spill to disk?
- What causes `ExecutorLostFailure` during shuffle?
- What is `spark.sql.shuffle.partitions`?
- Why is the default value `200` sometimes bad?
- How do you choose a good number of shuffle partitions?
- What is shuffle fetch failure?
- How do you debug a slow shuffle stage?

## 3. Partitioning

- What is the difference between Spark partitioning and table partitioning in Iceberg, Hive, or Glue?
- What is the difference between `repartition()` and `coalesce()`?
- When would you use `repartition(n)`?
- When would you use `repartition(col)`?
- When would you use `coalesce()`?
- Can too many partitions hurt performance?
- Can too few partitions hurt performance?
- How do you detect partition skew?
- What is the difference between input partitions and shuffle partitions?
- How does Spark read many small files?

## 4. Joins

- Explain the different join strategies in Spark.
- What is a broadcast hash join?
- When does Spark choose broadcast join?
- What is `spark.sql.autoBroadcastJoinThreshold`?
- What happens if the broadcast table is too large?
- What is a sort-merge join?
- What is a shuffled hash join?
- Why does Spark often use sort-merge join for large tables?
- How do you optimize a join between a huge table and a small table?
- How do you optimize a join between two huge tables?

## 5. Data Skew

- What is data skew?
- How do you detect skew from the Spark UI?
- How do you detect skew from data profiling?
- Why does one task run much longer than others?
- How can salting fix skew?
- What are the downsides of salting?
- How does Adaptive Query Execution handle skewed joins?
- What is `spark.sql.adaptive.skewJoin.enabled`?
- When is Adaptive Query Execution not enough to fix skew?
- How would you handle a celebrity key problem?

Celebrity key example:

```python
customer_id = "123"  # millions of rows
```

If one `customer_id` has millions of records, all rows for that key may go to one task during a join or aggregation.

## 6. Adaptive Query Execution

- What is Adaptive Query Execution?
- What problems does Adaptive Query Execution solve?
- How does Adaptive Query Execution coalesce shuffle partitions?
- How does Adaptive Query Execution optimize skew joins?
- How can Adaptive Query Execution switch join strategies at runtime?
- What is `spark.sql.adaptive.enabled`?
- Can Adaptive Query Execution make a query slower?
- Why might Adaptive Query Execution not activate?
- How do you verify Adaptive Query Execution from the Spark UI?
- What is the difference between the initial physical plan and final adaptive plan?

## 7. Memory Management

- Explain Spark executor memory.
- What is the difference between execution memory and storage memory?
- What is memory overhead?
- Why do PySpark jobs need more memory overhead?
- What causes `OutOfMemoryError`?
- What causes `GC overhead limit exceeded`?
- What is spill?
- Is spill always bad?
- How do you debug memory pressure?
- How do you decide executor memory size?

## 8. File Formats

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

## 9. Spark SQL And Catalyst

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

## 10. Caching And Persistence

- What is the difference between `cache()` and `persist()`?
- When should you cache a DataFrame?
- When should you avoid caching?
- What storage levels exist?
- What happens if cached data does not fit in memory?
- How do you unpersist?
- Why can caching make a job slower?
- Does Spark automatically cache intermediate results?
- Is caching useful before one action?
- How do you verify cache usage from the Spark UI?

## 11. Spark On AWS EMR And YARN

- What is the difference between Spark client mode and cluster mode?
- Where does the driver run in client mode?
- Where does the driver run in cluster mode?
- Why can client mode fail from a notebook?
- What happens during `spark-submit`?
- What are ApplicationMaster and containers in YARN?
- How do executor cores affect parallelism?
- How do executor instances affect parallelism?
- How do you size executors on EMR?
- How do you debug failed Spark jobs from YARN logs?
- What is the difference between EMR steps, `spark-submit`, and notebook-submitted jobs?
- How do EMR release versions affect Spark, Hadoop, Python, Java, and connector compatibility?
- How do core, task, and primary nodes affect Spark behavior on EMR?
- When would you use Spot task nodes, and what failure modes should you expect?

## 12. Production Debugging

- A Spark job was fast yesterday but slow today. How do you debug?
- A job fails with OOM. What do you check first?
- A join stage has one task running for 40 minutes while others finish in 2 minutes. What is likely happening?
- A job creates 50,000 small Parquet files. Why?
- A job reads 10 TB but outputs only 10 GB. How would you optimize?
- A job spills heavily to disk. What options do you have?
- A job is slow but no executor is using much CPU. What could be wrong?
- A job has high CPU but low IO. What could be happening?
- A Spark job fails only in production, not dev. Why?
- How would you create a checklist for Spark job failure triage?

## 13. Iceberg And Spark

- How does Spark write to an Iceberg table?
- What is the difference between Spark partitioning and Iceberg hidden partitioning?
- What is Iceberg metadata?
- What are manifests and manifest lists?
- What is snapshot isolation in Iceberg?
- How does Iceberg support time travel?
- Why does Iceberg avoid Hive-style partition problems?
- What is `MERGE INTO` in Iceberg?
- Why can `MERGE INTO` be expensive?
- How would you optimize a large Iceberg merge?

## 14. Structured Streaming

- What is the difference between batch Spark and Structured Streaming?
- What is a micro-batch?
- What is checkpointing?
- What is watermarking?
- What is stateful processing?
- What happens if checkpoint data is deleted?
- How does Spark guarantee fault tolerance in streaming?
- What is exactly-once processing?
- Is exactly-once always truly exactly-once end-to-end?
- How would you write Kafka data to Iceberg safely?

## 15. Staff-Level Spark Engineering

- How would you design a reusable Spark platform for multiple teams?
- How would you standardize Spark job observability?
- What metrics should every production Spark job emit?
- How would you prevent bad Spark jobs from overloading a shared EMR or YARN cluster?
- How would you enforce small-file control across pipelines?
- How would you design automatic Spark failure diagnosis?
- How would you create a Spark tuning guide for your company?
- How would you migrate legacy full reload jobs to incremental merge jobs?
- How would you design a data quality gate before writing to gold tables?
- How would you make Spark jobs cheaper without reducing reliability?

## 16. Data Correctness And Idempotency

- What does it mean for a Spark job to be idempotent?
- How do you safely retry a failed Spark write?
- How do you prevent partial output from corrupting downstream tables?
- What is the difference between append, overwrite, dynamic partition overwrite, and merge?
- How do you design a backfill so it does not duplicate or lose data?
- How do you validate row counts, null rates, duplicate keys, and business invariants?
- How do you handle schema drift in production?
- How do you design a data quality gate before publishing a table?
- How do you reconcile Spark output with a source-of-truth system?
- How do you make failures safe when a pipeline writes multiple tables?

## 17. Spark Write Path And Output Files

- What happens internally when Spark writes files?
- Why does each task usually write one or more output files?
- What is a commit protocol?
- Why are object stores different from HDFS for Spark writes?
- What causes duplicate, temporary, or orphan files?
- How can speculative execution and task retries affect writes?
- How do you control output file size?
- How do you safely overwrite partitions?
- What is the difference between writing a DataFrame and writing to a table format like Iceberg or Delta?
- How do you debug a job that writes far more files than expected?

## 18. S3 With Spark On EMR

- Why is S3 not the same as HDFS?
- Why are rename operations expensive or unsafe on object stores?
- How do list operations affect Spark planning and runtime?
- How do small files affect object-store cost and latency?
- What are common symptoms of S3 throttling?
- How do you tune Spark for S3-heavy workloads on EMR?
- How do EMRFS and S3A committers reduce S3 write problems?
- How do table formats like Iceberg, Delta, and Hudi reduce object-store risks?
- How do you design a pipeline to avoid excessive object-store metadata operations?
- What CloudWatch and Spark metrics would you monitor for S3 bottlenecks?

## 19. Statistics And Cost-Based Optimization

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

## 20. Dependency Management And Packaging

- How do you package a Spark application for production?
- What causes dependency conflicts in Spark jobs?
- What is the difference between driver classpath and executor classpath?
- How do Python dependencies get distributed to executors?
- Why can a job work locally but fail on EMR/YARN?
- How do Java, Scala, Python, and Spark version compatibility issues show up?
- How do you manage third-party connectors such as Iceberg, Delta, Kafka, or JDBC drivers?
- How do you make Spark builds reproducible?
- How do you handle secrets and environment-specific configuration during deployment?
- How do you debug `ClassNotFoundException`, `NoSuchMethodError`, or Python module import failures?

## 21. Security And Governance

- How do Spark jobs access data securely?
- How do IAM roles, instance profiles, and EMR security configuration affect Spark jobs?
- How do you prevent secrets from leaking into Spark logs?
- How do you enforce table-level, row-level, and column-level access control?
- How do you handle PII or sensitive data in Spark pipelines?
- How do you audit who read or wrote a dataset?
- How do encryption at rest and encryption in transit affect Spark architecture?
- How do you design secure access for shared EMR/YARN environments?
- How do you separate developer, staging, and production permissions?
- How do you design governance for bronze, silver, and gold datasets?

## 22. Testing And CI/CD

- How do you unit test Spark transformations?
- What should be tested with a local Spark session vs an integration environment?
- How do you create small but representative test datasets?
- How do you test skew, late data, schema evolution, duplicate input, and null-heavy data?
- How do you make Spark tests deterministic?
- How do you test streaming queries?
- How do you validate query plans or output file counts in tests?
- How do you run Spark tests in CI without making them slow or flaky?
- How do you promote Spark jobs from dev to staging to production?
- What should a Spark deployment rollback strategy look like?

## 23. Data Modeling And Table Design

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

## 24. Incremental Processing And Backfills

- How do you process only changed data?
- What is a high-watermark?
- What are late-arriving records?
- How do you design replayable Spark pipelines?
- How do you backfill one year of data without breaking current production runs?
- How do you reconcile incremental output with source-of-truth data?
- How do you handle deletes and updates in an incremental pipeline?
- How do you make backfills idempotent?
- How do you isolate backfill resources from daily production workloads?
- How do you validate that a backfill produced the same result as a full reload?

## 25. Cluster And Workload Isolation

- How do you separate interactive, batch, streaming, and backfill workloads?
- What is fair scheduling in Spark?
- How do queues work in YARN?
- How do EMR managed scaling, YARN queues, core nodes, and task nodes affect workload isolation?
- How do you prevent one team's Spark job from starving others?
- How do you set guardrails for executor count, memory, cores, runtime, and shuffle size?
- How do you design per-team cost attribution?
- How do you isolate high-priority pipelines from exploratory workloads?
- How do you manage autoscaling without hurting streaming or latency-sensitive jobs?
- How do you design cluster policies for a shared Spark platform?

## Answer Standard

Each answered section should include:

- Direct answer.
- Internal Spark behavior.
- Production tradeoffs.
- Performance and tuning guidance.
- Debugging signals.
- Best practices.
- Example code or architecture.
- Real use case.
