# Glossary

Practical terms for **reading the Spark UI**, **EXPLAIN** output, and **cluster** logs. This is
not a substitute for the [Spark documentation](https://spark.apache.org/docs/latest/).

| Term | Simple definition | Production meaning | UI / plan |
| --- | --- | --- | --- |
| **Action** | Triggers work (e.g. `count`, `write`). | End-to-end run time and side effects; every action = new **job** in the UI. | **Jobs** tab: one row per action. |
| **Application** | One `SparkContext` / `SparkSession` run. | One YARN/EMR **application**; holds all jobs in a session. | Application id in YARN; one UI 4040 per driver. |
| **AQE** (Adaptive Query Execution) | Re-optimizes the plan at runtime using real sizes. | Can fix join choice, coalesce shuffles, skew joins — *verify* in final plan. | **SQL** plan: `AdaptiveSparkPlan` / `QueryStage`. |
| **Broadcast join** | Copy small build side to all executors. | Fast, **no** shuffle of build side; **OOM** if build is not actually small. | `BroadcastHashJoin`, `BroadcastExchange`. |
| **Catalyst** | Spark SQL’s optimizer. | Turns your query into a better logical plan, then a physical plan. | **EXPLAIN** shows “Parsed → Analyzed → Optimized” (labels vary by format). |
| **Cache** | `cache()` / `persist()` in memory/disk. | **Steals** executor memory; only keep if you measure reuse. | **Storage** tab. |
| **Checkpoint (streaming)** | Durable state for fault tolerance. | **Do not** delete; must be **unique** per query; S3 in prod. | Streaming tab / driver logs. |
| **Coalesce** | Reduce **partitions** without a full shuffle. | Cheap but can put **all** data through few tasks → skew/oom. | Fewer **tasks** in a stage than `repartition` would. |
| **Driver** | JVM running the planner, scheduler, some actions. | **Bottleneck** for `collect`, large broadcasts, and huge `EXPLAIN` trees. | **Executors** tab has a driver row; high heap here is a smell. |
| **Dynamic allocation** | Add/remove executors by load. | Cost **vs** tail latency; watch removing executors during shuffle. | `spark.dynamicAllocation.*` in **Environment**; log lines about adding/removing. |
| **Exchange** | Shuffle: redistribute rows by partitioner. | **Default stage boundary**; where shuffle time and **skew** show up. | **SQL** plan `Exchange` / `ShuffleExchange` → **Shuffle read/write** in Stages. |
| **Executor** | Worker JVM running **tasks**. | If executors are **lost**, shuffle is **unsafe**; fix cluster before “tuning.” | **Executors** tab: slots, **failed tasks**, **GC**. |
| **Executor memory / overhead** | Heap + off-heap + **non-JVM** (Python) headroom. | YARN **kills** the container if **total** exceeds limit — **overhead** matters for PySpark. | Container diagnostics; pair with **GC** in UI. |
| **Iceberg manifest** | List of data files in a **snapshot** slice. | Planning reads/merges uses manifests; bloat can slow **planning**. | Iceberg metadata; not always in default Spark text plans. |
| **Iceberg metadata table** | `table_name$files`, `$snapshots`, etc. | First-class way to see **file counts** and **snapshots** without directory walks. | Queried with Spark SQL. |
| **Job** | Work for **one** action. | Cost and **failure** are usually attributed per job. | **Jobs** tab. |
| **Lineage** | History of a dataset in Spark’s logical plan. | Drives recompute when you lack **checkpoint**; also affects **column pruning**. | In **debug** of logical plan (rare in prod). |
| **Narrow transformation** | Input partition maps to a **small** set of output partitions, no shuffle. | Pipelines in one **stage** with adjacent narrow ops. | **One** stage, many pipelined ops. |
| **Partition (Spark)** | A slice of a **Dataset** in one **task** of a **stage**. | **Too few** = slow tasks, spill; **too many** = overhead, small files. | **Tasks** = input partitions; **Shuffle** = `spark.sql.shuffle.partitions`. |
| **Partition (table)** | Directory / Iceberg / Hive partition. | Drives **prune**; wrong column hurts every scan. | `PartitionFilters` on **FileScan** in **EXPLAIN**. |
| **Persist** | Like cache with chosen **StorageLevel** | Same as cache: measure **eviction** and **benefit**. | **Storage** tab. |
| **Shuffle** | Redistribute by key (sort/hash/range) across the cluster. | **Expensive** (disk + network + **CPU**); the core cost of big joins/aggregations. | **Shuffle read/write** in **Stages**; **FetchFailed** in logs. |
| **Skew** | A few keys/parts with **way** more rows. | **Long-tail** task duration; OOM on one **task**; AQE can help, not always. | **max** time ≫ **median**; one task with huge **shuffle read**. |
| **Sort-merge join** | Sort both sides and merge matching keys. | Default for two **large** sides; **two** shuffles if keys not pre-sorted. | `SortMergeJoin`, `Sort`, `Exchange` on each side. |
| **Spill** | Move sort/hash structures to **disk** when memory is tight. | **Slows** the task; often paired with too **few** partitions or skew. | **Spill** column in Stages/Tasks. |
| **Stage** | Part of a job with **no** shuffle in the middle. | You optimize **stages** one at a time; boundaries are **Exchanges** (usually). | **Stages** tab. |
| **State store (streaming)** | Durable per-key state for stateful operators. | **Grows** with keys; can OOM; needs **watermarking** to drop. | State operators in plan; size in streaming metrics. |
| **Table format (Iceberg)** | Snapshot-based ACID table. | Replaces “directory of Parquet” for **reliable** **MERGE** and **time travel**. | **Iceberg** scan/join; metadata tables. |
| **Task** | One partition of one **stage** on one **core** slot. | The row you sort by **duration** when hunting **stragglers**. | **Tasks** table in a stage. |
| **Tungsten** | Physical execution and memory in Spark SQL. | Columnar, **off-heap** buffers, **whole-stage codegen** — “why is SQL fast.” | **WholeStageCodegen** in **EXPLAIN**. |
| **Watermark (streaming)** | Time threshold to drop “too late” data for state. | Trades **completeness** for bounded **state**; must align with the product. | `watermark` in query; state size metrics. |
| **Wide transformation** | Partitions **depend** on many inputs — usually shuffles. | New **stage**; most performance incidents live here. | `Exchange` on **EXPLAIN**; new stage in **UI**. |
| **Output committer** | Coordinates **visible** output files for a job. | On **S3**, correctness and **duplicates** tie to committer + **speculation**. | `FileOutputCommitter` in logs; not always a single plan line. |
| **Compaction** | Merge small data files. | **Lowers** read cost; **adds** write cost — schedule, don’t do every run. | Iceberg `rewrite_data_files`, Delta `OPTIMIZE`. |
| **Small files** | Many tiny objects per table prefix. | **List** and **read** **amplification**; Spark planning can slow. | **Output** file count / size; **S3** list metrics. |
| **Snapshot (Iceberg)** | Consistent read pointer for a table. | **Time travel**; **concurrent** writes resolve at commit; not the same as Spark RDD `checkpoint`. | `snapshots` metadata; `AS OF` queries. |

**Plan vs UI** — Stages and **Exchange** nodes usually line up; **AQE** and **reused** exchanges need the **post-run** plan for truth.

**See also:** [`concept-map.md`](concept-map.md), [`observability/physical-plans.md`](observability/physical-plans.md)
