# Sample output (illustrative)

`repartition_vs_coalesce.py` might print:

```text
initial partitions 1
after repartition(4) 4
after coalesce(2) from 4 2
```

`output_file_count_demo.py` (structure depends on Spark; often one file per `coalesce` for tiny data):

```text
coalesce(8) -> 8 data parquet files under /tmp/.../eight-parts
coalesce(1) -> 1 data parquet files under /tmp/.../one-part
```

Your file counts can differ with Spark version and the small-range dataset; the **trend** is what
matters: **coalesce(1)** collapses to a single **write** task and usually one visible file in
trivial runs.
