# Diagrams

Use this directory for diagrams that explain Spark execution, shuffle flow, join strategies, lakehouse table mechanics, and streaming state management. Every diagram in this directory is paired with a chapter in the book and is designed to teach a production concept, not to decorate a slide deck.

## Conventions

- One concept per diagram. Composite "everything in one picture" diagrams are not useful for incidents or design reviews.
- Mermaid for structural diagrams (job/stage/task hierarchy, dataflow, decision trees) — readable in the GitHub Markdown renderer with no extra tooling.
- ASCII text diagrams in chapter source for inline explanations where Mermaid would be heavier than the concept warrants.
- Each Mermaid diagram in this directory ships as a `.md` file with: a short explanation, an engineering-focused level-3 Markdown heading (never a generic “diagram” label), a fenced mermaid code block, a one-line caption when it clarifies production signal, how to use it in the relevant chapter, and a "production interpretation" section that anchors the diagram to real failure modes.

## Index

| Diagram | Pairs With | What It Teaches |
| --- | --- | --- |
| [`spark-job-stage-task.md`](spark-job-stage-task.md) | [Chapter 1 — Execution Model](../docs/book/01-execution-model.md) | The job → stage → task hierarchy and how each maps to a Spark UI tab. |
| [`shuffle-read-write.md`](shuffle-read-write.md) | [Chapter 2 — Shuffle And Performance](../docs/book/02-shuffle-and-performance.md) | What "shuffle write" and "shuffle read" actually mean (executor-local disk + network), and why fetch-failure cascades happen. |
| [`broadcast-vs-sort-merge-join.md`](broadcast-vs-sort-merge-join.md) | [Chapter 4 — Joins](../docs/book/04-joins.md) | Side-by-side mechanics of the two most common production join strategies. |
| [`iceberg-merge-on-s3.md`](iceberg-merge-on-s3.md) | [Chapter 13 — Iceberg And Spark](../docs/book/13-iceberg-and-spark.md), [`emr-merge-memory-spill.md`](../docs/case-studies/emr-merge-memory-spill.md) | What an Iceberg `MERGE` actually does on S3, and where it fails operationally. |
| [`structured-streaming-checkpoint-state.md`](structured-streaming-checkpoint-state.md) | [Chapter 14 — Structured Streaming](../docs/book/14-structured-streaming.md), [`streaming-state-blowup.md`](../docs/case-studies/streaming-state-blowup.md) | How checkpoints, state stores, and watermarks tie together each micro-batch. |

## Contributing A Diagram

A diagram belongs in this directory if:

- It teaches a production concept that a staff engineer would draw on a whiteboard during a design review or incident.
- It can stand alone with a short explanation, the diagram itself, and a production interpretation.
- The Mermaid source renders correctly in GitHub.

A diagram does not belong here if it is decorative, vendor-specific, or duplicates content already explained more concisely in chapter prose.
