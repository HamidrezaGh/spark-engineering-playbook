# Documentation layout

The default **production** mental model in this repo is **Spark on YARN** (often **AWS EMR**)
with **S3**-backed data lakes and sometimes **Iceberg** or other table formats. Generic Spark
behavior is portable; failure modes and tuning often depend on the **cluster manager**, **storage**,
and **format**.

## How to navigate

- **By concept (sequential):** [`book/README.md`](book/README.md)
- **By symptom (trees):** [`troubleshooting/README.md`](troubleshooting/README.md) and
  [`field-guides/README.md`](field-guides/README.md)
- **By question:** [`concept-map.md`](concept-map.md)
- **By UI and plan:** [`observability/README.md`](observability/README.md)
- **Checklists (short):** [`checklists/README.md`](checklists/README.md)
- **Self-check depth:** [`practical-spark-questions.md`](practical-spark-questions.md) (older name:
  [`advanced-spark-questions.md`](advanced-spark-questions.md) stub)

## Directory index

- [`book/`](book/) — main handbook
- [`troubleshooting/`](troubleshooting/) — production decision trees
- [`observability/`](observability/) — Spark UI and physical plan guides
- [`field-guides/`](field-guides/) — short incident entry points
- [`patterns/`](patterns/) — reusable architecture patterns
- [`tuning/`](tuning/) — focused tuning notes
- [`configs/`](configs/) — configuration field guide
- [`checklists/`](checklists/) — operational and review checklists
- [`case-studies/`](case-studies/) — anonymized incidents
- [`templates/`](templates/) — review and authoring templates
- [`glossary.md`](glossary.md) — terms with UI/EXPLAIN meaning
- [`concept-map.md`](concept-map.md) — questions → chapters

**Contributing:** [`../CONTRIBUTING.md`](../CONTRIBUTING.md)
