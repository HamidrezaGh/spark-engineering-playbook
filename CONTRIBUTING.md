# Contributing to the Spark Engineering Playbook

This repo is a **practical** handbook. Changes should be reviewable, link-safe, and honest about
what is **observable in Spark** vs what is a platform opinion.

## Tone

- **Production-first:** tie claims to the Spark UI, `EXPLAIN`, metrics, and failure behavior.
- **No hype:** avoid “this always works,” vague “industry best” posturing, and unfalsifiable claims.
- **Actionable:** prefer checklists, decision trees, and runnable examples over abstract lists.

## Layout

| Area | Role |
| --- | --- |
| [`docs/book/`](docs/book/) | Long-form concepts in chapter order. |
| [`docs/troubleshooting/`](docs/troubleshooting/) | Symptom-first decision trees. |
| [`docs/observability/`](docs/observability/) | Spark UI and physical plan reading. |
| [`docs/field-guides/`](docs/field-guides/) | Short incident entry points. |
| [`docs/checklists/`](docs/checklists/) | One-page operable lists. |
| [`examples/`](examples/) | Runnable SQL and PySpark (see below). |
| [`diagrams/`](diagrams/) | One concept per Mermaid/ASCII diagram. |

## Chapter structure

Reuse the [chapter template](docs/templates/book-chapter-template.md). For core topics, prefer:

1. **Concept** — the mental model.
2. **Why it matters in production** — cost, SLO, failure, ops time.
3. **Internally in Spark** — what actually runs (no textbook filler).
4. **How to observe** — `EXPLAIN`, UI tabs, logs.
5. **Common failure modes** — concrete symptoms.
6. **Tuning / config** — a few **high-leverage** knobs, with tradeoffs and **how to verify**.
7. **Example** or pointer to `examples/`.

**Callouts (short):** when adding nuance, use **Production lesson** / **Common mistake** / **Tradeoff** as short
paragraphs, not adjectives in every sentence.

## How to add a runnable example

1. **SQL:** put scripts under `examples/sql/…` and wire them into
   [`examples/local/run_examples.sh`](examples/local/run_examples.sh) *if* they run on the
   bundled **CSV** data. Add `CREATE VIEW` entries to the same **init** block when you need
   new tables; keep sample data small.
2. **PySpark:** add a folder under `examples/pyspark/…` with a **README** (`how to run`, what
   to look at, one production lesson) and, when useful, a **sample_output.md** (results vary by
   Spark version — label them **illustrative**).
3. **Iceberg / cloud-only:** a **template** is fine. Document **prerequisites** and do not mark
   it runnable in `run_examples.sh` unless CI can run it.
4. Run `python3 -m compileall examples` and `bash -n` on any new `*.sh` before a PR.
5. Keep relative links from examples to `docs/` **correct** (`../` levels change by folder depth).

## Quality gates (local)

- **Markdown** — the repo is linted with `markdownlint` in CI; config is
  [`.markdownlint.json`](.markdownlint.json).
- **Links** — [`lychee`](https://github.com/lycheeverse/lychee) runs on `**/*.md` in CI. Prefer
  **relative** links to this repo. External URLs must be reachable.
- **Spellcheck** is **not** in CI; optional: run your editor spellcheck on touched docs.
- **Diagrams** — Mermaid in GitHub-flavored Markdown. Keep **titles** in the page heading, not
  generic `flowchart` labels.

## pull requests

- One **logical** change (docs + the examples that support it) is easier to review than a dozen
  unrelated rewrites.
- If you restructure, keep **stubs** or **redirects** for renamed files for at least one release.
