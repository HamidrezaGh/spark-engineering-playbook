# Repository Quality Checklist

Use this before tagging a release, merging a large docs batch, or whenever presentation quality matters as much as technical accuracy.

## Rendering And Structure

- [ ] Markdown renders correctly on GitHub (spot-check raw vs preview for edited files).
- [ ] Every table has a header row, a separator row (`| --- |`), and one row per line (no collapsed pipe rows).
- [ ] Headings have blank lines before and after; lists are not glued to headings.
- [ ] Fenced code blocks start and end on their own lines; opening fences use a language where it helps (`sql`, `python`, `text`, `mermaid`, `bash`, `yaml`, `json`).

## Diagrams And Concepts

- [ ] Every diagram file uses an engineering-focused heading above the fenced `mermaid` code block (use `mermaid` only in the fence language tag, never as the visible section title).
- [ ] No visible heading or caption reads like “Mermaid Diagram” or “Mermaid flow” — titles name the concept (for example, “Shuffle Write and Shuffle Read Flow”).

## Chapter Quality Bar

For each book chapter touched in a change:

- [ ] At least one practical example (SQL, plan snippet, or short PySpark) where it fits the topic.
- [ ] Explicit production signals (EMR/YARN, S3, Iceberg, cost, or operational risk) where the topic touches runtime.
- [ ] Debugging guidance (Spark UI, logs, metrics, or a pointer to a field guide).
- [ ] A clear **when this matters** (or equivalent: “why it matters in production”, “production smells”) so readers know when to invest depth.

## Honest Maturity

- [ ] Placeholder or outline sections are explicitly marked **TODO** or **In progress** in the chapter, not left looking finished.
- [ ] [`README.md`](../../README.md) repo maturity / reading path still matches what is actually in the tree.

## Automation

- [ ] `markdownlint-cli2` passes locally: `npx markdownlint-cli2 "**/*.md" "#node_modules"`.
- [ ] CI: [`docs-check.yml`](../../.github/workflows/docs-check.yml) (Markdown lint, link check, Python compile, shell checks) is green on the PR.
