# Diagrams

Use this directory for source diagrams that explain Spark execution, shuffle flow, join strategies, table metadata, and platform architecture.

Prefer editable source formats such as Mermaid or Excalidraw JSON when possible.

## Diagram Conventions

- Use Mermaid for conceptual flowcharts that should render in GitHub.
- Use ASCII/text diagrams when exact visual alignment matters in plain Markdown.
- Keep diagrams close to the chapter section they explain.
- Prefer one focused diagram over a large architecture drawing that mixes unrelated concepts.
- Pair diagrams with a small table when readers need quick tradeoff recall.

## Suggested Diagram Types

| Topic | Best Diagram |
| --- | --- |
| Execution flow | Mermaid flowchart |
| Shuffle mechanics | ASCII map/reduce block diagram |
| Join selection | Decision tree |
| Memory layout | ASCII container diagram |
| Table metadata | Mermaid hierarchy |
| Incident triage | Flowchart |
| Platform design | Layered architecture diagram |
