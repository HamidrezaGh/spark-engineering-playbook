# Spark Engineering Playbook

A public Spark handbook for engineers who want production-grade judgment, not only API familiarity.

This repo is intended to read like a staff-level engineering reference with an AWS EMR production bias: each topic should explain how Spark works, why design choices matter, how to diagnose tradeoffs, and how to apply the guidance in real EMR/S3 systems.

The handbook should stay practical and production-oriented: each topic should include mental models, tradeoffs, performance notes, best practices, examples, and realistic use cases.

The chapters use simple text diagrams and decision tables so the material works both as a book and as a fast review reference.

Primary operating context: Apache Spark on AWS EMR, using YARN, S3, IAM, CloudWatch, EMR steps, and AWS-native operational patterns unless a chapter explicitly says otherwise.

## Handbook Roadmap

The first public roadmap is the [Advanced Spark Questions](docs/advanced-spark-questions.md) list. These questions define the bar for the handbook: production behavior, debugging, tuning, lakehouse design, streaming, and staff-level platform thinking.

## Repository Structure

- [docs/book](docs/book/README.md): the main chapter-by-chapter handbook.
- [docs/field-guides](docs/field-guides/README.md): incident-oriented debugging guides.
- [docs/patterns](docs/patterns/README.md): reusable production Spark architecture patterns.
- [docs/tuning](docs/tuning/README.md): focused tuning guides for common performance levers.
- [docs/configs](docs/configs/README.md): a practical Spark configuration field manual.
- [docs/checklists](docs/checklists/README.md): operational review checklists.
- [examples](examples/README.md): PySpark, SQL, and configuration examples used by the handbook.
- [diagrams](diagrams/README.md): source files for execution, storage, and platform diagrams.

## License

This repository is under the [MIT License](LICENSE). Others may use, copy, modify, and distribute the content; they should include the copyright and license notice in substantial copies.

As the **copyright holder**, you are not “giving up” your right to publish: you can still create a book (print, ebook, paid course, etc.) from the same material. The MIT License grants broad permissions to **others**; it does not block you from commercial publishing later.
