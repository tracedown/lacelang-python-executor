# Governance

## Current model

The reference Python executor is maintained by Tracedown contributors
as part of the [Lace](https://github.com/tracedown/lacelang) project.
Final decisions about implementation, API design, and release cadence
are made by the maintainers.

All meaningful contributions from the community are welcome and will be
integrated on merit. There is no distinction between "core" and
"external" contributors — the quality and correctness of the
contribution is what matters.

## Relationship to the spec

This executor implements the Lace specification. The spec repository
([lacelang](https://github.com/tracedown/lacelang)) governs the
language itself. This repository governs the reference Python
implementation only.

## Decision process

- **Spec compliance**: if the executor diverges from the spec, the spec
  wins. File a bug.
- **API design**: discussed in GitHub issues or PR comments. The
  maintainers make the final call.
- **Dependencies**: evaluated carefully. The executor has minimal
  runtime dependencies by design.

## Future

The goal is to move Lace to an independent foundation when adoption is
meaningful enough to warrant shared governance. Until then, Tracedown
stewards the project with the commitment to keep it open, neutral, and
welcoming to independent implementations.

## Contact

Open a GitHub issue or discussion for any governance questions.
