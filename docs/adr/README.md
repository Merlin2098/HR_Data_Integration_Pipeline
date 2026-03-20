# Architecture Decision Records

This index tracks the Architecture Decision Records (ADRs) for the project.

## Status Guide

- `proposed`: drafted but not yet adopted
- `accepted`: approved and treated as the current decision
- `superseded`: replaced by a newer ADR

## ADR List

| ADR | Title | Status | Date |
| --- | --- | --- | --- |
| [ADR-0001](0001-desktop-modular-etl-architecture.md) | Adopt a desktop-first, modular ETL architecture with Bronze/Silver/Gold layers and contract-based validation | Accepted | 2026-03-16 |
| [ADR-0002](0002-separate-etl-modules-per-domain.md) | Separate ETL processing into one module per ETL domain instead of forcing full-pipeline runs | Accepted | 2026-03-16 |
| [ADR-0003](0003-schema-and-transformation-logic.md) | Schema and transformation logic | Accepted | 2026-03-16 |
| [ADR-0004](0004-preflight-validation.md) | Preflight validation | Accepted | 2026-03-16 |
| [ADR-0005](0005-cloud-migration-readiness.md) | Cloud migration readiness | Accepted | 2026-03-16 |

## Adding a New ADR

1. Create a new file in `docs/adr/` using the next available number.
2. Keep the title short and descriptive.
3. Use the standard sections defined in `docs/README.md`.
4. Link the new ADR from this index.
5. Mark older ADRs as `superseded` when a newer decision replaces them.
