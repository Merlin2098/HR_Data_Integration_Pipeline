# Project Documentation

This folder contains lightweight project documentation that complements the root `README.md`.

## Architecture Decision Records

Architecture Decision Records (ADRs) capture important technical decisions that shape how this project is built and maintained. Each ADR explains the context behind a decision, the option chosen, alternatives considered, and the consequences of that choice.

Use ADRs in this repository to document decisions that affect:

- application architecture
- ETL design patterns
- orchestration approach
- validation and data contracts
- packaging and runtime constraints

## ADR Convention

- Store ADRs in `docs/adr/`.
- Use stable numeric identifiers such as `0001`, `0002`, and `0003`.
- Name files as `<number>-<short-kebab-case-title>.md`.
- Do not renumber old ADRs after they are created.
- Add every new ADR to the ADR index in `docs/adr/README.md`.

## Allowed Statuses

- `proposed`: the decision is drafted and under review
- `accepted`: the decision reflects the chosen approach
- `superseded`: the decision was replaced by a newer ADR

## ADR Template

Each ADR should use this minimal structure:

1. Title
2. Status
3. Date
4. Context
5. Decision
6. Alternatives Considered
7. Consequences
8. References

## Current ADRs

- [ADR Index](adr/README.md)
- [ADR-0001: Desktop-First Modular ETL Architecture](adr/0001-desktop-modular-etl-architecture.md)
- [ADR-0002: Separate ETL Modules per Domain](adr/0002-separate-etl-modules-per-domain.md)
- [ADR-0003: Schema and Transformation Logic](adr/0003-schema-and-transformation-logic.md)
- [ADR-0004: Preflight Validation](adr/0004-preflight-validation.md)
- [ADR-0005: Cloud Migration Readiness](adr/0005-cloud-migration-readiness.md)
