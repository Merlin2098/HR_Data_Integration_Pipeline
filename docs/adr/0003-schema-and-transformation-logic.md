# ADR-0003: Schema and Transformation Logic

- Status: Accepted
- Date: 2026-03-16

## Context

This project processes Excel-based HR inputs that may change over time in structure, column naming, and business content. Different ETL domains need to validate sources before processing, normalize values into a consistent analytical format, and preserve stable output structures for downstream reporting.

If schema expectations and transformation rules were hardcoded directly in Python for every ETL, the project would become harder to maintain because:

- schema changes would require code edits in multiple places
- validation rules would be harder to review outside the implementation
- column normalization rules would be mixed with execution code
- business transformation logic would be less transparent to inspect and adjust

The repository already uses external assets for these responsibilities:

- JSON contracts in `assets/validate_source/` for source preflight checks
- JSON schemas in `assets/esquemas/` to define expected analytical structures, required columns, and target formats
- SQL files in `assets/queries/` to express business transformations for Gold-layer outputs

The Gold layer also targets analytical consumption, where Parquet outputs and DuckDB-based processing are a good fit for local, high-performance transformations and query-based enrichment.

## Decision

We externalize schema/configuration rules in JSON files and business transformations in SQL files instead of hardcoding them inside ETL Python modules.

This means:

- JSON assets define expected structures and validation rules for ETL inputs and outputs
- ETLs use these definitions to run preflight validation before processing
- ETLs can use schema metadata to normalize column values into a consistent analytical format
- SQL assets hold transformation logic for analytical shaping, enrichment, joins, and business rules in the Gold layer
- DuckDB is used with Parquet-oriented workflows to optimize Gold generation for analytical use cases
- Python remains responsible for orchestration, file handling, UI interaction, and execution flow, while schemas and transformation rules stay externalized

## Alternatives Considered

### 1. Hardcode Schemas and Transformations in Python

This would keep everything in one language, but it would couple validation, normalization, and business logic too tightly to the application code. It would also make schema evolution and review more difficult.

### 2. Use Only Python Dataframe Logic for Gold Transformations

This would avoid separate SQL assets, but it would mix business rules with orchestration code and make query-oriented transformations harder to read for analysts or maintainers who are comfortable with SQL.

### 3. Use SQL for All Layers Without Explicit JSON Schema Assets

This could centralize transformation logic, but it would weaken the explicit contract layer that helps detect source manipulation, validate required structures, and preserve stable analytical formats across ETLs.

## Consequences

### Positive

- ETLs can validate source structure before processing starts.
- Schema expectations are documented in a portable, reviewable format instead of being hidden in code paths.
- Column names, required fields, and expected formats can be standardized for analytical outputs.
- Users can detect file manipulation or source drift earlier through contract-driven validation.
- Business logic is easier to inspect and evolve when expressed in SQL assets.
- DuckDB plus Parquet supports efficient local Gold-layer generation for analytical workloads.
- The separation between orchestration code and transformation logic improves maintainability.

### Tradeoffs

- The project must keep Python, JSON, and SQL assets aligned as schemas and outputs evolve.
- Debugging may require tracing behavior across code, config, and query files instead of one implementation layer.
- Contributors need discipline around naming, versioning, and documenting external assets.
- Some complex transformations may still require hybrid logic between Python and SQL.

## References

- `README.md`
- `assets/validate_source/`
- `assets/esquemas/`
- `assets/esquemas/esquema_bd.json`
- `assets/queries/`
- `assets/queries/query_control_practicantes_gold.sql`
- `assets/queries/query_cc_join.sql`
- `assets/queries/query_licencias_agregadas.sql`
- `src/utils/validate_source.py`
