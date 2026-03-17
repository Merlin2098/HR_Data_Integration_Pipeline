# ADR-0005: Cloud Migration Readiness

- Status: Accepted
- Date: 2026-03-16

## Context

The current project is a desktop-first ETL application built for local execution on Windows, but several architectural choices already make it a reasonable candidate for progressive cloud migration, including to AWS.

The goal is not to describe the current system as cloud-native. Instead, it is to record why the existing design reduces migration effort compared with a tightly coupled desktop-only implementation.

The main migration driver is that ETL workloads, validation rules, transformation logic, and analytical outputs are already more modular than the UI that currently launches them.

## Decision

We recognize the current architecture as cloud-migration-ready at the ETL and data-processing layers, and we treat any future AWS migration as an incremental refactor rather than a full rewrite.

This decision is based on the following aspects of the project that already facilitate migration:

- ETLs are separated by domain under `src/modules/`, which makes it easier to move workloads one pipeline at a time instead of migrating the whole system at once.
- Shared workers and reusable execution patterns already separate orchestration concerns from domain-specific transformation logic.
- Multi-stage workflows are described with YAML pipeline definitions in `src/orchestrators/pipelines/`, which creates a natural bridge to cloud orchestration services.
- Validation contracts and schemas are externalized in JSON assets, so business rules are not fully embedded in UI code.
- SQL transformation assets already separate business logic from Python execution flow.
- Parquet is the main analytical storage format, which maps well to object storage and query-oriented cloud analytics workflows.
- DuckDB-based query processing and file-oriented ETL steps already use analytical batch patterns that are closer to cloud data processing than to interactive transactional systems.
- Most ETL execution paths operate on explicit file paths and output directories, which can be adapted to managed storage abstractions such as Amazon S3.

## Analysis

### Aspects That Facilitate Migration to AWS

#### 1. Modular ETL Boundaries

Each ETL domain has its own module, worker, and step logic. This reduces migration scope because payroll, internships, employee master data, and other domains can be moved independently.

This supports phased migration patterns such as:

- one ETL module per AWS batch job
- one orchestration flow per business domain
- selective migration of the highest-value pipelines first

#### 2. Externalized Contracts and Business Logic

Source validation contracts, output schemas, and SQL transformations already live outside the main Python orchestration code. This is useful in the cloud because:

- validation rules can be reused by non-UI execution environments
- business logic remains inspectable and portable
- schema governance is not tied to the desktop shell

#### 3. File-Based Analytical Outputs

The project already produces Parquet outputs for analytical consumption. This is a strong cloud enabler because Parquet is a natural fit for:

- Amazon S3 as Bronze, Silver, and Gold storage
- Athena, Glue, or downstream warehouse-style querying
- efficient batch-oriented processing instead of spreadsheet-centric delivery only

#### 4. Orchestration Patterns Already Exist

The repository already contains YAML-defined pipelines and executor classes for composed workflows. That makes it easier to map the current design to AWS orchestration patterns such as:

- Step Functions for multi-stage pipeline coordination
- ECS tasks or containerized jobs for ETL execution
- Lambda only for lightweight control or validation steps where runtime limits are acceptable

#### 5. Limited Dependence on Interactive Data Logic

Although the application uses a desktop UI, the core ETL logic is not entirely embedded in the UI layer. Workers call processing functions with explicit input and output paths, which is a better starting point for cloud migration than a design where all business logic lives inside widgets.

### Current Constraints That Still Need Refactoring

The project is migration-friendly, but not cloud-ready without changes.

Key constraints are:

- the main runtime entrypoint is still a desktop UI
- many execution flows assume local filesystem access through `Path` and local output directories
- resource resolution is designed around local development and PyInstaller packaging
- some ETL step scripts still include local desktop behavior such as file dialogs
- logging and operational monitoring are oriented to local execution, not centralized observability

## Alternatives Considered

### 1. Treat the Project as Desktop-Only and Not Suitable for Cloud Migration

This would ignore the existing modularity, externalized contracts, and analytical file patterns that already reduce migration effort.

### 2. Plan a Full Cloud Rewrite

This would be possible, but it would discard reusable assets such as ETL modules, schemas, SQL queries, and pipeline definitions that already encode valuable business behavior.

### 3. Migrate Only the Storage Layer

This would improve persistence, but it would leave orchestration and execution patterns underused and would not take full advantage of the project’s modular ETL design.

## Consequences

### Positive

- Future AWS migration can be planned incrementally instead of as a big-bang rewrite.
- Existing ETL domains, schemas, SQL assets, and Parquet outputs can be reused as migration building blocks.
- The current architecture supports a realistic path toward S3-based storage and managed batch orchestration.
- The ADR helps distinguish between what is already migration-friendly and what still needs refactoring.

### Tradeoffs

- The project should not be described as cloud-native in its current form.
- Migration will still require decoupling from desktop-only concerns such as local dialogs and PyInstaller-oriented resource handling.
- Storage abstraction, centralized logging, secrets handling, and deployment packaging still need explicit cloud design work.

## References

- `README.md`
- `requirements.txt`
- `src/modules/`
- `src/orchestrators/pipelines/`
- `src/orchestrators/pipeline_nomina_executor.py`
- `src/orchestrators/pipeline_control_practicantes_executor.py`
- `src/utils/validate_source.py`
- `src/utils/paths.py`
- `src/modules/bd/steps/step2_capagold.py`
- `src/modules/nomina/steps/step2_exportar.py`
- `assets/validate_source/`
- `assets/esquemas/`
- `assets/queries/`
