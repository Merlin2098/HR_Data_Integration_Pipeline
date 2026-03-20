# ADR-0001: Adopt a Desktop-First, Modular ETL Architecture with Bronze/Silver/Gold Layers and Contract-Based Validation

- Status: Accepted
- Date: 2026-03-16

## Context

This project processes human resources data from Excel-based operational sources and produces analytics-ready outputs for downstream reporting and business review. The codebase targets a workflow where non-technical users run ETL processes locally on Windows and need a guided interface instead of a script-only toolchain.

The current implementation already shows a clear architectural pattern:

- a PySide6 desktop application hosts the ETL experience
- ETL modules are discovered dynamically from `src/modules/*/ui`
- each ETL domain provides its own UI config, widget, worker, and processing steps
- shared UI and worker abstractions standardize lifecycle, progress reporting, and error handling
- transformations follow Bronze, Silver, and Gold stages
- input sources are validated against JSON contracts in `assets/validate_source/`
- analytics outputs are governed by JSON schemas in `assets/esquemas/`
- some multi-stage processes are orchestrated from YAML pipeline definitions in `src/orchestrators/pipelines/`

This architecture needs a formal record so future contributors understand why the project is a desktop ETL product, why modules are split by domain, and why validation and staged outputs are first-class design elements.

## Decision

We adopt a desktop-first, modular ETL architecture with the following characteristics:

- A PySide6 desktop shell is the primary runtime interface for business users.
- ETL functionality is organized into domain modules under `src/modules/`.
- ETL modules are discovered dynamically at runtime through a shared registry contract instead of being hardcoded into the main window.
- Shared base classes define consistent UI behavior, background execution, progress updates, logging, and error reporting across modules.
- Data processing follows a Bronze -> Silver -> Gold pattern to separate ingestion, normalization, and business-ready outputs.
- Source validation happens before transformations using JSON contracts.
- Gold-level outputs can be validated against JSON schemas.
- Multi-stage domain workflows may be orchestrated through YAML-defined pipelines executed by Python pipeline executors.
- Outputs are designed for both analytics consumption, especially Parquet-based downstream reporting, and business-facing review artifacts such as Excel files.

## Alternatives Considered

### 1. Script-Only ETL Without a GUI

This would reduce UI code and simplify runtime concerns, but it would make the tool less accessible to business users who need guided file selection, progress visibility, and desktop packaging.

### 2. Single Monolithic Pipeline Instead of Per-Domain Modules

This would centralize logic in fewer files, but it would make the system harder to extend and maintain as HR domains evolve independently. The current module-per-domain pattern better matches the separation between payroll, internships, retirement exams, and related workflows.

### 3. Direct Excel-to-Report Transformations Without Bronze/Silver/Gold Layers

This would shorten the path from input to output, but it would make debugging, schema governance, and reuse much harder. The staged approach provides clearer contracts between ingestion, standardization, and final business outputs.

### 4. Cloud-First Orchestration Instead of Local Execution

This could improve centralization and automation, but it would add operational overhead and move away from the current delivery model, which is optimized for local Windows execution and packaged distribution.

## Consequences

### Positive

- Non-technical users get a Windows-friendly desktop workflow instead of a CLI-only experience.
- New ETL domains can be added with a predictable module structure and shared base components.
- Bronze, Silver, and Gold layers make transformation logic easier to reason about and validate.
- Source contracts reduce failures caused by business-side Excel format drift.
- Schema validation improves confidence in analytics-ready outputs.
- YAML orchestration supports composed pipelines without forcing all logic into one worker class.
- Packaging the project as an executable remains aligned with the intended user experience.

### Tradeoffs

- Desktop UI, packaging, and asset resolution add complexity compared with pure scripts.
- Dynamic discovery requires contributors to follow the expected module contract.
- Local execution is well suited for the current use case but does not provide the scheduling, observability, or elasticity of a cloud-native platform.
- The repository currently has limited automated tests and operational monitoring, so some architectural guarantees still depend on conventions and manual verification.

## References

- `README.md`
- `etl_manager.py`
- `src/app_main.py`
- `src/utils/ui/etl_registry.py`
- `src/utils/ui/widgets/base_etl_widget.py`
- `src/utils/ui/workers/base_worker.py`
- `src/utils/validate_source.py`
- `src/orchestrators/pipeline_nomina_executor.py`
- `src/orchestrators/pipeline_control_practicantes_executor.py`
- `src/orchestrators/pipelines/pipeline_nomina_licencias.yaml`
- `src/orchestrators/pipelines/pipeline_control_practicantes.yaml`
- `assets/validate_source/`
- `assets/esquemas/`
