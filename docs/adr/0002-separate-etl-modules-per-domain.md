# ADR-0002: Separate ETL Processing into One Module per ETL Domain Instead of Forcing Full-Pipeline Runs

- Status: Accepted
- Date: 2026-03-16

## Context

The project works with multiple HR data domains, including payroll, mining payroll, internships, retirement exams, employee master data, leave records, and tax-related inputs. These sources do not update at the same time, and the business workflow does not require real-time processing.

Instead, users work with snapshots of the available source files and run ETL only when they need to refresh a specific analytical output. In practice:

- different source files arrive on different dates
- some refresh cycles affect only one file or one source domain
- some use cases require adding one new source without changing the rest of the data estate
- rerunning every ETL for every change would add unnecessary processing and operational friction

The codebase already reflects this operating model through separate ETL modules under `src/modules/`, each with its own UI configuration, widget, worker, and processing steps. Some scenarios use composed pipelines, but those are still built from bounded domain-specific components rather than one mandatory global pipeline.

This decision needs to be documented because modular ETLs are not only a code organization preference. They are a direct response to the update cadence of the source systems and to the decision to process snapshot inputs rather than real-time streams.

## Decision

We implement one module per ETL domain and do not require users to run a single full-project pipeline for every refresh.

This means:

- each ETL domain owns its own processing flow and business output
- users can execute only the ETL relevant to the source data that changed
- snapshot-based runs are the default operating model
- composed pipelines are allowed when a business flow truly spans multiple domains, but they remain targeted and explicit
- full end-to-end reprocessing of all domains is not the default refresh strategy

## Alternatives Considered

### 1. One Global Pipeline for All HR Data

This would provide a single execution entrypoint, but it would force unnecessary reruns when only one source changed. It would also couple unrelated data domains too tightly and make operational use less efficient for business users.

### 2. Real-Time or Event-Driven Processing

This could reduce latency between source updates and outputs, but it is not required for the business problem. The project works with periodic file deliveries, and snapshot-based refreshes are sufficient for reporting needs.

### 3. Shared Core Logic with No Domain Modules

This would centralize implementation, but it would make the UI, validation rules, and processing contracts harder to understand and maintain because domain-specific concerns would be mixed into one broad workflow.

## Consequences

### Positive

- Users can refresh only the dataset that changed instead of rerunning the whole platform.
- The architecture matches the real operating cadence of the business sources.
- Each ETL can evolve independently as file structures, business rules, or outputs change.
- Validation contracts and UI behavior stay closer to the domain they protect.
- New ETL domains can be introduced with limited impact on existing modules.
- Composed workflows such as payroll plus leave enrichment remain possible without making every ETL dependent on every other ETL.

### Tradeoffs

- Some cross-domain logic may need orchestration code when outputs from one ETL feed another.
- Shared behavior must be maintained carefully in base classes and utilities to avoid duplication.
- Contributors need to understand when a requirement deserves a new ETL module versus an extension of an existing one.
- A modular approach can look more fragmented than a single pipeline unless the documentation clearly explains the domain boundaries.

## References

- `README.md`
- `src/modules/`
- `src/utils/ui/etl_registry.py`
- `src/utils/ui/widgets/base_etl_widget.py`
- `src/utils/ui/workers/base_worker.py`
- `src/orchestrators/pipeline_nomina_executor.py`
- `src/orchestrators/pipeline_control_practicantes_executor.py`
- `src/orchestrators/pipelines/pipeline_nomina_licencias.yaml`
- `src/orchestrators/pipelines/pipeline_control_practicantes.yaml`
