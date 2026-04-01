# ADR-0004: Preflight Validation

- Status: Accepted
- Date: 2026-03-16

## Context

HR operational processes change frequently. In payroll, for example, new concepts can be added from one month to the next, file structures may shift, and Excel workbooks are often manually manipulated because they are treated like a database by business users.

At the same time, business analytics requires a minimum schema, stable formats, and predictable source structure. If those conditions are not met, downstream outputs can look valid while actually carrying incomplete, shifted, or misinterpreted data.

This creates a high-risk scenario for analytics:

- monthly source changes can introduce unexpected columns or missing required fields
- worksheet layouts can move without warning
- manual Excel manipulation can alter headers, data positions, or file structure
- transformations may still run even when the source no longer satisfies the analytical contract

Because the project is used for business decision support, allowing invalid inputs to pass deeper into the pipeline creates a risk of producing misleading Gold outputs and incorrect reporting.

The repository already includes a formal preflight mechanism in `src/utils/validate_source.py` and structured source contracts under `assets/validate_source/` to validate files before ETL execution starts.

## Decision

We require preflight validation at the beginning of ETL execution and treat contract violations as hard failures that stop the pipeline immediately.

This means:

- each ETL validates its input files before transformation starts
- required worksheets, headers, filename patterns, and minimum columns must match the declared contract
- files must be readable as supported Excel workbooks before processing continues
- missing or shifted structures are treated as blocking errors
- the pipeline fails fast instead of attempting partial or best-effort processing
- only inputs that satisfy the minimum analytical contract are allowed into Silver and Gold processing
- the packaged desktop application must ship the validation contracts as runtime assets

## Alternatives Considered

### 1. Best-Effort Processing with Warnings Only

This would allow pipelines to keep running when source structures drift, but it would increase the chance of silently generating incorrect analytical outputs.

### 2. Validate Only at the Gold Layer

This would detect some problems later, but it would waste processing effort and allow corrupted assumptions to travel through earlier ETL stages before failure becomes visible.

### 3. Manual Review Without Enforced Preflight Rules

This would reduce implementation strictness, but it would depend too heavily on human inspection and would not scale reliably across recurring monthly refreshes.

## Consequences

### Positive

- Invalid or manipulated source files are detected before they affect downstream analytics.
- The pipeline protects minimum schema and format requirements needed for business reporting.
- Monthly source variation is controlled through explicit contracts instead of implicit assumptions in code.
- Fail-fast behavior reduces the risk of silent data quality issues reaching decision makers.
- Users receive immediate feedback when a source file no longer satisfies the expected structure.
- Preflight rules create a stronger boundary between operational spreadsheets and analytical datasets.
- Executable builds remain aligned with runtime expectations because validation contracts are treated as required packaged assets.

### Tradeoffs

- ETLs may stop more often when business-side files change, requiring contract updates or source correction before processing can continue.
- Teams must maintain validation contracts as source formats evolve.
- Strict validation can feel less flexible in the short term, but it is safer for analytical reliability.

## References

- `README.md`
- `src/utils/validate_source.py`
- `assets/validate_source/`
- `assets/validate_source/nomina.yaml`
- `assets/validate_source/control_practicantes.yaml`
- `generar_exe.py`
- `src/modules/nomina/ui/worker.py`
- `src/modules/bd/ui/worker.py`
- `src/modules/control_practicantes/ui/worker.py`
