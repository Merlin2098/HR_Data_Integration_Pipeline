# HR ETL Manager

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)](https://www.microsoft.com/windows)

Desktop ETL application for human resources analytics, built in Python with a modular Bronze -> Silver -> Gold architecture, schema validation, SQL-based transformations, and analytics-ready Parquet outputs for downstream Power BI reporting.

## Overview

This project centralizes multiple HR data sources into a stable and maintainable analytics workflow. Instead of relying on fragile spreadsheet logic and complex Power Query / DAX transformations, the application moves data preparation into reproducible ETL pipelines with clear contracts and versioned outputs.

The repository demonstrates practical data engineering skills that are valuable in international roles:

- Designing layered data pipelines for analytical consumption
- Standardizing heterogeneous Excel-based business inputs
- Enforcing data quality with JSON contracts and schema validation
- Using DuckDB and Polars for local, high-performance transformations
- Delivering business-facing tooling through a desktop UI, not only scripts
- Packaging the solution as a standalone executable for non-technical users

## Business Problem

HR reporting depended on multiple disconnected files such as payroll spreadsheets, employee master data, tax declarations, internship tracking, medical retirement exams, and leave records. The previous reporting flow was difficult to maintain and required repeated manual rework whenever source files changed.

This solution automates the ingestion, consolidation, validation, enrichment, and export of those datasets into stable outputs that can be consumed by Power BI and reviewed by business users in Excel.

## Key Capabilities

- Modular ETL manager built with PySide6 and a multi-tab desktop interface
- 6 ETL modules discovered dynamically at runtime
- Bronze -> Silver -> Gold data processing pattern
- JSON-based source contracts for preflight validation
- JSON schemas for analytics-layer validation
- SQL transformation layer powered by DuckDB
- High-performance dataframe processing with Polars
- Dual output strategy: Parquet for analytics and Excel for business review
- Historical versioning plus stable `actual/` outputs for reporting tools
- PyInstaller build script for Windows executable distribution

## Available Pipelines

The application currently includes the following ETL domains:

- Employee master data
- Standard payroll
- Mining payroll regime
- Tax declaration / income relationship data
- Internship control
- Retirement medical exam processing

In addition, the project contains orchestrated YAML pipelines for:

- Payroll + leave enrichment
- Internship control with business flags

## Architecture

### Bronze

- Ingests source Excel files
- Preserves raw structure as the starting point
- Applies preflight checks for expected sheets, headers, filename rules, and required columns

### Silver

- Consolidates multiple source files
- Normalizes types and column structures
- Produces cleaned Parquet datasets

### Gold

- Applies business transformations and joins
- Enforces schema validation
- Generates analytics-ready datasets and business flags
- Exports both stable and historical outputs

## Data Quality and Validation

One of the strongest engineering aspects of this project is its validation layer.

Before transformations run, each ETL can validate inputs against contracts stored in `assets/validate_source/`. These checks include:

- Required worksheets
- Header starting position
- Required source columns
- Filename regex rules when needed

At the analytics stage, JSON schemas in `assets/esquemas/` help verify that Gold outputs match the expected structure. This makes the pipelines more resilient to business-side format changes and reduces the risk of silent reporting errors.

## Technology Stack

- Python 3.13+
- Polars
- DuckDB
- PySide6
- OpenPyXL
- PyArrow
- Pydantic
- JSON Schema
- PyYAML
- PyInstaller

## Repository Structure

```text
ETL_HumanResources_Dashboard/
|-- assets/
|   |-- config/            # UI theme, icon, app resources
|   |-- esquemas/          # JSON schemas for output validation
|   |-- queries/           # SQL transformations and business rules
|   `-- validate_source/   # Source validation contracts
|-- src/
|   |-- modules/           # Domain-specific ETL modules
|   |-- orchestrators/     # YAML-driven pipeline orchestration
|   `-- utils/             # Shared utilities, validation, paths, UI helpers
|-- etl_manager.py         # Desktop app entry point
`-- generar_exe.py         # PyInstaller build script
```

## Documentation

Additional project documentation lives under `docs/`.

- [Project documentation](docs/README.md)
- [Architecture Decision Records](docs/adr/README.md)

## How It Works

1. A user opens the desktop application.
2. The app discovers enabled ETL modules dynamically.
3. The user selects the relevant source files or folders.
4. The ETL validates the source structure before processing.
5. The pipeline writes Silver and Gold outputs to organized folders.
6. Power BI consumes stable Parquet outputs, while Excel exports support manual review.

## Local Setup for Development

```bash
git clone https://github.com/Merlin2098/ETL_HumanResources_Dashboard
cd ETL_HumanResources_Dashboard

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
pre-commit install
```

## Run the Application

```bash
python etl_manager.py
```

## Build the Executable

```bash
python generar_exe.py
```

The build script packages the application, static assets, validation contracts, and pipeline definitions into a Windows-ready distribution folder.

## Why This Project Matters for Data Engineering

This repository is not only a desktop utility. It is a practical example of end-to-end data engineering in a business environment:

- Batch ingestion from messy operational files
- Contract-based validation for reliability
- Transformation logic separated from reporting logic
- Analytics-ready storage in Parquet
- Reusable ETL module design
- Business-facing delivery through a deployable application

It reflects the kind of work often required in small and mid-sized organizations where data engineers must bridge raw files, business operations, and decision-support dashboards without relying on a large cloud platform.

## Confidentiality Note

For privacy and corporate confidentiality reasons, this repository does not include:

- Real HR source files
- Sensitive employee data
- The final Power BI dashboard

The codebase is shared to demonstrate the solution architecture, engineering approach, and implementation patterns.

## Author

Ricardo Uculmana Quispe  
LinkedIn: [ricardouculmanaquispe](https://pe.linkedin.com/in/ricardouculmanaquispe)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
