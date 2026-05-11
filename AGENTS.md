# AGENTS.md

## Purpose

This repository is an AWS + Terraform data engineering template used to
bootstrap host repositories.

Agents should support work that stays valid both in this template repository and
in host projects installed from it:

* Python data jobs and helpers
* SQL transformations
* Terraform infrastructure
* Config-driven workflows
* Lightweight testing and packaging workflows

---

## Knowledge Sources

Use:

* `ai/skills/` for patterns and best practices
* `ai/skills.yaml` as the authoritative skills index
* `ai/context.yaml` as the authoritative AI context-generation configuration
* `specs/template/` for inherited template contracts (cloud profile only, read-only)
* `specs/project/` for host-authored project specs (cloud profile only)

These files are guidance and configuration inputs copied into host repositories.
They are not executable orchestration logic.

---

## Working Style

When assisting in this repository or a host repository created from it:

1. Understand the objective and current repository shape
2. Search for existing implementations before proposing new files
3. Identify relevant skills from `ai/skills/`
4. Apply patterns as guidance, not as rigid rules
5. Prefer simple, explicit changes over frameworks or abstractions
6. Validate the result against repository principles and documented workflows

---

## Skill Usage

The agent should:

* discover relevant skills automatically from `ai/skills/`
* treat `ai/skills.yaml` and `ai/context.yaml` as the source of truth for AI guidance inputs
* match tasks with skill names such as `testing`, `ci_cd`, `mocks`, `glue`, or `terraform`
* use skills to guide implementation without requiring explicit invocation by the user

The agent must not:

* require explicit skill invocation
* enforce rigid one-to-one mappings between tasks and skills
* create skill composition or orchestration logic

---

## Execution Rules

Use explicit project commands only.

Preferred workflow:

* use `make <target>` when `make` is available
* in restricted Windows environments, use `scripts/windows/run_make.ps1` or the documented wrapper flow under `docs/windows_setup/`
* run Terraform commands directly and intentionally from `infra/`

Do not introduce hidden automation.

---

## Package Manager Awareness

Host repositories created from this template may use either `pip` or `uv`.

The agent should:

* inspect the files present in the repository before choosing a dependency workflow
* follow `requirements*.txt` workflows when the host is configured for `pip`
* follow `pyproject.toml` and `uv.lock` workflows when the host is configured for `uv`
* keep packaging, testing, and environment guidance aligned with the package-manager choice already installed in the host

---

## Approval Boundaries

### Never without approval

* `terraform apply`
* `terraform destroy`
* modify infrastructure state
* overwrite data or generated artifacts intentionally owned by users

### Ask before

* IAM changes
* Terraform module changes
* paid AWS services or production-grade infrastructure defaults
* data contract updates

---

## Principles

* separation of concerns across infra, code, and config
* SQL separate from Python
* config-driven pipelines
* contracts-first validation
* Terraform should optimize for destroyability, low-cost dev environments, reproducibility, and explicit resource ownership
* prefer simple over complex
* keep workflows explicit and reproducible

---

## Constraints

The agent must not:

* create orchestration frameworks
* define skill composition systems
* introduce meta-systems
* recreate hidden framework-like behavior

---

## Existing Code Awareness

Before generating any new file or artifact, the agent must:

1. search the repository for existing implementations
2. prefer modifying or extending existing files over creating new ones
3. avoid duplicating Terraform modules, ETL jobs, SQL transformations, or config files

If similar functionality already exists, reuse or refactor it instead of
creating parallel structures.

Only create new files when:

* no equivalent exists
* or the user explicitly requests it

---

## Skill Trigger Map

The map below is indicative, not exhaustive. If a task does not appear here,
follow the discovery flow in *Skill Usage*.

| When the task involves… | Consult |
|---|---|
| Designing or editing a Python ETL job | `ai/skills/data/etl_patterns.md`, `ai/skills/python/python_project_guidance.md` |
| Validation or data quality (Python/SQL/AWS) | `ai/skills/data/data_quality_guidance.md`, `ai/skills/data/data_contracts.md` |
| Python tests | `ai/skills/python/python_testing_quality.md` |
| New SQL or transformation refactor | `ai/skills/sql/sql_workflow_guidance.md` |
| AWS Glue (jobs, crawlers, catalog) | `ai/skills/aws/glue_jobs.md` |
| AWS Lambda | `ai/skills/aws/lambda_functions.md`, `ai/skills/aws/iam_policies.md` |
| Step Functions orchestration | `ai/skills/aws/step_functions.md` |
| Scheduling / event-driven | `ai/skills/aws/eventbridge.md` |
| S3 / data lake storage | `ai/skills/aws/s3_data_lake.md` |
| AWS logging / observability | `ai/skills/aws/cloudwatch_logging.md` |
| IAM (policies, roles) | `ai/skills/aws/iam_policies.md`, `ai/skills/terraform/iam_least_privilege.md` |
| Writing or refactoring Terraform | `ai/skills/terraform/terraform_style.md`, `ai/skills/terraform/modules.md` |
| Terraform state / backends | `ai/skills/terraform/state_management.md` |
| Terraform tests / mocks | `ai/skills/terraform/terraform_testing.md`, `ai/skills/terraform/terraform_mocks.md` |
| Terraform CI/CD | `ai/skills/terraform/terraform_ci_cd.md`, `ai/skills/terraform/terraform_orchestration.md` |
| Importing existing resources | `ai/skills/terraform/terraform_import_manual.md`, `ai/skills/terraform/terraform_import_discovery.md` |
| Module refactor / multi-env | `ai/skills/terraform/terraform_refactoring.md`, `ai/skills/terraform/terraform_stacks.md` |
| Infra security review | `ai/skills/terraform/terraform_security.md` |

---

## Philosophy

Simple. Explicit. Reproducible.

AI is a helper for the host project, not the system itself.
