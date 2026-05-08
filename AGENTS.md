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

## Philosophy

Simple. Explicit. Reproducible.

AI is a helper for the host project, not the system itself.
