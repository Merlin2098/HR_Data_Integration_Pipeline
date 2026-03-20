# Tinker Framework: Core Operating Rules

## Identity
You are an agent inside the **Tinker** framework.
Operate with a wrapper-first, deterministic execution model.

## Trigger
- Primary trigger: `Run task from .tinker/user_task.yaml`
- Read `.tinker/user_task.yaml` and execute only explicit objective + constraints.
- Do not require role-based routing.

## Execution Model
- Skills are thin interfaces.
- Wrappers/scripts under `agents2/tools/` are the execution source of truth.
- Do not reimplement logic already provided by wrappers.

## Governance
- Canonical governance: `agents2/engine/rules/agent_rules.md`
- `agents2/rules.md` is a compact runtime contract; avoid duplicating long policy text.
- Respect protected files and explicit user authorization boundaries.

## Runtime Constraints
- Use virtual environment Python only:
  - Windows: `.venv/Scripts/python.exe`
  - Unix/macOS: `.venv/bin/python`
- Set `PYTHONIOENCODING=utf-8` for Python terminal execution.
- Persist runtime artifacts under `.tinker/`.
- Plan docs and execution reports, if used, should also live under `.tinker/`.

## Context Hygiene
- Use `.tinker/context_bundle.yaml` as the compact runtime bundle.
- Load large artifacts on demand (`.tinker/treemap.md`, `.tinker/dependencies_report.md`).
- Keep loaded context minimal and task-scoped.

## Conflict Policy
1. `agents2/engine/rules/agent_rules.md`
2. `agents2/rules.md`
3. task-specific constraints

If constraints conflict or intent is ambiguous, stop and ask the user.
