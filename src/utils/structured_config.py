from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Sequence

import yaml

from src.utils.paths import get_resource_path


STRUCTURED_EXTENSIONS: tuple[str, ...] = (".yaml", ".yml", ".json")
STRUCTURED_FILETYPES: list[tuple[str, str]] = [
    ("YAML files", "*.yaml *.yml"),
    ("All files", "*.*"),
]


def _normalize_base_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if path.suffix.lower() in STRUCTURED_EXTENSIONS:
        return path.with_suffix("")
    return path


def iter_structured_candidates(path_like: str | Path) -> list[Path]:
    base_path = _normalize_base_path(path_like)
    return [base_path.with_suffix(ext) for ext in STRUCTURED_EXTENSIONS]


def resolve_structured_path(
    path_like: str | Path,
    *,
    prefer_resource_path: bool = True,
) -> Path:
    seen: set[Path] = set()
    ordered_candidates: list[Path] = []

    for candidate in iter_structured_candidates(path_like):
        expanded: list[Path] = []
        if candidate.is_absolute():
            expanded.append(candidate)
        else:
            if prefer_resource_path:
                expanded.append(get_resource_path(candidate.as_posix()))
            expanded.append(candidate)

        for expanded_candidate in expanded:
            if expanded_candidate not in seen:
                seen.add(expanded_candidate)
                ordered_candidates.append(expanded_candidate)

    for candidate in ordered_candidates:
        if candidate.exists():
            return candidate

    return ordered_candidates[0]


def find_first_structured_path(
    candidates: Sequence[str | Path],
    *,
    prefer_resource_path: bool = False,
) -> Path | None:
    for candidate in candidates:
        resolved = resolve_structured_path(
            candidate,
            prefer_resource_path=prefer_resource_path,
        )
        if resolved.exists():
            return resolved
    return None


def load_structured_data(
    path_like: str | Path,
    *,
    prefer_resource_path: bool = True,
) -> Any:
    resolved = resolve_structured_path(
        path_like,
        prefer_resource_path=prefer_resource_path,
    )

    if not resolved.exists():
        raise FileNotFoundError(f"Structured config not found: {resolved}")

    with open(resolved, "r", encoding="utf-8") as f:
        if resolved.suffix.lower() in {".yaml", ".yml"}:
            return yaml.safe_load(f)
        return json.load(f)


def list_structured_files(directory: str | Path) -> list[Path]:
    base_dir = Path(directory)
    files: list[Path] = []
    for extension in STRUCTURED_EXTENSIONS:
        files.extend(sorted(base_dir.glob(f"*{extension}")))
    return files


def structured_filetypes() -> list[tuple[str, str]]:
    return list(STRUCTURED_FILETYPES)
