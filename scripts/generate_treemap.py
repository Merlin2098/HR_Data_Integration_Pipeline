from __future__ import annotations

import argparse
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "treemap.md"
EXCLUDED_ROOTS = {".git", "ai"}
EXCLUDED_FILES = {".gitignore", "treemap.md"}


@dataclass(frozen=True)
class IgnoreRule:
    pattern: str
    directory_only: bool


def load_ignore_rules(ignore_file: Path) -> list[IgnoreRule]:
    if not ignore_file.exists():
        return []

    rules: list[IgnoreRule] = []
    for raw_line in ignore_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue

        directory_only = line.endswith("/")
        pattern = line.rstrip("/")
        if pattern:
            rules.append(IgnoreRule(pattern=pattern, directory_only=directory_only))
    return rules


def matches_rule(relative_path: str, rule: IgnoreRule, is_dir: bool) -> bool:
    if rule.directory_only and not is_dir:
        return False

    name = relative_path.rsplit("/", maxsplit=1)[-1]
    candidates = [relative_path, name]

    if "/" not in rule.pattern:
        candidates.append(f"{relative_path}/")

    for candidate in candidates:
        if fnmatch(candidate, rule.pattern):
            return True
    return False


def is_excluded(path: Path, rules: list[IgnoreRule]) -> bool:
    relative = path.relative_to(REPO_ROOT)
    relative_str = relative.as_posix()
    top_level = relative.parts[0]

    if top_level in EXCLUDED_ROOTS:
        return True
    if relative.name in EXCLUDED_FILES:
        return True

    for rule in rules:
        if matches_rule(relative_str, rule, path.is_dir()):
            return True
    return False


def iter_visible_children(directory: Path, rules: list[IgnoreRule]) -> list[Path]:
    children = [child for child in directory.iterdir() if not is_excluded(child, rules)]
    return sorted(children, key=lambda item: (not item.is_dir(), item.name.lower()))


def build_tree_lines(
    directory: Path, rules: list[IgnoreRule], prefix: str = ""
) -> list[str]:
    children = iter_visible_children(directory, rules)
    lines: list[str] = []

    for index, child in enumerate(children):
        is_last = index == len(children) - 1
        connector = "`-- " if is_last else "|-- "
        suffix = "/" if child.is_dir() else ""
        lines.append(f"{prefix}{connector}{child.name}{suffix}")

        if child.is_dir():
            extension = "    " if is_last else "|   "
            lines.extend(build_tree_lines(child, rules, prefix + extension))

    return lines


def write_treemap(output_path: Path) -> Path:
    rules = load_ignore_rules(REPO_ROOT / ".gitignore")
    tree_lines = [REPO_ROOT.name + "/"]
    tree_lines.extend(build_tree_lines(REPO_ROOT, rules))

    content = "\n".join(
        [
            "```text",
            *tree_lines,
            "```",
            "",
        ]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a markdown tree map for the repository."
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path to the generated markdown file. Defaults to docs/treemap.md.",
    )
    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    result = write_treemap(output_path)
    print(result)


if __name__ == "__main__":
    main()
