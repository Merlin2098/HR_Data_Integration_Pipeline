from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path


HASH_FILE = Path(".venv/.deps_hash")
REQUIREMENTS_GLOB = "requirements*.txt"
ENVIRONMENT_PROFILES = {"local", "cloud"}
PACKAGE_MANAGERS = {"pip", "uv"}
PROFILE_FILE = Path(".template-profile")


def venv_python() -> Path:
    candidates = (
        Path(".venv/Scripts/python.exe"),
        Path(".venv/bin/python"),
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return Path(sys.executable)


def requirement_files(profile: str) -> tuple[Path, ...]:
    names = ["requirements.local.txt"]
    if profile == "cloud":
        names.append("requirements.cloud.txt")
    names.append("requirements.dev.txt")
    files = tuple(Path(name) for name in names if Path(name).is_file())
    if files:
        return files
    return tuple(
        sorted(
            (
                path
                for path in Path(".").glob(REQUIREMENTS_GLOB)
                if path.is_file() and path.parent == Path(".")
            ),
            key=lambda path: path.name.lower(),
        )
    )


def dependency_files(manager: str, profile: str) -> tuple[Path, ...]:
    if manager == "pip":
        return requirement_files(profile)
    files = [Path("pyproject.toml")]
    lock_file = Path("uv.lock")
    if lock_file.exists():
        files.append(lock_file)
    if PROFILE_FILE.exists():
        files.append(PROFILE_FILE)
    return tuple(path for path in files if path.exists())


def dependencies_hash(paths: tuple[Path, ...], manager: str, profile: str) -> str:
    digest = hashlib.md5()
    digest.update(manager.encode("utf-8"))
    digest.update(profile.encode("utf-8"))
    for dependency_path in paths:
        if not dependency_path.exists():
            continue
        digest.update(str(dependency_path.resolve()).encode("utf-8"))
        digest.update(dependency_path.read_bytes())
    return digest.hexdigest()


def uv_command_prefix() -> list[str]:
    if shutil.which("uv"):
        return ["uv"]
    if sys.platform.startswith("win") and shutil.which("py"):
        return ["py", "-3", "-m", "uv"]
    return [str(sys.executable), "-m", "uv"]


def install_command(manager: str, profile: str, paths: tuple[Path, ...]) -> list[str]:
    if manager == "uv":
        command = uv_command_prefix() + ["sync", "--extra", "local", "--group", "dev-local"]
        if profile == "cloud":
            command.extend(["--extra", "cloud", "--group", "dev-cloud"])
        return command

    command = [str(venv_python()), "-m", "pip", "install"]
    for requirement_file in paths:
        command.extend(["-r", requirement_file.as_posix()])
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Synchronize host project dependencies."
    )
    parser.add_argument("--manager", choices=sorted(PACKAGE_MANAGERS), default="pip")
    parser.add_argument("--profile", choices=sorted(ENVIRONMENT_PROFILES))
    return parser.parse_args()


def profile_from_template_file() -> str | None:
    if not PROFILE_FILE.exists():
        return None
    for line in PROFILE_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator and key.strip() == "environment_profile":
            normalized = value.strip().lower()
            if normalized in ENVIRONMENT_PROFILES:
                return normalized
    return None


def resolve_profile(manager: str, selected: str | None) -> str:
    if selected:
        return selected
    if manager == "uv":
        persisted = profile_from_template_file()
        if persisted:
            return persisted
    return "local"


def main() -> None:
    args = parse_args()
    profile = resolve_profile(args.manager, args.profile)
    paths = dependency_files(args.manager, profile)
    if not paths:
        return

    current_hash = dependencies_hash(paths, args.manager, profile)
    if HASH_FILE.exists() and HASH_FILE.read_text() == current_hash:
        print("Dependencies unchanged. Skipping install.")
        return

    print("Installing dependencies...")
    HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(install_command(args.manager, profile, paths), check=True)
    HASH_FILE.write_text(current_hash)
    print("Dependencies updated.")


if __name__ == "__main__":
    main()
