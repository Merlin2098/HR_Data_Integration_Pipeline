from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_PATH = REPO_ROOT / "artifacts" / "data_platform_bundle.zip"
INCLUDE_DIRS = [REPO_ROOT / "src"]
CLOUD_REQUIREMENTS = REPO_ROOT / "requirements.cloud.txt"
PYPROJECT = REPO_ROOT / "pyproject.toml"
UV_LOCK = REPO_ROOT / "uv.lock"


def detect_package_manager(selected: str) -> str:
    if selected != "auto":
        return selected
    if CLOUD_REQUIREMENTS.exists():
        return "pip"
    if PYPROJECT.exists() and UV_LOCK.exists():
        return "uv"
    if PYPROJECT.exists():
        return "uv"
    return "pip"


def runtime_requirements_text(package_manager: str) -> str:
    if package_manager == "uv":
        result = subprocess.run(
            [
                "uv",
                "export",
                "--no-dev",
                "--extra",
                "cloud",
                "--format",
                "requirements.txt",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    return CLOUD_REQUIREMENTS.read_text(encoding="utf-8").strip() + "\n"


def build_bundle(package_manager: str) -> Path:
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(ARTIFACT_PATH, "w", compression=ZIP_DEFLATED) as archive:
        for directory in INCLUDE_DIRS:
            for file_path in sorted(directory.rglob("*")):
                if file_path.is_dir() and file_path.name == "__pycache__":
                    continue
                if file_path.suffix == ".pyc":
                    continue
                if "__pycache__" in file_path.parts:
                    continue
                if file_path.is_file():
                    archive.write(file_path, file_path.relative_to(REPO_ROOT))
        # Ship a resolved cloud runtime requirements file in the bundle.
        archive.writestr("requirements.txt", runtime_requirements_text(package_manager))
    return ARTIFACT_PATH


def clean_bundle() -> None:
    if ARTIFACT_PATH.exists():
        try:
            ARTIFACT_PATH.unlink()
        except PermissionError:
            print(
                f"Could not remove {ARTIFACT_PATH} because it is currently in use. Close any process using the bundle and retry.",
                file=sys.stderr,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build or remove the deployment bundle."
    )
    parser.add_argument(
        "--clean", action="store_true", help="Remove the generated bundle."
    )
    parser.add_argument(
        "--package-manager",
        choices=("auto", "pip", "uv"),
        default="auto",
        help="Dependency source to use for the bundled requirements.txt.",
    )
    args = parser.parse_args()

    if args.clean:
        clean_bundle()
        return

    artifact = build_bundle(detect_package_manager(args.package_manager))
    print(artifact)


if __name__ == "__main__":
    main()
