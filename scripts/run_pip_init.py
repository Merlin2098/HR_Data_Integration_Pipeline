from __future__ import annotations

import argparse
import shutil
import stat
import subprocess
import sys
import venv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VENV_DIR = REPO_ROOT / ".venv"
VENV_PYTHON = VENV_DIR / ("Scripts/python.exe" if sys.platform.startswith("win") else "bin/python")
ENVIRONMENT_PROFILES = {"local", "cloud"}


def ensure_venv() -> None:
    if VENV_PYTHON.exists():
        return
    builder = venv.EnvBuilder(with_pip=True)
    builder.create(VENV_DIR)


def remove_readonly(func, path, exc_info) -> None:  # pragma: no cover - platform callback
    Path(path).chmod(stat.S_IWRITE)
    func(path)


def reset_venv() -> None:
    resolved = VENV_DIR.resolve()
    if resolved == REPO_ROOT.resolve() or resolved.parent != REPO_ROOT.resolve():
        raise RuntimeError(f"Refusing to remove unexpected environment path: {resolved}")
    if VENV_DIR.exists():
        shutil.rmtree(VENV_DIR, onexc=remove_readonly)


def install_command(profile: str) -> list[str]:
    command = [
        str(VENV_PYTHON),
        "-m",
        "pip",
        "install",
        "-r",
        "requirements.local.txt",
    ]
    if profile == "cloud":
        command.extend(["-r", "requirements.cloud.txt"])
    command.extend(["-r", "requirements.dev.txt"])
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the local virtual environment if needed and install pip requirements."
    )
    parser.add_argument("--profile", choices=sorted(ENVIRONMENT_PROFILES), default="local")
    parser.add_argument("--dry-run", action="store_true", help="Print the install command without executing it.")
    return parser.parse_args()


def is_recoverable_install_error(error: subprocess.CalledProcessError) -> bool:
    text = ""
    if error.stdout:
        text += error.stdout
    if error.stderr:
        text += error.stderr
    lowered = text.lower()
    return (
        "uninstall-no-record-file" in lowered
        or "no record file was found" in lowered
        or "access denied" in lowered
        or "acceso denegado" in lowered
    )


def run_install(command: list[str]) -> None:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )


def main() -> None:
    args = parse_args()
    if not args.dry_run:
        ensure_venv()

    command = install_command(args.profile)
    print(" ".join(command))
    if args.dry_run:
        return

    try:
        run_install(command)
    except subprocess.CalledProcessError as error:
        if not is_recoverable_install_error(error):
            raise
        print("pip install hit an inconsistent .venv. Rebuilding the environment and retrying...")
        reset_venv()
        ensure_venv()
        run_install(command)


if __name__ == "__main__":
    main()
