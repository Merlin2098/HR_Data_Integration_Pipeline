from __future__ import annotations

import subprocess
import sys


NO_TESTS_COLLECTED_EXIT_CODE = 5


def main(argv: list[str] | None = None) -> int:
    command = [sys.executable, "-m", "pytest", *(argv or sys.argv[1:])]
    result = subprocess.run(command, check=False)
    if result.returncode == NO_TESTS_COLLECTED_EXIT_CODE:
        print("No tests were collected. Treating this as a successful no-op.")
        return 0
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
