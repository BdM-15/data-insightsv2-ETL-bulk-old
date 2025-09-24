"""Runtime utilities: enforce Python version and provide helper introspection."""
from __future__ import annotations

import os
import sys
from typing import Tuple

MIN_VERSION: Tuple[int, int] = (3, 13)


def assert_min_python() -> None:
    """Exit the process if the interpreter is below the mandated minimum.

    Constitution v1.10.0 mandates Python 3.13+ for this project.
    """
    if sys.version_info < (*MIN_VERSION, 0):
        msg = (
            f"Python {MIN_VERSION[0]}.{MIN_VERSION[1]}+ required; current interpreter is "
            f"{sys.version.split()[0]} (exiting)."
        )
        print(msg, file=sys.stderr)
        raise SystemExit(2)


def runtime_summary() -> dict:
    """Return a small dict of runtime info for logging / sidecar enrichment."""
    return {
        "python_version": sys.version.split()[0],
        "executable": sys.executable,
        "platform": sys.platform,
        "cwd": os.getcwd(),
    }


if __name__ == "__main__":  # quick manual check helper
    assert_min_python()
    print(runtime_summary())
