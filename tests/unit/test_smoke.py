"""Phase 1 smoke test verifying package layout, launchers, and Python 3.11 runtime."""

import sys
from pathlib import Path

import deliveryops

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_package_version() -> None:
    assert deliveryops.__version__ == "0.6.0"


def test_python_version_is_311() -> None:
    assert sys.version_info.major == 3
    assert sys.version_info.minor == 11


def test_launcher_scripts_exist() -> None:
    assert (REPO_ROOT / "start.bat").is_file()
    assert (REPO_ROOT / "stop.bat").is_file()
    assert (REPO_ROOT / "scripts" / "run_platform.py").is_file()
