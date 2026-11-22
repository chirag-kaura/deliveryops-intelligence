"""Phase 1 smoke test verifying package layout and Python 3.11 runtime."""

import sys

import deliveryops


def test_package_version() -> None:
    assert deliveryops.__version__ == "0.5.0"


def test_python_version_is_311() -> None:
    assert sys.version_info.major == 3
    assert sys.version_info.minor == 11
