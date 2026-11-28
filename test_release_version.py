# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Keep tag validation compatible with the package's dynamic version."""

import subprocess
import sys
from pathlib import Path


def test_release_reads_the_dynamic_package_version():
    workflow = Path(__file__).parent / ".github/workflows/release.yml"
    line = next(
        line for line in workflow.read_text().splitlines() if "PROJECT_VERSION=" in line
    )
    code = line.split("python -c '", 1)[1].split("'", 1)[0]
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    from workflow_engine import __version__

    assert result.stdout.strip() == __version__


def test_sdist_ships_only_the_package():
    """examples/ and samples/ are test material and must stay out of the sdist."""
    root = Path(__file__).parent
    manifest = (root / "MANIFEST.in").read_text(encoding="utf-8")
    assert "recursive-include examples" not in manifest
    assert "prune examples" in manifest
    assert "prune samples" in manifest
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert 'include = ["workflow_engine*"]' in pyproject
