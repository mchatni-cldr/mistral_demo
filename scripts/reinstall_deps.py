#!/usr/bin/env python3
"""Cleanly (re)install this project's dependencies.

Upgrading fastmcp 2.x -> 4.x in place leaves a broken install:

    ImportError: cannot import name 'FastMCP' from 'fastmcp' (unknown location)

pip installs 4.0.0's files, then uninstalls 2.9.2 and deletes the filenames the
two versions share -- __init__.py, settings.py, exceptions.py -- leaving a
directory Python treats as an empty namespace package. `--force-reinstall` does
not fix it, because the leftover directory is never removed.

This purges every fastmcp/mcp distribution and directory first, then installs
from requirements.txt and verifies the result.

Run from the project root, in a Cloudera AI session:

    python scripts/reinstall_deps.py
"""

import os
import shutil
import site
import subprocess
import sys

DISTS = ["fastmcp", "fastmcp-slim", "mcp"]
PKG_DIRS = ["fastmcp", "mcp"]


def pip(*args) -> int:
    return subprocess.call([sys.executable, "-m", "pip", *args])


def main() -> int:
    print("==> uninstalling existing distributions")
    pip("uninstall", "-y", *DISTS)

    print("==> removing leftover package directories")
    roots = set(site.getsitepackages())
    try:
        roots.add(site.getusersitepackages())  # where Cloudera AI installs
    except AttributeError:
        pass
    for root in roots:
        for pkg in PKG_DIRS:
            path = os.path.join(root, pkg)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
                print(f"    removed {path}")

    print("==> installing from requirements.txt")
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    req = os.path.join(here, "requirements.txt")
    if pip("install", "-r", req) != 0:
        print("!! pip install failed")
        return 1

    print("==> verifying")
    check = (
        "from fastmcp import FastMCP;"
        "from importlib.metadata import version;"
        "import mcp.types as t;"
        "print('    fastmcp', version('fastmcp'), '| mcp', version('mcp'));"
        "print('    protocol:', t.LATEST_PROTOCOL_VERSION)"
    )
    if subprocess.call([sys.executable, "-c", check]) != 0:
        print("!! verification failed -- the install is still broken")
        return 1

    print("\nOK. Now restart the application so it picks up the new packages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
