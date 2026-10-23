#!/usr/bin/env python3
"""Fail when CI no longer covers all modules or integration test directories."""
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[2]
workflow = (root / ".github/workflows/ci.yml").read_text()


def section(name):
    return workflow.split(f"# BEGIN {name}", 1)[1].split(f"# END {name}", 1)[0]


def check(label, expected, actual):
    if len(actual) != len(set(actual)) or set(actual) != expected:
        sys.exit(f"{label} mismatch: expected {sorted(expected)}, got {sorted(actual)}")
    print(f"{label}: {len(actual)} directories covered")


modules = {str(path.parent.relative_to(root)) for path in root.rglob("go.mod")}
listed = re.findall(r"^\s+- ([a-z0-9/]+)\s*$", section("MODULES"), re.M)
check("Module matrix", modules, listed)

integration = {
    str(path.parent.relative_to(root))
    for path in root.rglob("*_integration_test.go")
}
listed_integration = []
for line in re.findall(r"^\s+modules: (.+)$", section("INTEGRATIONS"), re.M):
    listed_integration.extend(line.split())
# Kubernetes has a separate kind job rather than a Docker service row.
if "make integration MODULES=discovery/kubernetes" not in workflow:
    sys.exit("Missing kind integration invocation")
listed_integration.append("discovery/kubernetes")
check("Integration matrix", integration, listed_integration)
