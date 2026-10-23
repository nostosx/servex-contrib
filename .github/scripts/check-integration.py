#!/usr/bin/env python3
"""Require passing integration tests and reject skipped or unfinished tests."""
import json
from pathlib import Path
import sys

started = set()
passed = set()
failed = set()
skipped = set()
for line in Path(sys.argv[1]).read_text().splitlines():
    # go test may include a non-JSON tool diagnostic; its exit status is checked
    # by the shell, and the absence of test results also fails this validator.
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    name = event.get("Test", "")
    if not name.startswith("TestIntegration"):
        continue
    key = (event.get("Package", ""), name)
    action = event.get("Action")
    if action == "run":
        started.add(key)
    elif action == "pass":
        passed.add(key)
    elif action == "fail":
        failed.add(key)
    elif action == "skip":
        skipped.add(key)

unfinished = started - passed - failed - skipped
if not passed or skipped or failed or unfinished:
    sys.exit(
        f"Integration results invalid: {len(passed)} passed, "
        f"skipped={sorted(skipped)}, failed={sorted(failed)}, "
        f"unfinished={sorted(unfinished)}"
    )
print(f"Integration results: {len(passed)} tests/subtests passed, none skipped")
