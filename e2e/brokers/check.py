"""Reads agent JSON lines on stdin; checks every agent saw every other before and after the restart."""
import json
import sys

names = sys.argv[1].split()
seen, errors = set(), []
for line in sys.stdin:
    event = json.loads(line)
    if event["ev"] == "seen":
        seen.add((event["agent"], event["from"], event["phase"]))
    if event["ev"] == "error":
        errors.append(event)

failed = bool(errors)
for phase, label in ((1, "before restart"), (2, "after restart")):
    missing = [f"{a}<-{b}" for a in names for b in names if a != b and (a, b, phase) not in seen]
    status = "FAIL" if missing else "ok  "
    detail = "missing " + ", ".join(missing) if missing else f"all {len(names)} agents saw each other"
    print(f"    {status}  {label}: {detail}")
    failed = failed or bool(missing)
for event in errors:
    print(f"    FAIL  {event['agent']} errored: {event['error']}")
sys.exit(1 if failed else 0)
