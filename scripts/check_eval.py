"""Fail the release if the AI classification path regresses on Finance-reviewed examples.

Usage: python3 scripts/check_eval.py eval.json <min_accuracy>
Gate applies to the method used in production (AI_CLASSIFY with label descriptions).
"""

import json
import sys

PRODUCTION_METHOD = "AI_CLASSIFY (label descriptions)"

rows = json.load(open(sys.argv[1]))
threshold = float(sys.argv[2])
value = next(iter(rows[0].values()))
value = json.loads(value) if isinstance(value, str) else value

results = {r["method"]: r for r in value["results"]}
for method, r in sorted(results.items()):
    print(f"{method:40s} {r['correct']}/{r['examples']}  accuracy={r['accuracy']:.2f}")

accuracy = results[PRODUCTION_METHOD]["accuracy"]
if accuracy < threshold:
    print(f"FAIL: {PRODUCTION_METHOD} accuracy {accuracy:.2f} < {threshold:.2f}")
    sys.exit(1)
print(f"PASS: {PRODUCTION_METHOD} accuracy {accuracy:.2f} >= {threshold:.2f}")
