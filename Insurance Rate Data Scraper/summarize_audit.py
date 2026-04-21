"""Read rate_change_audit.json and produce a focused summary of:
  - The N "indicated" classifications (suggest SERFF value is actuarial, not adopted)
  - The N "overall_impact" cases where PDF overall != current value (correctable)
  - The N "ambiguous" cases (no PDF evidence)
For each, show the matched PDF snippets so we can review by hand."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR

audit = json.loads((OUTPUT_DIR / "rate_change_audit.json").read_text(encoding="utf-8"))


def fmt(v):
    return f"{v:+.2f}%" if isinstance(v, (int, float)) else str(v)


print("=" * 70)
print("CASES CLASSIFIED AS 'indicated' (SERFF value matches actuarial indication)")
print("=" * 70)
for a in audit:
    if a["rate_change_type"] != "indicated":
        continue
    print(f"\n  {a['serff_tracking_number']} ({a['state']} {a['carrier']} — {a['line_of_business']})")
    print(f"    SERFF current value:  {a['current_rate_effect_value']}  ({a['current_rate_effect_source']})")
    print(f"    PDF suggested value:  {fmt(a['suggested_overall_value'])}")
    for m in a["matches"][:6]:
        print(f"      [{m['kind']}] {m['value']:+.2f}%  ({m['pdf']})")
        snip = m['snippet'][:200]
        print(f"         \"...{snip}...\"")

print("\n" + "=" * 70)
print("CASES CLASSIFIED AS 'overall_impact' WHERE PDF != current (correctable)")
print("=" * 70)
for a in audit:
    if a["rate_change_type"] != "overall_impact":
        continue
    cur = a["current_value_parsed"]
    sug = a["suggested_overall_value"]
    if cur is None or sug is None or abs(cur - sug) < 0.01:
        continue
    print(f"\n  {a['serff_tracking_number']} ({a['state']} {a['carrier']} — {a['line_of_business']})")
    print(f"    SERFF current value:  {a['current_rate_effect_value']}  ({a['current_rate_effect_source']})")
    print(f"    PDF overall value:    {fmt(sug)}  (delta {sug - cur:+.2f}pp)")
    for m in a["matches"][:8]:
        print(f"      [{m['kind']}] {m['value']:+.2f}%  ({m['pdf']})")
        snip = m['snippet'][:200]
        print(f"         \"...{snip}...\"")

print("\n" + "=" * 70)
print("CASES CLASSIFIED AS 'overall_impact' WHERE PDF == current (confirmed)")
print("=" * 70)
confirmed = [a for a in audit if a["rate_change_type"] == "overall_impact"
             and a["current_value_parsed"] is not None
             and a["suggested_overall_value"] is not None
             and abs(a["current_value_parsed"] - a["suggested_overall_value"]) < 0.01]
for a in confirmed:
    print(f"  {a['serff_tracking_number']:22s} {a['state']:3s} {a['carrier']:14s} {a['current_rate_effect_value']}")

print("\n" + "=" * 70)
print(f"AMBIGUOUS CASES ({sum(1 for a in audit if a['rate_change_type']=='ambiguous')}) — by carrier")
print("=" * 70)
from collections import Counter
amb_by_carrier = Counter()
for a in audit:
    if a["rate_change_type"] == "ambiguous":
        amb_by_carrier[a["carrier"]] += 1
for car, n in amb_by_carrier.most_common():
    print(f"  {car:18s}  {n}")
print()
for a in audit:
    if a["rate_change_type"] != "ambiguous":
        continue
    print(f"  {a['serff_tracking_number']:22s} {a['state']:3s} {a['carrier']:14s} {a['current_rate_effect_value']:>9s}  {a['line_of_business']}")
