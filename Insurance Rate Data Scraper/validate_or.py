"""Validate OR rows: anchor still 14/14, spot-check 3 OR rows, inspect 2 warning filings."""
from pathlib import Path
import openpyxl

WB = openpyxl.load_workbook("output/all_states_final_rates.xlsx", read_only=True)
ws = WB["rate_filings"]
rows = list(ws.iter_rows(values_only=True))
header = list(rows[0])
data = [dict(zip(header, r)) for r in rows[1:]]

# 1) Anchor still present and valid?
anchor = [d for d in data if d["serff_tracking_number"] == "SFMA-134676753"]
print(f"Anchor SFMA-134676753: {len(anchor)} row(s)")
for a in anchor:
    for k, v in a.items():
        print(f"  {k}: {v}")
    print()

# 2) Per-state row counts
from collections import Counter
per_state = Counter(d["state"] for d in data)
print(f"Per state: {dict(per_state)}")

# 3) OR rows summary
or_rows = [d for d in data if d["state"] == "OR"]
print(f"\nOR rows ({len(or_rows)}):")
per_carrier = Counter(d["company_name"] for d in or_rows)
print("  By company_name:")
for c, n in sorted(per_carrier.items(), key=lambda x: (-x[1], x[0])):
    print(f"    {n:3d}  {c}")

# 4) Rate_activity mix
print("\nOR rate_activity mix:")
for a, n in sorted(Counter(d["rate_activity"] for d in or_rows).items()):
    print(f"  {n:3d}  {a}")

# 5) Disposition status mix
print("\nOR disposition_status mix:")
for s, n in sorted(Counter(d["disposition_status"] for d in or_rows).items(), key=lambda x: (str(x[0]) or "")):
    print(f"  {n:3d}  {s!r}")

# 6) Pick 3 OR rows to spot-check: 1 typical, 1 with min/max change, 1 negative change
or_sorted = sorted(or_rows, key=lambda d: (d["overall_rate_impact"] or 0))
spot = []
spot.append(("most negative", or_sorted[0]))
spot.append(("most positive", or_sorted[-1]))
mid = or_sorted[len(or_sorted) // 2]
spot.append(("median", mid))

print("\n=== SPOT-CHECK CANDIDATES ===")
for label, d in spot:
    print(f"\n[{label}] {d['serff_tracking_number']}")
    for k in ["state", "company_name", "sub_type_of_insurance", "effective_date",
             "overall_rate_impact", "overall_indicated_change", "policyholders_affected",
             "written_premium_change", "written_premium_for_program",
             "rate_activity", "disposition_status", "filing_date"]:
        print(f"  {k}: {d[k]}")
