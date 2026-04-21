"""Inspect TOI/Sub-TOI columns in all_states_final.xlsx to design the classifier."""
from __future__ import annotations
import sys
from collections import Counter
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR

wb = openpyxl.load_workbook(OUTPUT_DIR / "all_states_final.xlsx", read_only=True, data_only=True)
ws = wb["Filings"]
rows = list(ws.iter_rows(values_only=True))
header = list(rows[0])
print(f"Headers ({len(header)}):")
for i, h in enumerate(header):
    print(f"  {i:3d}  {h}")

records = [dict(zip(header, r)) for r in rows[1:]]
print(f"\nTotal filings: {len(records)}")

print("\n=== type_of_insurance value counts (top 30) ===")
toi_counts = Counter(str(r.get("type_of_insurance") or "(missing)") for r in records)
for v, n in toi_counts.most_common(30):
    print(f"  {n:4d}  {v!r}")

print("\n=== sub_type_of_insurance value counts (top 50) ===")
sub_counts = Counter(str(r.get("sub_type_of_insurance") or "(missing)") for r in records)
for v, n in sub_counts.most_common(50):
    print(f"  {n:4d}  {v!r}")

# Cross-check: how do current in_target_lines=True rows look?
print("\n=== Currently in_target_lines=True breakdown ===")
true_rows = [r for r in records if str(r.get("in_target_lines") or "").lower() in ("true", "1", "yes")]
print(f"Total in_target_lines=True: {len(true_rows)}")
true_toi = Counter(str(r.get("type_of_insurance") or "(missing)") for r in true_rows)
for v, n in true_toi.most_common():
    print(f"  TOI {n:4d}  {v!r}")
true_sub = Counter(str(r.get("sub_type_of_insurance") or "(missing)") for r in true_rows)
print()
for v, n in true_sub.most_common():
    print(f"  Sub-TOI {n:4d}  {v!r}")
