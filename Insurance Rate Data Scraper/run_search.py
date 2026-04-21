"""Step 4 checkpoint runner: search-only scrape for a single (state, company) pair.

Writes results to output/{state}_{company}_search.xlsx. Use --all to run the
full matrix of TARGET_COMPANIES x STATES (deferred until the single-pair run
is validated).

    ./.venv/Scripts/python.exe run_search.py                     # State Farm / WA
    ./.venv/Scripts/python.exe run_search.py --company GEICO     # GEICO / WA
    ./.venv/Scripts/python.exe run_search.py --all               # every target x state
"""
from __future__ import annotations

import argparse
from pathlib import Path

from src.config import OUTPUT_DIR, STATES, TARGET_COMPANIES
from src.output import write_excel
from src.search import search_all


def _slug(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s.lower()).strip("_")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--state", default="WA")
    ap.add_argument("--company", default="State Farm")
    ap.add_argument("--all", action="store_true", help="Run every TARGET_COMPANY x STATE pair")
    ap.add_argument("--all-companies", action="store_true",
                    help="Run all TARGET_COMPANIES against --state (default WA)")
    args = ap.parse_args()

    if args.all:
        pairs = [(s, c) for s in STATES for c in TARGET_COMPANIES]
        out_name = "all_search.xlsx"
    elif args.all_companies:
        pairs = [(args.state, c) for c in TARGET_COMPANIES]
        out_name = f"{_slug(args.state)}_all_companies_search.xlsx"
    else:
        pairs = [(args.state, args.company)]
        out_name = f"{_slug(args.state)}_{_slug(args.company)}_search.xlsx"

    filings = search_all(pairs)
    out = Path(OUTPUT_DIR) / out_name
    write_excel(filings, out)

    print(f"\n=== Summary ===")
    print(f"Total filings: {len(filings)}")
    by_group: dict[tuple[str, str], int] = {}
    for f in filings:
        key = (f.state, f.target_company)
        by_group[key] = by_group.get(key, 0) + 1
    for (state, company), n in sorted(by_group.items()):
        print(f"  {state} / {company}: {n}")
    print(f"Wrote: {out}")
    return 0 if filings else 1


if __name__ == "__main__":
    raise SystemExit(main())
