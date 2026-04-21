"""Combine wa_final.xlsx + id_final.xlsx + co_final.xlsx into one workbook.

Produces `output/all_states_final.xlsx` with the full 7-sheet structure
spanning WA + ID + CO. Also prints the CO-only and combined metrics table.

No scraping or parsing happens here — purely joins already-enriched state
workbooks. Re-run after any state's final xlsx is updated.
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR
from src.models import AttachedPdf, Filing
from src.output import (
    _has_rate_effect,
    _is_inactive_disposition,
    annotate_filings,
    write_excel,
)

STATES = [
    ("WA", OUTPUT_DIR / "wa_final.xlsx"),
    ("ID", OUTPUT_DIR / "id_final.xlsx"),
    ("CO", OUTPUT_DIR / "co_final.xlsx"),
]

OUT_PATH = OUTPUT_DIR / "all_states_final.xlsx"


def _parse_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.fromisoformat(str(v)).date()
    except Exception:
        return None


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_bool(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _hydrate(path: Path, default_state: str) -> list[Filing]:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb["Filings"]
    rows = list(ws.iter_rows(values_only=True))
    header = list(rows[0])
    idx = {h: i for i, h in enumerate(header)}

    def g(r, col):
        i = idx.get(col)
        return r[i] if i is not None else None

    filings: list[Filing] = []
    for r in rows[1:]:
        naic_raw = g(r, "naic_codes") or ""
        naic = [c.strip() for c in str(naic_raw).split(";") if c.strip()]
        pdfs_raw = g(r, "pdfs") or ""
        pdf_urls = [u.strip() for u in str(pdfs_raw).split(";") if u.strip()]
        pdfs = [AttachedPdf(category="unknown", display_name="", url=u) for u in pdf_urls]
        fields_raw = g(r, "pdf_parse_fields_found") or ""
        fields_found = [x.strip() for x in str(fields_raw).split(";") if x.strip()]

        filings.append(
            Filing(
                state=g(r, "state") or default_state,
                serff_tracking_number=g(r, "serff_tracking_number") or "",
                filing_id=str(g(r, "filing_id") or ""),
                company_name=g(r, "company_name") or "",
                target_company=g(r, "target_company") or "",
                naic_codes=naic,
                product_name=g(r, "product_name"),
                type_of_insurance=g(r, "type_of_insurance"),
                sub_type_of_insurance=g(r, "sub_type_of_insurance"),
                filing_type=g(r, "filing_type"),
                filing_status=g(r, "filing_status"),
                submission_date=_parse_date(g(r, "submission_date")),
                disposition_date=_parse_date(g(r, "disposition_date")),
                disposition_status=g(r, "disposition_status"),
                state_status=g(r, "state_status"),
                requested_rate_effect=_to_float(g(r, "requested_rate_effect")),
                approved_rate_effect=_to_float(g(r, "approved_rate_effect")),
                overall_rate_effect=_to_float(g(r, "overall_rate_effect")),
                affected_policyholders=_to_int(g(r, "affected_policyholders")),
                written_premium_volume=_to_float(g(r, "written_premium_volume")),
                annual_premium_impact_dollars=_to_float(g(r, "annual_premium_impact_dollars")),
                current_avg_premium=_to_float(g(r, "current_avg_premium")),
                proposed_avg_premium=_to_float(g(r, "proposed_avg_premium")),
                premium_change_dollars=_to_float(g(r, "premium_change_dollars")),
                program_name=g(r, "program_name"),
                filing_reason=g(r, "filing_reason"),
                prior_approval=_to_bool(g(r, "prior_approval")),
                pdfs=pdfs,
                pdf_parse_status=g(r, "pdf_parse_status") or "not_attempted",
                pdf_parse_fields_found=fields_found,
                detail_url=g(r, "detail_url"),
                is_resubmission_of=g(r, "is_resubmission_of"),
            )
        )
    return filings


def _format_pct(v) -> str:
    if v is None:
        return "None"
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return str(v)


def _rate_pct(f: Filing):
    if f.overall_rate_effect is not None:
        return f.overall_rate_effect
    if f.approved_rate_effect is not None:
        return f.approved_rate_effect
    return f.requested_rate_effect


def _metrics(filings: list[Filing]) -> dict:
    rate_rule = [f for f in filings if f.filing_type == "Rate/Rule"]
    parsed = [f for f in rate_rule if _has_rate_effect(f)]
    parsed_active = [f for f in parsed if not _is_inactive_disposition(f)]
    parsed_inactive = [f for f in parsed if _is_inactive_disposition(f)]
    launches = [f for f in rate_rule if f.pdf_parse_status == "new_product_launch"]
    target_rr = [f for f in rate_rule if f.in_target_lines]
    target_parsed_active = [
        f for f in target_rr if _has_rate_effect(f) and not _is_inactive_disposition(f)
    ]
    return {
        "total": len(filings),
        "rate_rule": len(rate_rule),
        "rate_effects": len(parsed),
        "active": len(parsed_active),
        "inactive": len(parsed_inactive),
        "launches": len(launches),
        "target_rr": len(target_rr),
        "core": len(target_parsed_active),
        "core_filings": target_parsed_active,
        "inactive_filings": parsed_inactive,
    }


def _print_metrics(label: str, m: dict) -> None:
    print(f"\n--- {label} ---")
    print(f"  Total filings:                      {m['total']}")
    print(f"  Rate/Rule filings:                  {m['rate_rule']}")
    print(f"  with any rate effect extracted:     {m['rate_effects']}")
    print(f"    active (FILED/APPROVED):          {m['active']}")
    print(f"    inactive (WITHDRAWN/DISAPPROVED): {m['inactive']}")
    print(f"  classified as new_product_launch:   {m['launches']}")
    print(f"  in TARGET_LINES (personal lines):   {m['target_rr']}")
    print(f"  >>> CORE OUTPUT (target × active):  {m['core']}")


def _print_core_table(label: str, filings: list[Filing]) -> None:
    if not filings:
        return
    print(f"\n  Core-output rate effects ({label}):")
    for f in sorted(filings, key=lambda x: (x.state, x.target_company, x.serff_tracking_number)):
        ore = _rate_pct(f)
        toi = (f.sub_type_of_insurance or f.type_of_insurance or "").strip()[:40]
        print(f"    {f.state}  {f.serff_tracking_number:22s}  {f.target_company:15s}  {toi:40s}  {_format_pct(ore)}")


def main() -> int:
    by_state: dict[str, list[Filing]] = {}
    for state, path in STATES:
        if not path.exists():
            print(f"[skip] {path.name} not found")
            continue
        filings = _hydrate(path, state)
        filings = annotate_filings(filings)
        by_state[state] = filings
        print(f"[load] {state}: {len(filings)} filings from {path.name}")

    if "CO" not in by_state:
        print("ERROR: co_final.xlsx missing — run run_co_full.py first.")
        return 1

    all_filings: list[Filing] = []
    for state, filings in by_state.items():
        all_filings.extend(filings)

    print(f"\n[write] {OUT_PATH} ({len(all_filings)} filings total)")
    write_excel(all_filings, OUT_PATH)

    print("\n" + "=" * 60)
    print("METRICS")
    print("=" * 60)

    # Per-state
    for state in ("WA", "ID", "CO"):
        if state in by_state:
            _print_metrics(state, _metrics(by_state[state]))

    # CO-only detail
    if "CO" in by_state:
        co_m = _metrics(by_state["CO"])
        _print_core_table("CO", co_m["core_filings"])
        if co_m["inactive_filings"]:
            print(f"\n  CO inactive-disposition rate effects (Withdrawn/Disapproved sheet):")
            for f in co_m["inactive_filings"]:
                disp = f.disposition_status or f.state_status or f.filing_status
                print(f"    {f.serff_tracking_number:22s}  ore={_format_pct(_rate_pct(f))}  disposition={disp}")

    # Combined
    combined_m = _metrics(all_filings)
    _print_metrics("COMBINED WA + ID + CO", combined_m)
    _print_core_table("WA + ID + CO", combined_m["core_filings"])

    # Headline
    print("\n" + "=" * 60)
    print("HEADLINE")
    print("=" * 60)
    print(f">>> Core output: {combined_m['core']} target-lines rate effects "
          f"across WA + ID + CO (active dispositions only).")

    # Carrier breakdown of core output
    by_carrier: Counter = Counter()
    for f in combined_m["core_filings"]:
        by_carrier[f.target_company] += 1
    if by_carrier:
        print("\nCore output by carrier:")
        for carrier, n in by_carrier.most_common():
            print(f"    {carrier:20s}  {n}")

    # Per-state breakdown of core output
    by_state_core: Counter = Counter()
    for f in combined_m["core_filings"]:
        by_state_core[f.state] += 1
    if by_state_core:
        print("\nCore output by state:")
        for state in ("WA", "ID", "CO"):
            print(f"    {state}  {by_state_core.get(state, 0)}")

    print(f"\nFinal workbook: {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
