"""Re-parse all WA + ID Rate/Rule filings against existing downloaded PDFs.

No re-scraping — this loads the existing `output/wa_final.xlsx` and
`output/id_final.xlsx`, hydrates Filing objects, and re-runs the parser
against already-downloaded PDFs with the latest `src/utils.py` changes.
The output is re-written through `write_excel`, which applies the new
target-lines / withdrawn / launch sheet splits at the output layer.

Intended use: whenever parser patterns change, so we don't need to replay
40-90 minute Playwright runs just to see the effect on classification.

Manually-specified resubmission links (e.g. TRVD pair in ID) are applied
from `RESUBMISSION_LINKS` below.
"""
from __future__ import annotations

import sys
from collections import Counter
from datetime import datetime, date
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR
from src.models import AttachedPdf, Filing
from src.output import (
    INACTIVE_DISPOSITIONS,
    _has_rate_effect,
    _is_inactive_disposition,
    annotate_filings,
    write_excel,
)
from src.utils import parse_rate_effect_pdf

STATES = [
    ("WA", OUTPUT_DIR / "wa_final.xlsx", OUTPUT_DIR / "pdfs" / "WA"),
    ("ID", OUTPUT_DIR / "id_final.xlsx", OUTPUT_DIR / "pdfs" / "ID"),
]

LARGE_PDF_SKIP_PARSE_MB = 15.0
PER_PDF_TIMEOUT_S = 60.0

MEMO_KEYWORDS = ("memo", "summary", "cover letter", "justification", "filing packet")
SKIP_KEYWORDS = ("manual", "tracked changes", "rate pages", "exhibit", "complete", "compare")

# Filings we know are resubmissions of an earlier, rejected filing.
# Populated from findings in the ID investigation.
RESUBMISSION_LINKS: dict[str, str] = {
    "134734348": "TRVD-134534594",  # TRVD-134734348 = approved resubmission of 134534594 (DISAPPROVED same-day)
}

RATE_EFFECT_FIELD_NAMES = (
    "overall_rate_effect",
    "requested_rate_effect",
    "approved_rate_effect",
    "affected_policyholders",
    "written_premium_volume",
    "current_avg_premium",
    "proposed_avg_premium",
    "annual_premium_impact_dollars",
)


def _categorize(name: str) -> str:
    n = name.lower()
    if any(k in n for k in MEMO_KEYWORDS):
        return "memo"
    if any(k in n for k in SKIP_KEYWORDS):
        return "skip"
    return "default"


def _prioritize(paths: list[Path]) -> list[Path]:
    memos, defaults, skips = [], [], []
    for p in paths:
        c = _categorize(p.name)
        (memos if c == "memo" else defaults if c == "default" else skips).append(p)
    memos.sort(key=lambda p: p.name)
    defaults.sort(key=lambda p: p.name)
    skips.sort(key=lambda p: p.name)
    if memos:
        return memos + defaults
    return defaults + skips


def _parse_filing_pdfs(pdf_dir: Path) -> tuple[str, dict]:
    """Walk a filing's PDFs and return (status, merged_fields)."""
    if not pdf_dir.exists():
        return "no_pdfs_attached", {}
    available = sorted(pdf_dir.glob("*.pdf"))
    if not available:
        return "no_pdfs_attached", {}

    ordered = _prioritize(available)
    fields: dict = {}
    parse_attempted = 0
    timeouts = 0
    new_product_hits = 0

    for pdf in ordered:
        size_mb = pdf.stat().st_size / (1024 * 1024)
        if size_mb > LARGE_PDF_SKIP_PARSE_MB:
            continue
        parse_attempted += 1
        try:
            result, status = parse_rate_effect_pdf(
                pdf, tracking_number=pdf_dir.name, timeout_s=PER_PDF_TIMEOUT_S
            )
        except Exception:
            continue
        if status == "timeout":
            timeouts += 1
            continue
        if status == "new_product_launch":
            new_product_hits += 1
        for k, v in result.items():
            fields.setdefault(k, v)

    if fields:
        return "parsed", fields
    if parse_attempted == 0:
        return "all_pdfs_too_large_to_parse", fields
    if new_product_hits > 0:
        return "new_product_launch", fields
    if timeouts > 0 and timeouts == parse_attempted:
        return "timeout_skipped", fields
    if timeouts > 0:
        return "no_fields_matched_with_timeouts", fields
    return "no_fields_matched", fields


def _parse_date(v) -> object:
    if v is None or v == "":
        return None
    if isinstance(v, (datetime, date)):
        return v.date() if isinstance(v, datetime) else v
    try:
        return datetime.fromisoformat(str(v)).date()
    except Exception:
        return None


def _hydrate_filings(path: Path, state: str) -> list[Filing]:
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
        filing = Filing(
            state=g(r, "state") or state,
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
            requested_rate_effect=g(r, "requested_rate_effect"),
            approved_rate_effect=g(r, "approved_rate_effect"),
            overall_rate_effect=g(r, "overall_rate_effect"),
            pdfs=pdfs,
            pdf_parse_status=g(r, "pdf_parse_status") or "not_attempted",
            detail_url=g(r, "detail_url"),
        )
        filings.append(filing)
    return filings


def _reparse_state(state: str, xlsx_path: Path, pdf_root: Path) -> tuple[list[Filing], dict]:
    print(f"\n{'=' * 60}")
    print(f"{state}  —  {xlsx_path.name}")
    print("=" * 60)
    filings = _hydrate_filings(xlsx_path, state)
    print(f"  loaded {len(filings)} filings")

    rate_rule = [f for f in filings if f.filing_type == "Rate/Rule"]
    print(f"  Rate/Rule: {len(rate_rule)}")

    changes = {"rate_removed": [], "rate_added": [], "rate_changed": [], "now_launch": []}

    for i, f in enumerate(rate_rule, 1):
        old_ore = f.overall_rate_effect
        old_req = f.requested_rate_effect
        old_appr = f.approved_rate_effect
        old_status = f.pdf_parse_status

        pdf_dir = pdf_root / f.filing_id
        status, fields = _parse_filing_pdfs(pdf_dir)

        # Reset rate-effect fields before re-populating, so removals stick.
        for k in RATE_EFFECT_FIELD_NAMES:
            setattr(f, k, None)
        for k, v in fields.items():
            if hasattr(f, k):
                setattr(f, k, v)
        f.pdf_parse_status = status
        f.pdf_parse_fields_found = list(fields.keys())

        # Detect classification changes vs prior run
        new_has_rate = any(
            fields.get(k) is not None for k in ("overall_rate_effect", "requested_rate_effect", "approved_rate_effect")
        )
        old_has_rate = any(v is not None for v in (old_ore, old_req, old_appr))

        if old_has_rate and not new_has_rate:
            changes["rate_removed"].append((f.serff_tracking_number, old_ore, old_req, old_appr, status))
        elif new_has_rate and not old_has_rate:
            changes["rate_added"].append((f.serff_tracking_number, fields.get("overall_rate_effect")))
        elif old_has_rate and new_has_rate:
            # Compare overall_rate_effect
            new_ore = fields.get("overall_rate_effect")
            try:
                if old_ore is not None and new_ore is not None and abs(float(old_ore) - float(new_ore)) > 0.01:
                    changes["rate_changed"].append((f.serff_tracking_number, old_ore, new_ore))
                elif (old_ore is None) != (new_ore is None):
                    changes["rate_changed"].append((f.serff_tracking_number, old_ore, new_ore))
            except (TypeError, ValueError):
                pass

        if status == "new_product_launch" and old_status != "new_product_launch":
            changes["now_launch"].append(f.serff_tracking_number)

        if i % 20 == 0:
            print(f"    ... {i}/{len(rate_rule)} re-parsed")

    # Apply resubmission links
    for f in filings:
        if f.filing_id in RESUBMISSION_LINKS:
            f.is_resubmission_of = RESUBMISSION_LINKS[f.filing_id]
            print(f"  link: {f.serff_tracking_number} → is_resubmission_of={f.is_resubmission_of}")

    return filings, changes


def _format_pct(v) -> str:
    if v is None:
        return "None"
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return str(v)


def _summarize(state: str, filings: list[Filing], changes: dict) -> None:
    filings = annotate_filings(filings)
    rate_rule = [f for f in filings if f.filing_type == "Rate/Rule"]

    parsed = [f for f in rate_rule if _has_rate_effect(f)]
    parsed_active = [f for f in parsed if not _is_inactive_disposition(f)]
    parsed_inactive = [f for f in parsed if _is_inactive_disposition(f)]
    launches = [f for f in rate_rule if f.pdf_parse_status == "new_product_launch"]

    target_rr = [f for f in rate_rule if f.in_target_lines]
    target_parsed = [f for f in target_rr if _has_rate_effect(f)]
    target_parsed_active = [f for f in target_parsed if not _is_inactive_disposition(f)]

    print(f"\n--- {state} re-parse summary ---")
    print(f"  Rate/Rule filings:                  {len(rate_rule)}")
    print(f"  with any rate effect extracted:     {len(parsed)}")
    print(f"    active (FILED/APPROVED):          {len(parsed_active)}")
    print(f"    inactive (WITHDRAWN/DISAPPROVED): {len(parsed_inactive)}")
    print(f"  classified as new_product_launch:   {len(launches)}")
    print(f"  in TARGET_LINES (personal lines):   {len(target_rr)}")
    print(f"    with rate effects (core output):  {len(target_parsed_active)}")

    print(f"\n  Changes vs prior run:")
    print(f"    rate effect REMOVED:   {len(changes['rate_removed'])}")
    for serff, ore, req, appr, status in changes["rate_removed"]:
        print(f"      {serff:22s}  was ore={_format_pct(ore)} → status={status}")
    print(f"    rate effect ADDED:     {len(changes['rate_added'])}")
    for serff, ore in changes["rate_added"]:
        print(f"      {serff:22s}  now ore={_format_pct(ore)}")
    print(f"    rate effect CHANGED:   {len(changes['rate_changed'])}")
    for serff, old, new in changes["rate_changed"]:
        print(f"      {serff:22s}  {_format_pct(old)} → {_format_pct(new)}")
    print(f"    reclassified as launch: {len(changes['now_launch'])}")

    if parsed_inactive:
        print(f"\n  Inactive-disposition filings (moved to Withdrawn-Disapproved sheet):")
        for f in parsed_inactive:
            ore = f.overall_rate_effect if f.overall_rate_effect is not None else (
                f.approved_rate_effect if f.approved_rate_effect is not None else f.requested_rate_effect
            )
            disp = f.disposition_status or f.state_status or f.filing_status
            print(f"    {f.serff_tracking_number:22s}  ore={_format_pct(ore)}  disposition={disp}")

    if target_parsed_active:
        print(f"\n  TARGET_LINES rate effects ({state}):")
        for f in target_parsed_active:
            ore = f.overall_rate_effect if f.overall_rate_effect is not None else (
                f.approved_rate_effect if f.approved_rate_effect is not None else f.requested_rate_effect
            )
            toi = (f.sub_type_of_insurance or f.type_of_insurance or "").strip()[:40]
            print(f"    {f.serff_tracking_number:22s}  {f.target_company:15s}  {toi:40s}  {_format_pct(ore)}")

    # LOB audit
    print(f"\n  Type-of-insurance distribution (all filings):")
    toi_counter: Counter = Counter()
    for f in filings:
        key = (f.type_of_insurance or "(none)")[:50]
        toi_counter[key] += 1
    for toi, n in sorted(toi_counter.items(), key=lambda kv: -kv[1])[:15]:
        print(f"    {n:4d}  {toi}")


def main() -> int:
    all_filings_by_state: dict[str, list[Filing]] = {}
    all_changes_by_state: dict[str, dict] = {}

    for state, xlsx_path, pdf_root in STATES:
        if not xlsx_path.exists():
            print(f"  [skip] {xlsx_path} not found")
            continue
        filings, changes = _reparse_state(state, xlsx_path, pdf_root)
        all_filings_by_state[state] = filings
        all_changes_by_state[state] = changes
        out_path = xlsx_path  # Overwrite in place
        write_excel(filings, out_path)
        print(f"  [write] {out_path}")

    # Combined summary
    print(f"\n{'=' * 60}")
    print("CROSS-STATE TOTALS")
    print("=" * 60)
    for state in all_filings_by_state:
        _summarize(state, all_filings_by_state[state], all_changes_by_state[state])

    # Combined TARGET_LINES rate-effects table (core product output)
    print(f"\n{'=' * 60}")
    print("CORE OUTPUT: TARGET_LINES rate effects, active dispositions only (WA + ID)")
    print("=" * 60)
    all_effects = []
    for state, filings in all_filings_by_state.items():
        for f in filings:
            if f.filing_type != "Rate/Rule":
                continue
            if not f.in_target_lines:
                continue
            if not _has_rate_effect(f):
                continue
            if _is_inactive_disposition(f):
                continue
            all_effects.append(f)

    if not all_effects:
        print("  (none)")
    else:
        for f in sorted(all_effects, key=lambda x: (x.state, x.target_company, x.serff_tracking_number)):
            ore = f.overall_rate_effect if f.overall_rate_effect is not None else (
                f.approved_rate_effect if f.approved_rate_effect is not None else f.requested_rate_effect
            )
            toi = (f.sub_type_of_insurance or f.type_of_insurance or "").strip()[:40]
            print(f"  {f.state}  {f.serff_tracking_number:22s}  {f.target_company:15s}  {toi:40s}  {_format_pct(ore)}")
    print(f"\nTotal core-output rate effects: {len(all_effects)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
