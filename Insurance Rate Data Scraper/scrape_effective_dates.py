"""Targeted re-scrape of SERFF detail pages for effective-date labels only.

Takes the 23 target-line rate-change filings and re-visits each detail
page to extract any label containing "effective date" (Requested /
Proposed / Approved / plain). Writes output/effective_dates.json mapping
SERFF tracking number → {requested, approved, proposed, plain, source}.

Does NOT touch rate_changes.xlsx directly — that's `build_rate_changes.py`'s
job. Run this first, then re-run build_rate_changes.py to merge.
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import openpyxl
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import HEADLESS, OUTPUT_DIR, REQUEST_DELAY, USER_AGENT
from src.search import (
    _back_to_results,
    _click_row_to_detail,
    _set_rows_per_page_100,
    _submit_search,
)

SOURCE_XLSX = OUTPUT_DIR / "all_states_final.xlsx"
CACHE_PATH = OUTPUT_DIR / "effective_dates.json"

INACTIVE_TOKENS = ("WITHDRAWN", "DISAPPROVED")


def _to_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "yes")


def _is_inactive(status_fields: tuple) -> bool:
    status = " ".join(str(s or "").upper() for s in status_fields)
    return any(tok in status for tok in INACTIVE_TOKENS)


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _load_targets(path: Path) -> list[dict]:
    """Re-derive the 23 filings: target lines + active + any rate effect."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["Filings"]
    rows = list(ws.iter_rows(values_only=True))
    header = list(rows[0])
    records = [dict(zip(header, r)) for r in rows[1:]]
    out = []
    for r in records:
        if not _to_bool(r.get("in_target_lines")):
            continue
        if _is_inactive((r.get("disposition_status"), r.get("state_status"), r.get("filing_status"))):
            continue
        if (
            _to_float(r.get("overall_rate_effect")) is None
            and _to_float(r.get("requested_rate_effect")) is None
            and _to_float(r.get("approved_rate_effect")) is None
        ):
            continue
        out.append({
            "state": r.get("state") or "",
            "target_company": r.get("target_company") or "",
            "serff_tracking_number": r.get("serff_tracking_number") or "",
            "filing_id": str(r.get("filing_id") or ""),
        })
    return out


_EXTRACT_EFFECTIVE_JS = r"""() => {
    const out = {labels: {}};
    const nodes = Array.from(document.querySelectorAll('label'));
    for (const l of nodes) {
        const key = (l.textContent || '').replace(/:\s*$/, '').trim();
        const keyLower = key.toLowerCase();
        if (!keyLower.includes('effective')) continue;
        const row = l.closest('.row');
        if (!row) continue;
        const val = row.querySelector('div');
        if (!val) continue;
        out.labels[keyLower] = (val.textContent || '').trim();
    }
    return out;
}"""


_DATE_FORMATS = (
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y",
    "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
)


def _parse_date_any(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip().rstrip(".")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _pick_effective_date(labels: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (iso_date, source). Prefer approved > requested > proposed > plain."""
    # Priority-ordered matchers
    priorities = [
        ("approved", ("approved effective", "approved effective date")),
        ("requested", ("requested effective",)),
        ("proposed", ("proposed effective",)),
        ("plain", ("effective date",)),
    ]
    chosen = {}
    for key, text in labels.items():
        for source, needles in priorities:
            if any(n in key for n in needles):
                d = _parse_date_any(text)
                if d and source not in chosen:
                    chosen[source] = d
    for source, _ in priorities:
        if source in chosen:
            return chosen[source], source
    return None, None


def main() -> int:
    targets = _load_targets(SOURCE_XLSX)
    print(f"[targets] {len(targets)} filings to re-visit")

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in targets:
        groups[(t["state"], t["target_company"])].append(t)

    results: dict[str, dict] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        try:
            for (state, company), group in groups.items():
                print(f"\n[group] {state} / {company}: {len(group)} filings")
                ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=True)
                page = ctx.new_page()
                try:
                    if not _submit_search(page, state, company):
                        print(f"  ! search failed, skipping")
                        for t in group:
                            results[t["serff_tracking_number"]] = {
                                "effective_date": None, "source": None,
                                "all_labels": {}, "error": "search_failed",
                            }
                        continue
                    _set_rows_per_page_100(page)

                    for t in group:
                        serff = t["serff_tracking_number"]
                        fid = t["filing_id"]
                        labels: dict = {}
                        error = None
                        try:
                            if not page.locator(f'tr[data-rk="{fid}"]').count():
                                _set_rows_per_page_100(page)
                            if not page.locator(f'tr[data-rk="{fid}"]').count():
                                error = "row_not_found"
                            elif not _click_row_to_detail(page, fid):
                                error = "could_not_open_detail"
                            else:
                                data = page.evaluate(_EXTRACT_EFFECTIVE_JS)
                                labels = data.get("labels") or {}
                                _back_to_results(page)
                        except Exception as e:
                            error = f"exception:{type(e).__name__}:{e}"
                            try:
                                _back_to_results(page)
                            except Exception:
                                pass

                        eff, src = _pick_effective_date(labels)
                        results[serff] = {
                            "effective_date": eff,
                            "source": src,
                            "all_labels": labels,
                            "error": error,
                        }
                        flag = "Y" if eff else ("!" if error else "-")
                        label_summary = ", ".join(f"{k}={v!r}" for k, v in labels.items()) or "(no effective-date labels)"
                        print(f"  [{flag}] {serff:22s} eff={eff or '-'} src={src or '-'}  | {label_summary}")
                        if error:
                            print(f"       error: {error}")
                        time.sleep(REQUEST_DELAY)
                finally:
                    ctx.close()
        finally:
            browser.close()

    CACHE_PATH.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    populated = sum(1 for r in results.values() if r.get("effective_date"))
    errors = sum(1 for r in results.values() if r.get("error"))
    no_field = sum(1 for r in results.values() if not r.get("effective_date") and not r.get("error"))
    print(f"\n[write] {CACHE_PATH}")
    print(f"effective_date populated: {populated}/{len(results)}")
    print(f"scrape errors:            {errors}")
    print(f"no effective-date labels: {no_field}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
