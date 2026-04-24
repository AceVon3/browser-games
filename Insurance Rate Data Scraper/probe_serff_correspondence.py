"""Look across several Idaho target filings to see what the Correspondence
panel contains and whether any attachment names signal Disposition Page Data.
"""
from __future__ import annotations
import sys
from pathlib import Path
import openpyxl
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")
from src.config import USER_AGENT, HEADLESS
from src.search import _submit_search, _set_rows_per_page_100, _click_row_to_detail, _back_to_results

# Hit one filing per target carrier group
TARGETS = [
    ("state farm",   "134676753", "SFMA-134676753 auto"),
    ("state farm",   "134872376", "SFMA-134872376 HO pending"),
    ("geico",        "134794993", "GECC-134794993 motorcycle"),
    ("allstate",     "134651294", "ALSE-134651294 NAIC auto"),   # Form A filer expected
    ("travelers",    "134677302", "TRVD-G134677302 TCIC PPA"),
    ("liberty",      "134557139", "LBPM-134557139 LMIC HO"),
]
STATE = "ID"


def dump_filing(page, filing_id: str, label: str):
    print(f"\n=== {label}  filing_id={filing_id} ===")
    ok = _click_row_to_detail(page, filing_id)
    if not ok:
        print("  ! could not click row"); return
    page.wait_for_load_state("networkidle", timeout=20000)
    page.wait_for_timeout(800)

    # All attachments by category (includes Correspondence)
    atts = page.evaluate(
        """() => {
            const panels = [
                ['Forms', 'summaryForm:formAttachmentPanel'],
                ['Rate/Rule', 'summaryForm:rateRuleAttachmentPanel'],
                ['Supporting Documentation', 'summaryForm:supportingDocumentAttachmentPanel'],
                ['Correspondence', 'summaryForm:correspondenceAttachmentPanel'],
            ];
            const out = [];
            for (const [category, pid] of panels) {
                const panel = document.getElementById(pid);
                const content = panel && panel.querySelector('.ui-panel-content');
                if (!content) { out.push({category, status: 'MISSING'}); continue; }
                const txt = (content.textContent || '').trim();
                if (/^none\\s+available/i.test(txt)) { out.push({category, status: 'None Available'}); continue; }
                // rows of document names + pdf links
                const rows = [];
                // each row is a div.row with a doc name and at least one attachment link
                const rowDivs = content.querySelectorAll('div.row');
                for (const r of rowDivs) {
                    const cells = r.querySelectorAll(':scope > div.summaryScheduleItemData');
                    if (!cells.length) continue;
                    const docName = (cells[0].textContent || '').trim();
                    const pdfLinks = [];
                    for (const a of r.querySelectorAll('a[id$="downloadAttachment_"]')) {
                        pdfLinks.push((a.textContent || '').trim());
                    }
                    if (docName || pdfLinks.length) rows.push({docName, pdfLinks});
                }
                out.push({category, status: 'ok', rows});
            }
            return out;
        }"""
    )
    for panel in atts:
        print(f"  [{panel['category']}] {panel.get('status','?')}")
        for r in panel.get("rows", []) or []:
            dn = r.get("docName") or ""
            pdfs = r.get("pdfLinks") or []
            print(f"    doc={dn[:80]!r}")
            for p in pdfs:
                print(f"       pdf: {p[:80]}")
    _back_to_results(page)
    page.wait_for_load_state("domcontentloaded", timeout=15000)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=False)
        page = ctx.new_page()
        # Start one broad search so we capture all 6 filings (approx; may need re-search)
        # Simpler: search once per company
        done_companies = set()
        for company, filing_id, label in TARGETS:
            if company not in done_companies:
                print(f"\n>>> searching company={company!r}")
                if not _submit_search(page, STATE, company):
                    print("  ! search failed"); continue
                _set_rows_per_page_100(page)
                done_companies.add(company)
            # find row, paginating
            for attempt in range(6):
                if page.locator(f'tr[data-rk="{filing_id}"]').count():
                    break
                nxt = page.locator(".ui-paginator-next").first
                if not nxt.count() or "ui-state-disabled" in (nxt.get_attribute("class") or ""):
                    break
                nxt.click()
                page.wait_for_load_state("networkidle", timeout=15000)
            if not page.locator(f'tr[data-rk="{filing_id}"]').count():
                print(f"  ! {filing_id} not visible after pagination for {company}")
                continue
            dump_filing(page, filing_id, label)
        browser.close()


if __name__ == "__main__":
    main()
