"""Detail-page scraping and PDF download for SERFF filings.

From the detail page (reached by clicking a row on the results page — direct
navigation via filingSummary.xhtml URL returns HTTP 500 because the JSF
ViewState is bound to the row click), we extract:

  * Metadata fields (dates, status, product name, sub-TOI, NAIC codes)
  * Attachments grouped by category: Forms, Rate/Rule, Supporting
    Documentation, Correspondence

For Rate/Rule filings only, we download prioritized PDFs (Supporting
Documentation first, then non-manual Rate/Rule files), cap the set at
`MAX_PDFS_PER_FILING`, and route them through `utils.parse_rate_effect_pdf`
to populate rate-effect fields. Huge PDFs (>15MB, typically 4000-page manuals)
are downloaded for archival but skipped for parsing to avoid hanging
pdfplumber.
"""
from __future__ import annotations

import re
import time
import traceback
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

from playwright.sync_api import Browser, Page, sync_playwright

from .config import (
    HEADLESS,
    PDF_DIR,
    REQUEST_DELAY,
    USER_AGENT,
)
from .models import AttachedPdf, Filing
from .search import (
    _back_to_results,
    _click_row_to_detail,
    _parse_date,
    _set_rows_per_page_100,
    _submit_search,
)
from .utils import parse_rate_effect_pdf

MAX_PDFS_PER_FILING = 10
LARGE_PDF_SKIP_PARSE_MB = 15.0


def scrape_detail_fields(page: Page, filing: Filing) -> None:
    """Populate labeled metadata on `filing` from the filing summary page.

    Fills: submission_date, disposition_date, disposition_status, state_status,
    product_name, type_of_insurance, sub_type_of_insurance, filing_status, naic_codes.
    """
    fields = page.evaluate(
        """() => {
            const out = {};
            const labels = Array.from(document.querySelectorAll('label'));
            for (const l of labels) {
                const key = (l.textContent || '').replace(/:\\s*$/, '').trim().toLowerCase();
                if (!key) continue;
                const row = l.closest('.row');
                if (!row) continue;
                const val = row.querySelector('div');
                if (!val) continue;
                out[key] = (val.textContent || '').trim();
            }
            // Company Information: rows under the first 'Company Information' h2
            const comp = Array.from(document.querySelectorAll('h2'))
                .find(h => /company\\s+information/i.test(h.textContent || ''));
            const companies = [];
            if (comp) {
                let n = comp.nextElementSibling;
                while (n && n.tagName !== 'H2' && n.id !== 'filingContainer') {
                    if (n.classList && n.classList.contains('row')) {
                        const spans = n.querySelectorAll('span.col-sm-2.text-center');
                        if (spans.length) {
                            const code = (spans[0].textContent || '').trim();
                            const nameEl = n.querySelector('span.col-sm-3');
                            const name = nameEl ? (nameEl.textContent || '').trim() : '';
                            if (/^\\d{3,5}$/.test(code)) companies.push({ code, name });
                        }
                    }
                    n = n.nextElementSibling;
                }
            }
            out['__companies__'] = companies;
            return out;
        }"""
    )

    def _get(name: str) -> Optional[str]:
        v = fields.get(name)
        return v.strip() if isinstance(v, str) and v.strip() else None

    if not filing.submission_date:
        filing.submission_date = _parse_date(_get("submission date"))
    filing.disposition_date = _parse_date(_get("disposition date"))
    filing.disposition_status = _get("disposition status")
    filing.state_status = _get("state status")
    if not filing.product_name:
        filing.product_name = _get("product name")
    if not filing.type_of_insurance:
        filing.type_of_insurance = _get("type of insurance")
    if not filing.sub_type_of_insurance:
        filing.sub_type_of_insurance = _get("sub type of insurance")
    if not filing.filing_status:
        filing.filing_status = _get("filing status")

    companies = fields.get("__companies__") or []
    if companies and not filing.naic_codes:
        filing.naic_codes = [c["code"] for c in companies if c.get("code")]


def list_attachments(page: Page) -> list[dict]:
    """Return [{category, document_name, display_name, attachment_id}] for every
    downloadable attachment on the detail page.
    """
    return page.evaluate(
        """() => {
            const panels = [
                ['Forms', 'summaryForm:formAttachmentPanel'],
                ['Rate/Rule', 'summaryForm:rateRuleAttachmentPanel'],
                ['Supporting Documentation', 'summaryForm:supportingDocumentAttachmentPanel'],
                ['Correspondence', 'summaryForm:correspondenceAttachmentPanel'],
            ];
            const out = [];
            for (const [category, panelId] of panels) {
                const panel = document.getElementById(panelId);
                if (!panel) continue;
                const content = panel.querySelector('.ui-panel-content');
                if (!content) continue;
                if (/^\\s*none\\s+available\\s*$/i.test((content.textContent || '').trim())) continue;
                const anchors = content.querySelectorAll('a[id$="downloadAttachment_"]');
                for (const a of anchors) {
                    const cell = a.closest('div.summaryScheduleItemData');
                    const outerRow = cell ? cell.parentElement : null;
                    let docName = '';
                    if (outerRow) {
                        const cells = outerRow.querySelectorAll(':scope > div.summaryScheduleItemData');
                        if (cells.length) {
                            docName = (cells[0].textContent || '').trim();
                            if (cells[0] === cell && cells.length > 1) {
                                // unlikely: attachment cell is first — take next
                                docName = (cells[1].textContent || '').trim();
                            }
                        }
                    }
                    out.push({
                        category,
                        document_name: docName,
                        display_name: (a.textContent || '').trim(),
                        attachment_id: a.id,
                    });
                }
            }
            return out;
        }"""
    )


def _is_manual_like(att: dict) -> bool:
    """Heuristic: Rate/Rule attachments with 'manual' in doc/file name are
    typically huge rate pages — skip unless nothing else is available."""
    text = f"{att.get('document_name','')} {att.get('display_name','')}".lower()
    return bool(re.search(r"\bmanual\b", text))


def prioritize_attachments(attachments: list[dict], cap: int = MAX_PDFS_PER_FILING) -> list[dict]:
    """Supporting Documentation first (memos), then non-manual Rate/Rule,
    then fall back to manuals/forms/correspondence only if we still have slots.
    """
    supporting = [a for a in attachments if a["category"] == "Supporting Documentation"]
    rate_rule = [a for a in attachments if a["category"] == "Rate/Rule"]
    forms = [a for a in attachments if a["category"] == "Forms"]
    corr = [a for a in attachments if a["category"] == "Correspondence"]

    rate_non_manual = [a for a in rate_rule if not _is_manual_like(a)]
    rate_manual = [a for a in rate_rule if _is_manual_like(a)]

    ordered: list[dict] = []
    for pool in (supporting, rate_non_manual, rate_manual, forms, corr):
        for a in pool:
            if len(ordered) >= cap:
                break
            ordered.append(a)
        if len(ordered) >= cap:
            break
    return ordered


def _safe_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name).strip() or "attachment.pdf"


def download_attachment(page: Page, att: dict, dest_dir: Path) -> Optional[Path]:
    """Click the attachment link via its id; return the saved path or None on failure."""
    dest = dest_dir / _safe_filename(att["display_name"])
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    try:
        with page.expect_download(timeout=60000) as dl_info:
            page.evaluate(f"document.getElementById('{att['attachment_id']}').click();")
        dl = dl_info.value
        dl.save_as(str(dest))
        return dest
    except Exception as e:
        print(f"    [pdf fail] {att['display_name']}: {e}", flush=True)
        return None


def download_and_parse_pdfs(page: Page, filing: Filing) -> None:
    """Downloads up to MAX_PDFS_PER_FILING prioritized PDFs for a Rate/Rule
    filing, parses each (unless >15MB), merges parsed fields into `filing`.
    """
    attachments = list_attachments(page)
    selected = prioritize_attachments(attachments)
    if not selected:
        filing.pdf_parse_status = "no_pdfs_attached"
        return

    dest_dir = PDF_DIR / filing.state / filing.filing_id
    dest_dir.mkdir(parents=True, exist_ok=True)

    fields_found_union: set[str] = set()
    parse_attempted = 0

    for att in selected:
        local = download_attachment(page, att, dest_dir)
        if local is None:
            continue
        filing.pdfs.append(
            AttachedPdf(
                category=att["category"],
                display_name=att["display_name"],
                url="",
                local_path=str(local),
            )
        )
        size_mb = local.stat().st_size / (1024 * 1024)
        if size_mb > LARGE_PDF_SKIP_PARSE_MB:
            continue
        parse_attempted += 1
        try:
            result, parse_status = parse_rate_effect_pdf(
                local, filing.serff_tracking_number
            )
        except Exception as e:
            print(f"    [parse fail] {local.name}: {e}", flush=True)
            continue
        if parse_status == "timeout":
            print(f"    [pdf timeout] {local.name}", flush=True)
        for field, value in result.items():
            if getattr(filing, field, None) is None:
                setattr(filing, field, value)
                fields_found_union.add(field)

    filing.pdf_parse_fields_found = sorted(fields_found_union)
    filing.compute_premium_change()

    if not filing.pdfs:
        filing.pdf_parse_status = "no_pdfs_downloaded"
    elif fields_found_union:
        filing.pdf_parse_status = "parsed"
    elif parse_attempted == 0:
        filing.pdf_parse_status = "all_pdfs_too_large_to_parse"
    else:
        filing.pdf_parse_status = "no_fields_matched"


def enrich_filing(page: Page, filing: Filing, *, download_pdfs: bool) -> bool:
    """Click into the filing's detail page, extract fields (and PDFs if
    Rate/Rule + enabled), then navigate back to results.
    """
    # Make sure the row is visible in the current pagination view.
    for _ in range(2):
        if page.locator(f'tr[data-rk="{filing.filing_id}"]').count():
            break
        _set_rows_per_page_100(page)

    if not page.locator(f'tr[data-rk="{filing.filing_id}"]').count():
        print(f"  [skip] {filing.serff_tracking_number}: row not found in results", flush=True)
        return False

    if not _click_row_to_detail(page, filing.filing_id):
        print(f"  [skip] {filing.serff_tracking_number}: could not open detail", flush=True)
        return False

    try:
        scrape_detail_fields(page, filing)
        if download_pdfs and filing.filing_type == "Rate/Rule":
            download_and_parse_pdfs(page, filing)
    except Exception:
        traceback.print_exc()
    finally:
        _back_to_results(page)
    return True


def _enrich_group(
    browser: Browser,
    state: str,
    company: str,
    filings: list[Filing],
    *,
    download_pdfs: bool,
    delay_s: float,
) -> None:
    ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=True)
    page = ctx.new_page()
    try:
        print(f"[detail] {state} / {company}: opening search for {len(filings)} filing(s)", flush=True)
        if not _submit_search(page, state, company):
            print(f"  ! search failed for {company}, skipping group", flush=True)
            return
        _set_rows_per_page_100(page)

        for i, f in enumerate(filings, 1):
            enrich_filing(page, f, download_pdfs=download_pdfs)
            if i % 5 == 0:
                print(f"    {i}/{len(filings)} enriched", flush=True)
            time.sleep(delay_s)
    finally:
        ctx.close()


def enrich_filings(
    filings: Iterable[Filing],
    *,
    only_tracking_numbers: Optional[Iterable[str]] = None,
    download_pdfs: bool = True,
    headless: bool = HEADLESS,
    delay_s: float = REQUEST_DELAY,
) -> list[Filing]:
    """Drive detail-page enrichment across all filings, grouped by
    (state, target_company) so we reuse one browser context per group.

    If `only_tracking_numbers` is provided, only filings with those SERFF
    tracking numbers are processed (others pass through untouched).
    """
    all_filings = list(filings)
    targets: Optional[set[str]] = set(only_tracking_numbers) if only_tracking_numbers else None

    groups: dict[tuple[str, str], list[Filing]] = defaultdict(list)
    for f in all_filings:
        if targets is not None and f.serff_tracking_number not in targets:
            continue
        groups[(f.state, f.target_company)].append(f)

    if not groups:
        print("[detail] nothing to enrich", flush=True)
        return all_filings

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            for (state, company), group in groups.items():
                _enrich_group(
                    browser, state, company, group,
                    download_pdfs=download_pdfs, delay_s=delay_s,
                )
        finally:
            browser.close()
    return all_filings


__all__ = [
    "scrape_detail_fields",
    "list_attachments",
    "prioritize_attachments",
    "download_attachment",
    "download_and_parse_pdfs",
    "enrich_filing",
    "enrich_filings",
    "MAX_PDFS_PER_FILING",
]
