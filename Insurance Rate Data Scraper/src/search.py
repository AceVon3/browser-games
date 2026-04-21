"""SERFF Filing Access search-only scraper.

Runs a PrimeFaces/JSF search for one (state, target_company) pair, iterates
paginated results, and returns `Filing` objects populated with the columns
visible on the results table. No detail-page navigation, no PDF download.

Detail-page scraping and PDF download are added in Step 5.
"""
from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Iterable, Optional

from playwright.sync_api import Page, Browser, sync_playwright

from .config import (
    DATE_FROM,
    DATE_TO,
    HEADLESS,
    REQUEST_DELAY,
    SERFF_BASE,
    SERFF_DETAIL_URL,
    SERFF_HOME_URL,
    TARGET_COMPANIES,
    USER_AGENT,
)
from .models import Filing


def _byid(page: Page, jsf_id: str):
    return page.locator(f'[id="{jsf_id}"]')


def _set_primefaces_select(page: Page, panel_id: str, option_label_regex: str) -> bool:
    label = _byid(page, f"{panel_id}_label")
    if not label.count():
        return False
    label.first.click()
    items = _byid(page, f"{panel_id}_items")
    items.wait_for(state="visible", timeout=5000)
    opt = items.locator("li", has_text=re.compile(option_label_regex, re.I))
    if not opt.count():
        return False
    opt.first.click()
    page.wait_for_load_state("networkidle", timeout=15000)
    return True


def _fill_and_blur(page: Page, jsf_id: str, value: str) -> None:
    loc = _byid(page, jsf_id).first
    loc.click()
    loc.fill(value)
    loc.press("Tab")


def _wait_for_results(page: Page) -> None:
    page.wait_for_load_state("domcontentloaded", timeout=30000)
    page.wait_for_function(
        """() => {
            const hasRows = document.querySelector('tr[data-rk]');
            const hasNoRec = Array.from(document.querySelectorAll('span, div, td, h5'))
                .some(e => /no\\s+records|no\\s+filings?\\s+(were|matched|found)|0\\s+filing/i.test(e.textContent || ''));
            return hasRows || hasNoRec;
        }""",
        timeout=30000,
    )


def _submit_search(page: Page, state: str, company: str) -> bool:
    page.goto(SERFF_HOME_URL.format(state=state), wait_until="domcontentloaded", timeout=30000)
    page.get_by_role("link", name=re.compile(r"begin\s*search", re.I)).first.click()
    page.wait_for_load_state("domcontentloaded", timeout=30000)
    # ToU accept is required once per session.
    try:
        page.get_by_role("button", name=re.compile(r"^accept$", re.I)).first.click(timeout=5000)
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass

    if not _set_primefaces_select(page, "simpleSearch:businessType", r"property\s*&\s*casualty"):
        return False

    _fill_and_blur(page, "simpleSearch:companyName", company)
    _fill_and_blur(page, "simpleSearch:submissionStartDate_input", DATE_FROM)
    _fill_and_blur(page, "simpleSearch:submissionEndDate_input", DATE_TO)

    _byid(page, "simpleSearch:saveBtn").first.click()
    try:
        _wait_for_results(page)
    except Exception:
        return False
    return True


def _set_rows_per_page_100(page: Page) -> None:
    """Reduce pagination by selecting the 100 rows-per-page option if present.

    PrimeFaces re-renders the table via AJAX (`persistRows`) after the change
    event — we must wait for networkidle, not just for the existing rows.
    """
    try:
        sel = page.locator("select.ui-paginator-rpp-options").first
        if not sel.count():
            return
        sel.select_option("100")
        page.wait_for_load_state("networkidle", timeout=15000)
        time.sleep(1.0)
    except Exception:
        pass


def _extract_rows(page: Page, state: str, company: str) -> list[Filing]:
    """Read the visible results table rows and map each to a Filing."""
    data = page.evaluate(
        """() => {
            const headers = Array.from(document.querySelectorAll('thead th .ui-column-title'))
                .map(h => (h.textContent || '').trim());
            const rows = Array.from(document.querySelectorAll('tbody#j_idt25\\\\:filingTable_data > tr[data-rk], tr[data-rk]'));
            const out = [];
            for (const r of rows) {
                const cells = Array.from(r.querySelectorAll('td')).map(td => (td.textContent || '').trim());
                out.push({ data_rk: r.getAttribute('data-rk'), cells });
            }
            return { headers, rows: out };
        }"""
    )
    headers: list[str] = [h for h in data.get("headers", []) if h]
    rows = data.get("rows", [])

    def col_index(name_regex: str) -> int | None:
        for i, h in enumerate(headers):
            if re.search(name_regex, h, re.I):
                return i
        return None

    idx_company = col_index(r"company\s*name")
    idx_naic = col_index(r"naic")
    idx_product = col_index(r"product")
    idx_subtoi = col_index(r"sub\s*type")
    idx_ftype = col_index(r"filing\s*type")
    idx_fstatus = col_index(r"filing\s*status")
    idx_serff = col_index(r"serff\s*tracking")

    filings: list[Filing] = []
    for r in rows:
        cells: list[str] = r["cells"]
        filing_id = r["data_rk"]
        if not filing_id or not cells:
            continue
        # The first cell contains a toggler glyph prefix; cells already stripped.
        def cell(i: int | None) -> str | None:
            if i is None or i >= len(cells):
                return None
            v = cells[i].strip()
            return v or None

        serff_tid = cell(idx_serff) or f"{state}-{filing_id}"
        naic_raw = cell(idx_naic) or ""
        naic_codes = [c.strip() for c in re.split(r"[,\s/;]+", naic_raw) if c.strip()]

        filings.append(
            Filing(
                state=state,
                serff_tracking_number=serff_tid,
                filing_id=filing_id,
                company_name=cell(idx_company) or "",
                target_company=company,
                naic_codes=naic_codes,
                product_name=cell(idx_product),
                sub_type_of_insurance=cell(idx_subtoi),
                filing_type=cell(idx_ftype),
                filing_status=cell(idx_fstatus),
                detail_url=SERFF_DETAIL_URL.format(filing_id=filing_id),
            )
        )
    return filings


def _has_next_page(page: Page) -> bool:
    nxt = page.locator(".ui-paginator-top .ui-paginator-next").first
    if not nxt.count():
        return False
    cls = (nxt.get_attribute("class") or "")
    return "ui-state-disabled" not in cls


def _click_next_page(page: Page) -> bool:
    nxt = page.locator(".ui-paginator-top .ui-paginator-next").first
    if not nxt.count():
        return False
    nxt.click()
    try:
        _wait_for_results(page)
        return True
    except Exception:
        return False


def _parse_date(s: str) -> Optional[date]:
    """Accept SERFF's m/d/yy or m/d/yyyy formats."""
    s = (s or "").strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _click_row_to_detail(page: Page, filing_id: str) -> bool:
    """Click the SERFF Tracking cell on a results row to load its detail page.

    Direct URL navigation (filingSummary.xhtml?filingId=...) redirects to
    /sfa/500.xhtml because the detail page requires the JSF ViewState session
    produced by a row click on the results page.
    """
    row = page.locator(f'tr[data-rk="{filing_id}"]').first
    if not row.count():
        return False
    cells = row.locator("td")
    n = cells.count()
    if n < 2:
        return False
    url_before = page.url
    cells.nth(n - 1).click()
    try:
        page.wait_for_url(lambda u: u != url_before and "filingSummary" in u, timeout=15000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        return True
    except Exception:
        return False


def _extract_submission_date_from_detail(page: Page) -> Optional[date]:
    """Pull the 'Submission Date: <date>' value from the filing summary page."""
    txt = page.evaluate(
        """() => {
            const labels = Array.from(document.querySelectorAll('label'));
            for (const l of labels) {
                if (/^\\s*submission\\s+date\\s*:\\s*$/i.test(l.textContent || '')) {
                    const row = l.closest('.row');
                    if (!row) continue;
                    const val = row.querySelector('div');
                    return val ? (val.textContent || '').trim() : null;
                }
            }
            return null;
        }"""
    )
    return _parse_date(txt) if txt else None


def _back_to_results(page: Page) -> bool:
    try:
        page.go_back(wait_until="domcontentloaded", timeout=15000)
        page.wait_for_selector('tr[data-rk]', timeout=10000)
        return True
    except Exception:
        return False


def search_company(
    browser: Browser,
    state: str,
    company: str,
    *,
    fetch_submission_dates: bool = True,
    delay_s: float = REQUEST_DELAY,
) -> list[Filing]:
    """Run one search; return a Filing per results-table row across all pages.

    When `fetch_submission_dates` is True, we make a lightweight detail-page
    fetch per filing to populate `submission_date` (no PDF parsing here —
    that's Step 5). Reuses the search's browser context for session state.
    """
    ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=False)
    page = ctx.new_page()
    filings: list[Filing] = []
    try:
        if not _submit_search(page, state, company):
            return filings
        _set_rows_per_page_100(page)

        seen: set[str] = set()
        while True:
            batch = _extract_rows(page, state, company)
            new = [f for f in batch if f.filing_id not in seen]
            for f in new:
                seen.add(f.filing_id)
            filings.extend(new)
            if not _has_next_page(page):
                break
            if not _click_next_page(page):
                break

        if fetch_submission_dates and filings:
            print(f"    fetching submission dates for {len(filings)} filings ...", flush=True)
            for i, f in enumerate(filings, 1):
                try:
                    # If the row isn't in the current view (pagination reverted
                    # after a go_back), re-set RPP=100 to bring all rows back.
                    if not page.locator(f'tr[data-rk="{f.filing_id}"]').count():
                        _set_rows_per_page_100(page)
                    if not _click_row_to_detail(page, f.filing_id):
                        print(f"      [warn] {f.serff_tracking_number}: could not open detail", flush=True)
                        continue
                    f.submission_date = _extract_submission_date_from_detail(page)
                    _back_to_results(page)
                except Exception as e:
                    print(f"      [warn] {f.serff_tracking_number}: {e}", flush=True)
                time.sleep(delay_s)
                if i % 10 == 0:
                    print(f"      {i}/{len(filings)} ...", flush=True)
    finally:
        ctx.close()
    return filings


def search_all(
    state_company_pairs: Iterable[tuple[str, str]],
    *,
    headless: bool = HEADLESS,
    delay_s: float = REQUEST_DELAY,
) -> list[Filing]:
    """Run search_company for each (state, company) pair with polite delays."""
    all_filings: list[Filing] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            for state, company in state_company_pairs:
                print(f"[search] {state} / {company} ...", flush=True)
                results = search_company(browser, state, company)
                print(f"  -> {len(results)} rows", flush=True)
                all_filings.extend(results)
                time.sleep(delay_s)
        finally:
            browser.close()
    return all_filings


__all__ = ["search_company", "search_all", "TARGET_COMPANIES", "SERFF_BASE"]
