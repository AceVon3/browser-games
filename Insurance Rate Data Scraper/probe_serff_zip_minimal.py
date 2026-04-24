"""Test: Download Zip File with NO checkboxes selected.
Hypothesis: returns tiny zip with just the system PDF + usage agreement.
"""
from __future__ import annotations
import sys, zipfile
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")
from src.config import USER_AGENT, HEADLESS
from src.search import _submit_search, _set_rows_per_page_100, _click_row_to_detail

STATE = "ID"; COMPANY = "state farm"; FILING_ID = "134676753"
OUT = Path(__file__).parent / "probe_serff_zip_minimal"
OUT.mkdir(exist_ok=True)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=True)
        page = ctx.new_page()
        if not _submit_search(page, STATE, COMPANY): return
        _set_rows_per_page_100(page)
        for _ in range(6):
            if page.locator(f'tr[data-rk="{FILING_ID}"]').count(): break
            nxt = page.locator(".ui-paginator-next").first
            if not nxt.count(): break
            nxt.click(); page.wait_for_load_state("networkidle", timeout=15000)
        if not _click_row_to_detail(page, FILING_ID): return
        page.wait_for_load_state("networkidle", timeout=20000)
        page.wait_for_timeout(800)

        # Verify nothing is selected
        n_checked = page.evaluate("$('input:checkbox[id$=\"_selected\"]:checked').length")
        print(f"  checkboxes pre-click: checked={n_checked}")

        print("  clicking Download Zip File with no selections...")
        try:
            with page.expect_download(timeout=60000) as dl_info:
                page.evaluate("document.getElementById('summaryForm:downloadLink').click();")
            dl = dl_info.value
            zp = OUT / (dl.suggested_filename or "filing_minimal.zip")
            dl.save_as(str(zp))
            print(f"  saved -> {zp.name}  ({zp.stat().st_size / 1024:.1f} KB)")
        except Exception as e:
            print(f"  ! failed: {e}"); return

        with zipfile.ZipFile(zp) as zf:
            names = zf.namelist()
            print(f"\n  zip contains {len(names)} entries:")
            for n in names:
                info = zf.getinfo(n)
                print(f"    {info.file_size:>10d}  {n}")
        browser.close()


if __name__ == "__main__":
    main()
