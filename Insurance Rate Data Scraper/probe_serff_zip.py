"""Click 'Download Zip File' on SFMA-134676753 detail page and inspect contents.
Save the zip to probe_serff_zip/ and list every file inside.
"""
from __future__ import annotations
import sys, zipfile
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")
from src.config import USER_AGENT, HEADLESS
from src.search import _submit_search, _set_rows_per_page_100, _click_row_to_detail

STATE = "ID"
COMPANY = "state farm"
FILING_ID = "134676753"
OUT_DIR = Path(__file__).parent / "probe_serff_zip"
OUT_DIR.mkdir(exist_ok=True)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=True)
        page = ctx.new_page()

        print(f"[1/5] searching state={STATE} company={COMPANY!r}")
        if not _submit_search(page, STATE, COMPANY):
            print("  ! search failed"); return
        _set_rows_per_page_100(page)

        for _ in range(6):
            if page.locator(f'tr[data-rk="{FILING_ID}"]').count(): break
            nxt = page.locator(".ui-paginator-next").first
            if not nxt.count(): break
            nxt.click(); page.wait_for_load_state("networkidle", timeout=15000)

        print(f"[2/5] opening detail for {FILING_ID}")
        if not _click_row_to_detail(page, FILING_ID):
            print("  ! row click failed"); return
        page.wait_for_load_state("networkidle", timeout=20000)
        page.wait_for_timeout(1000)

        print("[3/5] selecting all attachments in all panels (so the zip includes everything)")
        # Click 'Select All' in each panel that has it. We also need to see if zip
        # works WITHOUT selecting anything (which would mean SERFF auto-bundles the
        # system Filing Summary PDF only).
        for sel_all_id in [
            "formAttachmentSelectAllButton",
            "rateRuleAttachmentSelectAllButton",
            "supportingDocumentAttachmentSelectAllButton",
            "correspondenceAttachmentSelectAllButton",
        ]:
            try:
                page.evaluate(f"document.getElementById('{sel_all_id}')?.click()")
            except Exception:
                pass
        page.wait_for_timeout(500)

        print("[4/5] clicking 'Download Zip File'")
        try:
            with page.expect_download(timeout=120000) as dl_info:
                page.evaluate("document.getElementById('summaryForm:downloadLink').click();")
            dl = dl_info.value
            zip_path = OUT_DIR / (dl.suggested_filename or "filing_bundle.zip")
            dl.save_as(str(zip_path))
            print(f"  saved -> {zip_path}  ({zip_path.stat().st_size / 1024:.1f} KB)")
        except Exception as e:
            print(f"  ! download failed: {e}")
            browser.close(); return

        print("[5/5] unzipping and listing contents")
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            print(f"  zip contains {len(names)} entries:")
            for n in names:
                info = zf.getinfo(n)
                print(f"    {info.file_size:>10d}  {n}")
            # Extract all to a subdir
            extract_dir = OUT_DIR / "unzipped"
            extract_dir.mkdir(exist_ok=True)
            zf.extractall(extract_dir)
        print(f"\n  extracted to {extract_dir}")
        browser.close()


if __name__ == "__main__":
    main()
