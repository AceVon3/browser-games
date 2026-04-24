"""Probe SERFF detail HTML for Disposition Page Data.

Target: SFMA-134676753 (ID, State Farm Auto, filing_id=134676753).
AM Best shows:
  - State Farm Mutual Auto: -9.700% rate impact, -$25,716,996, 360,274 policyholders
  - State Farm Fire&Cas:    -2.100% rate impact, -$554,469,    20,679  policyholders

Goal: find where these values live on the SERFF filing detail page.
"""
from __future__ import annotations
import sys, re
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")
from src.config import USER_AGENT, HEADLESS
from src.search import _submit_search, _set_rows_per_page_100, _click_row_to_detail

STATE = "ID"
COMPANY = "state farm"      # match both subsidiaries
FILING_ID = "134676753"
OUT_HTML = Path(__file__).parent / "probe_serff_disposition.html"
OUT_TXT  = Path(__file__).parent / "probe_serff_disposition.txt"


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=False)
        page = ctx.new_page()
        print(f"[1/5] Submitting search: state={STATE} company={COMPANY!r}")
        if not _submit_search(page, STATE, COMPANY):
            print("  ! search submit failed"); return
        _set_rows_per_page_100(page)
        print(f"[2/5] Clicking row for filing_id={FILING_ID}")
        # search for the row; may need to paginate if not on page 1
        for attempt in range(10):
            if page.locator(f'tr[data-rk="{FILING_ID}"]').count():
                break
            # try next page
            nxt = page.locator(".ui-paginator-next").first
            if not nxt.count(): break
            nxt.click()
            page.wait_for_load_state("networkidle", timeout=15000)
        else:
            print("  ! could not find row"); return
        if not _click_row_to_detail(page, FILING_ID):
            print("  ! row click failed"); return
        print(f"[3/5] Detail page loaded: {page.url}")
        page.wait_for_load_state("networkidle", timeout=20000)

        # Wait a beat for any lazy panels
        page.wait_for_timeout(1500)

        print("[4/5] Dumping full HTML + visible text")
        html = page.content()
        OUT_HTML.write_text(html, encoding="utf-8")
        text = page.evaluate("() => document.body.innerText")
        OUT_TXT.write_text(text, encoding="utf-8")

        print(f"[5/5] Searching for target values in the page:")
        for needle in ["360,274", "25,716,996", "554,469", "20,679",
                      "Rate Information", "Policy Holders", "Written Premium",
                      "Overall %", "Indicated Change", "Rate Impact",
                      "Disposition", "Company Rate Information"]:
            n = text.count(needle)
            html_n = html.count(needle)
            if n or html_n:
                print(f"  {needle!r:32s}  text={n:3d}  html={html_n:3d}")

        # Also dump all <h2>/<h3> section headers to see page structure
        sections = page.evaluate(
            """() => Array.from(document.querySelectorAll('h1,h2,h3'))
                .map(h => ({ tag: h.tagName, text: (h.textContent||'').trim().slice(0,120) }))
                .filter(x => x.text)"""
        )
        print("\n  Section headers on page:")
        for s in sections:
            print(f"    [{s['tag']}] {s['text']}")

        # Look for tabs / buttons that might reveal more data
        tabs = page.evaluate(
            """() => Array.from(document.querySelectorAll('a[role=tab], button, .ui-tabs-nav li, .nav-tabs li'))
                .map(e => (e.textContent||'').trim().slice(0,80))
                .filter(t => t && t.length < 80)"""
        )
        uniq_tabs = list(dict.fromkeys(t for t in tabs if t and "\n" not in t))[:40]
        print("\n  Tabs / buttons on page:")
        for t in uniq_tabs:
            print(f"    {t}")

        browser.close()
    print(f"\nHTML: {OUT_HTML}")
    print(f"TXT:  {OUT_TXT}")


if __name__ == "__main__":
    main()
