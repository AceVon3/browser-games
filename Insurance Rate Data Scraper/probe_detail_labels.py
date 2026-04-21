"""Dump every <label> (and every <dt>) on one filing's SERFF detail page
so we can see whether effective date is exposed anywhere on the HTML.
"""
from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import HEADLESS, USER_AGENT
from src.search import _click_row_to_detail, _set_rows_per_page_100, _submit_search


TARGET_STATE = "WA"
TARGET_COMPANY = "Allstate"
TARGET_FILING_ID = "134517504"  # ALSE-134517504, +11.90% Condo HO


_PROBE_JS = r"""() => {
    const all = [];
    for (const l of document.querySelectorAll('label')) {
        const key = (l.textContent || '').trim();
        if (!key) continue;
        const row = l.closest('.row');
        const val = row ? row.querySelector('div') : null;
        all.push({tag: 'label', key, value: val ? (val.textContent || '').trim() : ''});
    }
    for (const dt of document.querySelectorAll('dt')) {
        const key = (dt.textContent || '').trim();
        const dd = dt.nextElementSibling;
        all.push({tag: 'dt', key, value: dd ? (dd.textContent || '').trim() : ''});
    }
    // Also scan any element whose text mentions "effective"
    const eff = [];
    for (const el of document.querySelectorAll('*')) {
        const t = (el.textContent || '').trim();
        if (t && t.length < 150 && /effective/i.test(t) && el.children.length === 0) {
            eff.push({tag: el.tagName, text: t});
        }
    }
    return {all, eff};
}"""


def main() -> int:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(user_agent=USER_AGENT)
        page = ctx.new_page()
        if not _submit_search(page, TARGET_STATE, TARGET_COMPANY):
            print("search failed")
            return 1
        _set_rows_per_page_100(page)
        if not _click_row_to_detail(page, TARGET_FILING_ID):
            print("could not open detail")
            return 1
        data = page.evaluate(_PROBE_JS)
        print("=== ALL LABELS & DTs ===")
        for row in data["all"]:
            print(f"  [{row['tag']}] {row['key']!r}: {row['value']!r}")
        print("\n=== LEAF ELEMENTS MENTIONING 'effective' ===")
        for row in data["eff"]:
            print(f"  <{row['tag']}> {row['text']!r}")
        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
