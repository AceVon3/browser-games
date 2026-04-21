"""Deeper probe: enumerate tabs, sections, tables on a SERFF detail page.

The first probe (probe_detail_labels.py) only dumped top-level <label> elements
on the General Information panel. SERFF detail pages typically expose Rate Data
/ Rate Information / Companies & Contacts as separate tabs or accordion panels,
where per-company "Effective Date Requested (New/Renewal)" lives. This probe
walks every tab/panel and prints any table containing the word "effective".
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


_TAB_DUMP_JS = r"""() => {
    const out = {tabs: [], links: [], headings: [], tables: []};
    // PrimeFaces tabs: <li role="tab"> with text
    for (const t of document.querySelectorAll('li[role="tab"], a[role="tab"], .ui-tabs-anchor')) {
        const txt = (t.textContent || '').trim();
        if (txt) out.tabs.push(txt);
    }
    // Any anchor whose visible text suggests rate/data/companies
    for (const a of document.querySelectorAll('a')) {
        const txt = (a.textContent || '').trim();
        if (!txt) continue;
        if (/rate\s*data|rate\s*info|compan(?:y|ies)\s*&|effective/i.test(txt)) {
            out.links.push({text: txt, href: a.getAttribute('href') || '', id: a.id || ''});
        }
    }
    // All h1/h2/h3/h4 headings
    for (const h of document.querySelectorAll('h1,h2,h3,h4,h5,legend')) {
        const txt = (h.textContent || '').trim();
        if (txt) out.headings.push({tag: h.tagName, text: txt});
    }
    // All tables — capture header row + first 3 rows + flag if any cell mentions effective
    for (const tbl of document.querySelectorAll('table')) {
        const headerCells = Array.from(tbl.querySelectorAll('thead th, thead td')).map(c => (c.textContent || '').trim());
        const bodyRows = Array.from(tbl.querySelectorAll('tbody tr')).slice(0, 5).map(r =>
            Array.from(r.querySelectorAll('td,th')).map(c => (c.textContent || '').trim())
        );
        const allText = (tbl.textContent || '').toLowerCase();
        out.tables.push({
            id: tbl.id || '',
            mentionsEffective: /effective/.test(allText),
            mentionsPremium: /average\s+premium|avg\s+premium/.test(allText),
            mentionsRate: /rate\s+data|rate\s+information|written\s+premium/.test(allText),
            headerCells,
            bodyRows,
        });
    }
    return out;
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

        # Wait a beat — JSF panels sometimes lazy-load
        page.wait_for_load_state("networkidle", timeout=15000)

        data = page.evaluate(_TAB_DUMP_JS)

        print("=== TABS ===")
        for t in data["tabs"]:
            print(f"  {t!r}")
        if not data["tabs"]:
            print("  (none found)")

        print("\n=== LINKS mentioning rate/data/companies/effective ===")
        for l in data["links"]:
            print(f"  {l['text']!r}  href={l['href']!r}  id={l['id']!r}")
        if not data["links"]:
            print("  (none found)")

        print("\n=== HEADINGS ===")
        for h in data["headings"]:
            print(f"  <{h['tag']}> {h['text']!r}")

        print(f"\n=== TABLES ({len(data['tables'])} total) ===")
        for i, tbl in enumerate(data["tables"]):
            flags = []
            if tbl["mentionsEffective"]: flags.append("EFFECTIVE")
            if tbl["mentionsPremium"]: flags.append("PREMIUM")
            if tbl["mentionsRate"]: flags.append("RATE")
            flag_str = f" [{','.join(flags)}]" if flags else ""
            print(f"\n--- table {i} id={tbl['id']!r}{flag_str} ---")
            print(f"  header: {tbl['headerCells']}")
            for j, row in enumerate(tbl["bodyRows"]):
                print(f"  row {j}: {row}")

        # Also dump full body text of any element containing "effective date"
        # (case insensitive) so we can see if it's hidden in some non-table
        # structure we haven't matched.
        print("\n=== ANY ELEMENT TEXT containing 'effective date' (max 300 chars) ===")
        eff_texts = page.evaluate(r"""() => {
            const out = [];
            const seen = new Set();
            for (const el of document.querySelectorAll('*')) {
                if (el.children.length) continue;
                const t = (el.textContent || '').trim();
                if (!t) continue;
                if (!/effective\s+date/i.test(t)) continue;
                if (t.length > 300) continue;
                if (seen.has(t)) continue;
                seen.add(t);
                out.push({tag: el.tagName, text: t});
            }
            return out;
        }""")
        for row in eff_texts:
            print(f"  <{row['tag']}> {row['text']!r}")
        if not eff_texts:
            print("  (none found)")

        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
