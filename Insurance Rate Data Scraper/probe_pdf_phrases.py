"""Sample PDF text around 'overall', 'rate change', 'base rate', 'indicat'
phrases for 3 filings to see what carrier prose actually looks like."""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR
from src.utils import extract_pdf_text_with_timeout

PDF_ROOT = OUTPUT_DIR / "pdfs"

SAMPLES = [
    ("WA", "134517504", "ALSE +11.9%"),
    ("WA", "134538132", "ALSE +26.6%"),
    ("ID", "134416811", "ALSE 0%"),
    ("CO", "134551958", "LBPM +7.2%"),
    ("CO", "134702926", "SFMA +0.3%"),
]

KEYWORDS = [
    r"overall",
    r"base\s+rate",
    r"indicat",
    r"rate\s+change",
    r"rate\s+impact",
    r"premium\s+income",
    r"revision",
    r"weighted",
]

for state, fid, label in SAMPLES:
    pdf_dir = PDF_ROOT / state / fid
    if not pdf_dir.exists():
        print(f"\n=== {label} {state}/{fid}: DIR NOT FOUND ===\n")
        continue
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    print(f"\n=== {label} ({state}/{fid}) — {len(pdfs)} PDFs ===")
    for pdf in pdfs[:3]:
        size_mb = pdf.stat().st_size / (1024 * 1024)
        if size_mb > 15:
            print(f"  -- skip {pdf.name} ({size_mb:.1f} MB)")
            continue
        text, status = extract_pdf_text_with_timeout(pdf, timeout_s=45)
        if status != "ok" or not text:
            print(f"  -- {pdf.name}: status={status}")
            continue
        flat = re.sub(r"\s+", " ", text)
        print(f"\n  --- {pdf.name} ({len(flat)} chars) ---")
        for kw in KEYWORDS:
            for m in re.finditer(kw, flat, re.IGNORECASE):
                ctx_start = max(0, m.start() - 60)
                ctx_end = min(len(flat), m.end() + 100)
                snip = flat[ctx_start:ctx_end].strip()
                print(f"    [{kw}] ...{snip}...")
                break  # one match per keyword per pdf
