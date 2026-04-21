"""Dump every occurrence of 'rate' / '%' / 'impact' in one specific PDF
with 100-char context. Lets us see what the rate memo actually says."""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8")

from src.config import OUTPUT_DIR
from src.utils import extract_pdf_text_with_timeout

# Big filings I want to confirm:
TARGETS = [
    ("WA", "134422784", "@WA MH Filing Packet.pdf"),  # SFMA +23.9%
    ("CO", "134551958", "CO_PL_AO_Exhibits_07-09-2025.pdf"),  # LBPM +7.2%
    ("CO", "134702926", "CO Filing Packet - SFM.pdf"),  # SFMA +0.3%
]

for state, fid, fname in TARGETS:
    pdf = OUTPUT_DIR / "pdfs" / state / fid / fname
    if not pdf.exists():
        print(f"\n=== {state}/{fid}/{fname}: NOT FOUND ===")
        continue
    text, status = extract_pdf_text_with_timeout(pdf, timeout_s=90)
    print(f"\n=== {state}/{fid}/{fname} ({pdf.stat().st_size//1024} KB, status={status}) ===")
    if not text:
        continue
    # Normalize
    flat = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    flat = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", flat)
    flat = re.sub(r"(\d)([a-zA-Z])", r"\1 \2", flat)
    flat = re.sub(r"\s+", " ", flat)
    print(f"  text length after normalize: {len(flat)} chars")
    # Find every percentage between 0.01% and 100% with surrounding context
    pct_re = re.compile(r"([+\-]?\d+(?:\.\d+)?\s*%)")
    seen = set()
    for m in pct_re.finditer(flat):
        ctx_start = max(0, m.start() - 80)
        ctx_end = min(len(flat), m.end() + 40)
        snip = flat[ctx_start:ctx_end].strip()
        # Only show if the snippet mentions rate/impact/change/indicated/base
        if not re.search(r"rate|impact|change|indicated|base|overall|weighted|adoption|propos|request|approv", snip, re.I):
            continue
        key = snip[:80]
        if key in seen:
            continue
        seen.add(key)
        print(f"    [{m.group(1)}] ...{snip}...")
        if len(seen) >= 15:
            print(f"    ... (truncated, more matches exist)")
            break
