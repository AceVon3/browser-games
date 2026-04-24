"""Inspect the 2 OR filings' full Disposition/Rate Information section."""
import pdfplumber
from pathlib import Path

for fid, tracking in [("134491344", "SFMA-134491344"), ("134619497", "SFMA-134619497")]:
    pdf_path = Path(f"output/pdfs/OR/{fid}/filing_summary.pdf")
    print(f"\n{'='*60}\n{tracking}\n{'='*60}")
    with pdfplumber.open(str(pdf_path)) as pdf:
        print(f"pages: {len(pdf.pages)}")
        for p_i, pg in enumerate(pdf.pages):
            text = pg.extract_text() or ""
            if "Company Rate Information" in text or "Rate Information" in text or "Overall" in text:
                print(f"\n--- page {p_i+1} ---")
                print(text)
