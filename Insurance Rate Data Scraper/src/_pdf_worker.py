"""Subprocess worker that extracts text from a single PDF.

Invoked by `extract_pdf_text_with_timeout` in src/utils.py via subprocess.run
so that pdfplumber/pypdf hangs can be killed with a hard timeout. Not meant
for direct use.

Usage:
    python -m src._pdf_worker <path/to/file.pdf>
Writes UTF-8 PDF text to stdout. Empty stdout means no extractable text.
"""
from __future__ import annotations

import sys
from pathlib import Path


def _extract(pdf_path: Path) -> str:
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            chunks = []
            for page in pdf.pages:
                chunks.append(page.extract_text() or "")
            text = "\n".join(chunks)
    except Exception:
        text = ""
    if not text.strip():
        try:
            from pypdf import PdfReader
            r = PdfReader(str(pdf_path))
            text = "\n".join((p.extract_text() or "") for p in r.pages)
        except Exception:
            text = ""
    return text


def main() -> int:
    if len(sys.argv) < 2:
        return 2
    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        return 3
    text = _extract(pdf_path)
    sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
