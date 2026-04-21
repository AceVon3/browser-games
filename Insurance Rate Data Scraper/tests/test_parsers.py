"""Unit tests for src/utils.py plus a parse-success report against real SERFF PDFs.

Run:
    ./.venv/Scripts/python.exe -m pytest tests/test_parsers.py -v
    ./.venv/Scripts/python.exe tests/test_parsers.py  # prints the success report
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src.utils import (
    extract_pdf_text,
    match_target_company,
    normalize_company_name,
    parse_date,
    parse_int,
    parse_money,
    parse_percent,
    parse_rate_effect_pdf,
)
from src.config import TARGET_COMPANIES

FIXTURES = Path(__file__).parent / "fixtures" / "pdfs"

RATE_EFFECT_FIELDS = [
    "overall_rate_effect",
    "requested_rate_effect",
    "approved_rate_effect",
    "affected_policyholders",
    "written_premium_volume",
    "current_avg_premium",
    "proposed_avg_premium",
    "annual_premium_impact_dollars",
]


# ---------- scalar parsers ----------

def test_parse_date_variants():
    assert parse_date("01/02/2025") == date(2025, 1, 2)
    assert parse_date("1/2/25") == date(2025, 1, 2)
    assert parse_date("2025-01-02") == date(2025, 1, 2)
    assert parse_date("  1/2/2025  ") == date(2025, 1, 2)


def test_parse_date_bad_input():
    assert parse_date(None) is None
    assert parse_date("") is None
    assert parse_date("not a date") is None


def test_parse_money_basic():
    assert parse_money("$1,234.56") == 1234.56
    assert parse_money("1234") == 1234.0
    assert parse_money("$42") == 42.0


def test_parse_money_negative():
    assert parse_money("($500.00)") == -500.0
    assert parse_money("-$25.50") == -25.5


def test_parse_money_bad_input():
    assert parse_money(None) is None
    assert parse_money("") is None
    assert parse_money("no money here") is None


def test_parse_percent_basic():
    assert parse_percent("7.5%") == 7.5
    assert parse_percent("+12.34 %") == 12.34
    assert parse_percent("-0.5%") == -0.5


def test_parse_percent_parens_negative():
    assert parse_percent("(0.5%)") == -0.5
    assert parse_percent("(2.3%)") == -2.3


def test_parse_percent_bad_input():
    assert parse_percent(None) is None
    assert parse_percent("") is None
    assert parse_percent("no percent") is None


def test_parse_int_basic():
    assert parse_int("1,234") == 1234
    assert parse_int("42") == 42
    assert parse_int("  99 ") == 99


def test_parse_int_bad_input():
    assert parse_int(None) is None
    assert parse_int("") is None
    assert parse_int("none") is None


# ---------- company matching ----------

def test_normalize_company_name_strips_suffixes():
    n = normalize_company_name("State Farm Mutual Automobile Insurance Company")
    assert "state farm" in n
    assert "company" not in n
    assert "insurance" not in n
    assert "mutual" not in n


def test_match_target_company_state_farm():
    assert match_target_company(
        "State Farm Fire and Casualty Company", TARGET_COMPANIES
    ) == "State Farm"
    assert match_target_company(
        "State Farm Mutual Automobile Insurance Company", TARGET_COMPANIES
    ) == "State Farm"


def test_match_target_company_geico():
    assert match_target_company(
        "Government Employees Insurance Company", TARGET_COMPANIES
    ) == "GEICO"
    assert match_target_company("GEICO Indemnity Company", TARGET_COMPANIES) == "GEICO"


def test_match_target_company_others():
    assert match_target_company("Progressive Direct Insurance Company", TARGET_COMPANIES) == "Progressive"
    assert match_target_company("Allstate Property and Casualty Insurance Company", TARGET_COMPANIES) == "Allstate"
    assert match_target_company("Travelers Indemnity Company", TARGET_COMPANIES) == "Travelers"
    # Safeco is a Liberty Mutual subsidiary
    assert match_target_company("Safeco Insurance Company of America", TARGET_COMPANIES) == "Liberty Mutual"


def test_match_target_company_no_match():
    assert match_target_company("USAA Casualty Insurance", TARGET_COMPANIES) is None
    assert match_target_company("", TARGET_COMPANIES) is None


# ---------- PDF extraction smoke tests ----------

@pytest.mark.parametrize("pdf_path", sorted(FIXTURES.glob("*.pdf")))
def test_pdf_text_extractable(pdf_path: Path):
    """Every fixture PDF must yield non-empty text."""
    text = extract_pdf_text(pdf_path)
    assert text is not None, f"no text extracted from {pdf_path.name}"
    assert len(text.strip()) > 100, f"suspiciously short text from {pdf_path.name}"


# ---------- synthetic rate-effect parser tests ----------

def test_parse_rate_effect_synthetic(tmp_path: Path):
    """Regex matching on a constructed text blob — no PDF required."""
    # Simulate a flat text dump with the common NAIC-templated phrases.
    from src.utils import PDF_FIELD_PATTERNS
    import re as _re

    fake = (
        "Section 1. Summary of Filing\n"
        "Overall Rate Level Change: 7.5%\n"
        "Requested Rate Change is 8.0%\n"
        "Approved Rate Change: 7.5%\n"
        "Number of Affected Policyholders: 12,345\n"
        "Written Premium Volume of $45,678,900.00\n"
        "Current Average Premium: $1,234.56\n"
        "Proposed Average Premium: $1,325.00\n"
        "Annual Premium Impact: $3,456,789\n"
    )
    # We don't actually need a PDF — call the regex layer directly
    flat = _re.sub(r"\s+", " ", fake)
    found = {}
    for name, pattern, parser in PDF_FIELD_PATTERNS:
        if name in found:
            continue
        m = _re.search(pattern, flat, _re.IGNORECASE)
        if m:
            v = parser(m.group(1))
            if v is not None:
                found[name] = v
    assert found.get("overall_rate_effect") == 7.5
    assert found.get("requested_rate_effect") == 8.0
    assert found.get("approved_rate_effect") == 7.5
    assert found.get("affected_policyholders") == 12345
    assert found.get("written_premium_volume") == 45678900.0
    assert found.get("current_avg_premium") == 1234.56
    assert found.get("proposed_avg_premium") == 1325.0
    assert found.get("annual_premium_impact_dollars") == 3456789.0


# ---------- parse-success report against real PDFs ----------

def _parse_fixture_pdfs() -> list[tuple[str, dict]]:
    """Return [(pdf_name, parsed_fields_dict)] for every fixture PDF."""
    out = []
    for pdf in sorted(FIXTURES.glob("*.pdf")):
        parsed, _status = parse_rate_effect_pdf(pdf, tracking_number=pdf.stem)
        out.append((pdf.name, parsed))
    return out


def test_fixture_parse_report(capsys):
    """Not a strict assertion — prints a per-file parse report for the user.

    Fails only if NO fields parse from ANY fixture, which would indicate the
    regex patterns are completely broken.
    """
    results = _parse_fixture_pdfs()
    assert results, "no fixture PDFs found — did fetch_fixtures.py run?"

    lines = ["", "=== Fixture PDF parse report ===", ""]
    total_files = len(results)
    files_with_any = 0
    field_hit_counts: dict[str, int] = {f: 0 for f in RATE_EFFECT_FIELDS}

    for name, parsed in results:
        lines.append(f"[{name}]")
        if parsed:
            files_with_any += 1
            for field, value in parsed.items():
                lines.append(f"    {field}: {value}")
                field_hit_counts[field] = field_hit_counts.get(field, 0) + 1
        else:
            lines.append("    (no rate-effect fields found)")
        lines.append("")

    lines.append(f"Files with any field parsed: {files_with_any}/{total_files}")
    lines.append("Per-field hit counts across fixtures:")
    for field in RATE_EFFECT_FIELDS:
        lines.append(f"    {field}: {field_hit_counts.get(field, 0)}/{total_files}")
    report = "\n".join(lines)

    # Always print (use -s to see) and also attach to captured output.
    print(report)
    with capsys.disabled():
        print(report)

    assert files_with_any > 0, "parser extracted ZERO fields from all fixture PDFs"


if __name__ == "__main__":
    # Allow `python tests/test_parsers.py` to print the parse report without pytest.
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    results = _parse_fixture_pdfs()
    total = len(results)
    any_count = sum(1 for _, p in results if p)
    print(f"\nParsed {any_count}/{total} fixture PDFs with at least one rate-effect field.\n")
    for name, parsed in results:
        print(f"[{name}]")
        if parsed:
            for k, v in parsed.items():
                print(f"    {k}: {v}")
        else:
            print("    (no rate-effect fields found)")
        print()
