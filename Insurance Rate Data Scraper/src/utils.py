"""Parsing utilities: dates, currency, percentages, company names, and PDF rate-effect extraction.

The PDF parser targets NAIC-templated phrases in rate filing memos. On failure it
returns None for the affected field and logs the tracking number — never guesses.
"""

from __future__ import annotations

import logging
import re
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ---------- scalar parsers ----------

_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y")


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


_MONEY_RE = re.compile(r"-?\(?\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*\)?")


def parse_money(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    negative = "(" in s and ")" in s or s.startswith("-")
    m = _MONEY_RE.search(s.replace(",", ""))
    # redo without replace to preserve comma-based match, but we already stripped
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s.replace(",", ""))
    if not m:
        return None
    val = float(m.group(1))
    return -val if negative else val


_PCT_RE = re.compile(
    r"\(?\s*([+-]?\s*[0-9]+(?:\.[0-9]+)?)\s*%\s*\)?"
)


def parse_percent(s: Optional[str]) -> Optional[float]:
    """Parse a percentage string like '7.5%', '(0.5%)', '-0.5 %', '+12.34%'.

    Returns a float on the 0–100 scale (so 7.5% → 7.5, not 0.075).
    Parentheses indicate a negative value.
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    m = _PCT_RE.search(s)
    if not m:
        return None
    raw = m.group(1).replace(" ", "")
    try:
        val = float(raw)
    except ValueError:
        return None
    if "(" in s and ")" in s and val > 0:
        val = -val
    return val


def parse_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    m = re.search(r"-?\d[\d,]*", str(s))
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


# ---------- company name normalization ----------

_COMPANY_SUFFIXES = re.compile(
    r"\b(company|corporation|corp\.?|insurance|ins\.?|group|llc|inc\.?|co\.?|"
    r"mutual|automobile|auto|casualty|fire|general|property|indemnity|holdings?|"
    r"services?|services|usa|of america)\b",
    re.IGNORECASE,
)

_TARGET_ALIASES = {
    "State Farm": ["state farm"],
    "GEICO": ["geico", "government employees insurance"],
    "Progressive": ["progressive"],
    "Allstate": ["allstate"],
    "Travelers": ["travelers", "travelers indemnity", "st. paul"],
    "Liberty Mutual": ["liberty mutual", "liberty insurance", "safeco"],
}


def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower()
    s = _COMPANY_SUFFIXES.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def match_target_company(company_name: str, targets: list[str]) -> Optional[str]:
    """Return the canonical target-company label if `company_name` matches one."""
    if not company_name:
        return None
    lower = company_name.lower()
    for target in targets:
        for alias in _TARGET_ALIASES.get(target, [target.lower()]):
            if alias in lower:
                return target
    return None


# ---------- PDF text extraction ----------

DEFAULT_PDF_PARSE_TIMEOUT_S = 60.0


def extract_pdf_text(pdf_path: Path) -> Optional[str]:
    """Extract text from a PDF. Tries pdfplumber first, falls back to pypdf.

    NOTE: This in-process variant has no timeout protection — pdfplumber can
    hang on certain malformed/complex PDFs even when they're under the 15MB
    cap. Prefer `extract_pdf_text_with_timeout` for new code.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        return None
    text = _extract_with_pdfplumber(pdf_path)
    if text and text.strip():
        return text
    text = _extract_with_pypdf(pdf_path)
    if text and text.strip():
        return text
    return None


def extract_pdf_text_with_timeout(
    pdf_path: Path,
    timeout_s: float = DEFAULT_PDF_PARSE_TIMEOUT_S,
) -> Tuple[Optional[str], str]:
    """Extract text via a subprocess so a hard timeout can be enforced.

    Returns (text_or_none, status). Status values:
      * "ok"        — extracted non-empty text
      * "no_text"   — extractors ran cleanly but yielded nothing (image PDF)
      * "timeout"   — worker didn't return within `timeout_s`; killed
      * "missing"   — file does not exist
      * "error:<n>" — subprocess exited non-zero (n = exit code)
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        return None, "missing"
    worker_module = "src._pdf_worker"
    project_root = Path(__file__).resolve().parent.parent
    try:
        r = subprocess.run(
            [sys.executable, "-m", worker_module, str(pdf_path)],
            capture_output=True,
            timeout=timeout_s,
            cwd=str(project_root),
        )
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except Exception as e:
        return None, f"error:spawn:{type(e).__name__}"
    if r.returncode != 0:
        return None, f"error:{r.returncode}"
    text = r.stdout.decode("utf-8", errors="replace")
    if not text.strip():
        return None, "no_text"
    return text, "ok"


def _extract_with_pdfplumber(pdf_path: Path) -> Optional[str]:
    try:
        import pdfplumber
    except ImportError:
        return None
    try:
        chunks = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                chunks.append(t)
        return "\n".join(chunks)
    except Exception as e:
        logger.warning("pdfplumber failed for %s: %s", pdf_path.name, e)
        return None


def _extract_with_pypdf(pdf_path: Path) -> Optional[str]:
    try:
        from pypdf import PdfReader
    except ImportError:
        return None
    try:
        reader = PdfReader(str(pdf_path))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception as e:
        logger.warning("pypdf failed for %s: %s", pdf_path.name, e)
        return None


# ---------- PDF rate-effect parsing ----------

# Each pattern maps a regex (case-insensitive, DOTALL) capturing one group containing
# the value "chunk" (a percentage, money amount, or integer), to a parser function
# and the target field name.

_PCT_CAPTURE = r"([+-]?\(?\s*[0-9]+(?:\.[0-9]+)?\s*%\)?)"
_MONEY_CAPTURE = r"(\(?\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?)"
_INT_CAPTURE = r"(-?[0-9][0-9,]*)"

PDF_FIELD_PATTERNS: list[tuple[str, str, callable]] = [
    # ----- overall_rate_effect (first match wins) -----
    # Company-prose: "filing includes a revision of 19.1% to the/our premium income level"
    (
        "overall_rate_effect",
        rf"filing\s+includes\s+(?:a\s+)?revision\s+of\s+{_PCT_CAPTURE}\s+to\s+(?:our|the)?\s*premium\s+income\s+level",
        parse_percent,
    ),
    # Company-prose: "will result in an overall rate change of 19.1% for the ... Program/Plan/Line"
    (
        "overall_rate_effect",
        rf"(?:will\s+)?result\s+in\s+an?\s+overall\s+rate\s+change\s+of\s+{_PCT_CAPTURE}\s+for\s+(?:the|our)\s+[\w\s\-&/]{{0,60}}?(?:Program|Plan|Line)\b",
        parse_percent,
    ),
    # Company-prose: "result in a -2.4% overall rate decrease" (Allstate PPA filing memos)
    # / "result in a 0% rate impact" (Travelers actuarial memos).
    # Handled separately below so we can sign-flip when the direction word is
    # "decrease" and the captured value has no explicit sign.
    # NAIC-templated
    (
        "overall_rate_effect",
        rf"Overall\s+Rate\s+Level\s+(?:Change|Impact|Effect|Increase)\b[^%\n\r]{{0,80}}?{_PCT_CAPTURE}",
        parse_percent,
    ),
    (
        "overall_rate_effect",
        rf"Total\s+Rate\s+(?:Level\s+)?(?:Change|Impact|Effect|Increase)\b[^%\n\r]{{0,80}}?{_PCT_CAPTURE}",
        parse_percent,
    ),
    # ----- requested_rate_effect -----
    (
        "requested_rate_effect",
        rf"Requested\s+(?:Overall\s+)?(?:Rate\s+)?(?:Level\s+)?(?:Change|Impact|Effect|Increase)\b[^%\n\r]{{0,80}}?{_PCT_CAPTURE}",
        parse_percent,
    ),
    # Indicated rate change — actuarial indication (closest to "requested" in the company's view)
    (
        "requested_rate_effect",
        rf"Indicated\s+Rate\s+(?:Level\s+)?(?:Change|Increase|Effect)\b[^%\n\r]{{0,80}}?{_PCT_CAPTURE}",
        parse_percent,
    ),
    # ----- approved_rate_effect -----
    (
        "approved_rate_effect",
        rf"Approved\s+(?:Overall\s+)?(?:Rate\s+)?(?:Level\s+)?(?:Change|Impact|Effect|Increase)\b[^%\n\r]{{0,80}}?{_PCT_CAPTURE}",
        parse_percent,
    ),
    # ----- affected_policyholders -----
    (
        "affected_policyholders",
        rf"Number\s+of\s+(?:Affected\s+)?Policyholders?\b[^0-9\n\r]{{0,40}}?{_INT_CAPTURE}",
        parse_int,
    ),
    (
        "affected_policyholders",
        rf"Policyholders?\s+Affected\b[^0-9\n\r]{{0,40}}?{_INT_CAPTURE}",
        parse_int,
    ),
    (
        "affected_policyholders",
        rf"(?:There\s+are|Approximately)\s+{_INT_CAPTURE}\s+(?:polic(?:ies|yholders?))\s+(?:impacted|affected|in\s+force)",
        parse_int,
    ),
    # ----- written_premium_volume -----
    (
        "written_premium_volume",
        rf"Written\s+Premium\s+Volume\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
    (
        "written_premium_volume",
        rf"Total\s+Written\s+Premium\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
    # ----- current / proposed avg premium -----
    (
        "current_avg_premium",
        rf"Current\s+(?:Annual\s+)?Average\s+Premium\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
    (
        "proposed_avg_premium",
        rf"Proposed\s+(?:Annual\s+)?Average\s+Premium\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
    # ----- annual premium impact -----
    (
        "annual_premium_impact_dollars",
        rf"Annual\s+Premium\s+(?:Change|Impact|Effect)\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
    (
        "annual_premium_impact_dollars",
        rf"Total\s+Annual\s+Premium\s+(?:Change|Impact)\b[^$0-9\n\r]{{0,60}}?{_MONEY_CAPTURE}",
        parse_money,
    ),
]

# Direction-aware "result in a X% overall rate DIRECTION" extractor.
# Matches variants like:
#   "result in a -2.4% overall rate decrease"         (Allstate ANAIC PPA memos)
#   "result in a 0% rate impact"                      (Travelers VCSE actuarial memo)
#   "will result in an 11.9% overall rate change"
# When the direction word is "decrease" and the captured number has no explicit
# sign (and isn't zero), the value is flipped negative so the recorded
# overall_rate_effect has the correct direction.
_RESULT_IN_RATE_RE = re.compile(
    r"result\s+in\s+an?\s+"
    r"([+\-]?\(?\s*[0-9]+(?:\.[0-9]+)?\s*%\)?)"  # signed/unsigned/parenthesized %
    r"\s+(?:overall\s+)?rate\s+"
    r"(impact|change|decrease|increase|effect|adjustment)",
    re.IGNORECASE,
)


def _extract_result_in_rate(flat_text: str) -> Optional[float]:
    m = _RESULT_IN_RATE_RE.search(flat_text)
    if not m:
        return None
    val = parse_percent(m.group(1))
    if val is None:
        return None
    direction = m.group(2).lower()
    raw = m.group(1)
    # If direction says "decrease" and the number is unsigned positive, flip it.
    if direction == "decrease" and val > 0 and not re.search(r"[+\-]|\(", raw):
        val = -val
    return val


# Tightly-bound "credibility-weighted indication of X.X%" extractor. Lower
# confidence than NAIC-templated patterns and the result-in-rate catcher above
# — only runs as a final fallback. Used for filings where the rate change is
# stated in an objection-response letter rather than the company's filing memo
# (e.g., SFMA-134603714 PLUP, where WA OIC said "this results in a
# credibility-weighted indication of 73.9%" and State Farm agreed to that
# rate).
#
# Constraints to suppress false positives:
#   * "indication" must appear immediately before the percentage — bare
#     "credibility-weighted" is far too broad (it describes loss ratios in
#     nearly every actuarial PDF).
#   * Reject if the IMMEDIATE 30-char neighborhood is a loss-ratio descriptor:
#     pre-context ending with "loss [and lae] ratio" (catches "loss ratio
#     indication of X%") or post-context starting with "loss [and lae] ratio"
#     (catches "indication of X% loss ratio"). A blanket "loss or ratio
#     anywhere within 50 chars" filter rejects the SFMA case (where
#     "permissible loss ratio" is in a separate clause five words upstream),
#     so we look for the specific antagonist phrases instead.
_CREDIBILITY_INDICATION_RE = re.compile(
    r"credibility[-\s]+weighted\s+indication\s+of\s+"
    r"([+\-]?\(?\s*[0-9]+(?:\.[0-9]+)?\s*%\)?)",
    re.IGNORECASE,
)
_LOSS_RATIO_PHRASE = re.compile(
    r"\b(?:loss|lae)(?:\s+(?:and|&)\s+lae)?\s+ratio\b",
    re.IGNORECASE,
)


def _extract_credibility_indication(flat_text: str) -> Optional[float]:
    for m in _CREDIBILITY_INDICATION_RE.finditer(flat_text):
        pre = flat_text[max(0, m.start() - 30):m.start()]
        post = flat_text[m.end():m.end() + 30]
        # "<...> loss ratio  indication of X%" — adjacent loss-ratio descriptor
        if re.search(r"(?:loss|lae)(?:\s+(?:and|&)\s+lae)?\s+ratio\s*$", pre, re.IGNORECASE):
            continue
        # "indication of X%  loss ratio <...>" — adjacent loss-ratio descriptor
        if re.search(r"^\s*(?:loss|lae)(?:\s+(?:and|&)\s+lae)?\s+ratio\b", post, re.IGNORECASE):
            continue
        val = parse_percent(m.group(1))
        if val is not None:
            return val
    return None


# New-product/new-company launch indicators. When a PDF has no parseable rate
# fields AND contains one of these phrases, the filing is a product launch (no
# existing book to change rates on) rather than a parser failure. ANAIC-style
# greenfield launches in ID put Allstate at 0/9 parse rate, which is correct —
# these filings genuinely don't carry an overall rate change %.
NEW_PRODUCT_LAUNCH_PATTERNS = [
    r"introduc(?:ing|e|es)\s+(?:a|the)\s+new\s+(?:\w+\s+){0,6}?"
    r"(?:product|company|risk\s+classification\s+plan|rating\s+plan|program)",
    r"introduc(?:ing|e|es)\s+coverage\s+for\s+",
    r"new\s+company\s*:\s*[A-Z]",
    r"proposed\s+new\s+business\s+effective\s+date",
]


def _is_new_product_launch(flat_text: str) -> bool:
    for pattern in NEW_PRODUCT_LAUNCH_PATTERNS:
        if re.search(pattern, flat_text, re.IGNORECASE):
            return True
    return False


# Sentinel phrases meaning "no rate change" — when matched (and overall_rate_effect
# hasn't already been set from a numeric pattern), we record 0.0.
ZERO_RATE_CHANGE_SENTINELS = [
    r"premium[\s-]+neutral",
    r"no\s+(?:specific\s+|overall\s+)?rate\s+impact",
    r"no\s+rate\s+change",
    r"will\s+have\s+no\s+rate\s+impact",
    r"rate[\s-]+neutral",
    r"there\s+is\s+no\s+specific\s+rate\s+impact",
]

# Minimum plausible values — rejects matches that pull tiny footnote numbers.
_MIN_SANITY = {
    "written_premium_volume": 10_000.0,  # no real WPV is under $10k
    "current_avg_premium": 50.0,
    "proposed_avg_premium": 50.0,
    "annual_premium_impact_dollars": 100.0,
}


def parse_rate_effect_pdf(
    pdf_path: Path,
    tracking_number: str = "",
    timeout_s: float = DEFAULT_PDF_PARSE_TIMEOUT_S,
) -> Tuple[dict, str]:
    """Extract rate-effect fields from a filing PDF.

    Returns (fields_dict, parse_status). Parse status mirrors
    `extract_pdf_text_with_timeout` plus parser-level outcomes:
      * "ok"            — text extracted, ≥1 field matched
      * "no_fields"     — text extracted, no rate-effect field matched
      * "no_text"       — extractor returned empty (image-only PDF)
      * "timeout"       — pdfplumber/pypdf killed after `timeout_s`
      * "missing"       — file not on disk
      * "error:<n>"     — worker exited non-zero

    Field dict keys (any subset, never set to None):
      overall_rate_effect, requested_rate_effect, approved_rate_effect,
      affected_policyholders, written_premium_volume, current_avg_premium,
      proposed_avg_premium, annual_premium_impact_dollars.
    """
    pdf_path = Path(pdf_path)
    result: dict = {}
    text, status = extract_pdf_text_with_timeout(pdf_path, timeout_s=timeout_s)
    if status != "ok" or not text:
        logger.warning(
            "pdf_parse_failed tracking=%s path=%s reason=%s",
            tracking_number,
            pdf_path.name,
            status,
        )
        return result, status

    # Normalize whitespace (PDFs often have weird line breaks mid-phrase)
    flat = re.sub(r"\s+", " ", text)

    for field_name, pattern, parser in PDF_FIELD_PATTERNS:
        if field_name in result:
            continue  # first pattern wins per field
        m = re.search(pattern, flat, re.IGNORECASE)
        if not m:
            continue
        raw = m.group(1)
        value = parser(raw)
        if value is None:
            continue
        # Sanity-check numeric fields
        floor = _MIN_SANITY.get(field_name)
        if floor is not None and abs(value) < floor:
            continue
        result[field_name] = value

    # Direction-aware "result in a X% overall rate …" catcher — runs only if the
    # NAIC-templated patterns above didn't already fill overall_rate_effect.
    if "overall_rate_effect" not in result:
        val = _extract_result_in_rate(flat)
        if val is not None:
            result["overall_rate_effect"] = val

    # Credibility-weighted indication — lowest-confidence fallback. Only runs
    # if neither the NAIC-templated patterns nor the result-in-rate catcher
    # set overall_rate_effect, so OVERALL RATE IMPACT always wins on conflict.
    if "overall_rate_effect" not in result:
        val = _extract_credibility_indication(flat)
        if val is not None:
            result["overall_rate_effect"] = val

    # Sentinel check for 0.0 overall rate change (only if not already set)
    if "overall_rate_effect" not in result:
        for sentinel in ZERO_RATE_CHANGE_SENTINELS:
            if re.search(sentinel, flat, re.IGNORECASE):
                result["overall_rate_effect"] = 0.0
                break

    if not result:
        if _is_new_product_launch(flat):
            logger.info(
                "pdf_parse_new_product_launch tracking=%s path=%s",
                tracking_number,
                pdf_path.name,
            )
            return result, "new_product_launch"
        logger.info(
            "pdf_parse_empty tracking=%s path=%s — no rate-effect fields matched",
            tracking_number,
            pdf_path.name,
        )
        return result, "no_fields"
    return result, "ok"
