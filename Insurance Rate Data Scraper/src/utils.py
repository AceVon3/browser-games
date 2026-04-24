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


# =====================================================================
# SERFF system-generated Filing Summary PDF parser
#
# The system PDF (named `{tracking}.pdf` at the root of the SERFF zip
# bundle) contains the Disposition section with the per-company
# Rate Information table. When the filer marks "Rate data applies to
# filing.", values match AM Best Disposition Page Data exactly.
# =====================================================================
from dataclasses import dataclass, field as _field


@dataclass
class CompanyRateRow:
    company_name: str
    overall_indicated_change: Optional[str] = None     # "15.900%"
    overall_rate_impact: Optional[str] = None          # "-2.100%"
    written_premium_change: Optional[str] = None       # "-554469" (signed numeric str)
    policyholders_affected: Optional[int] = None
    written_premium_for_program: Optional[str] = None  # "26357498"
    maximum_pct_change: Optional[str] = None           # "388.400%"
    minimum_pct_change: Optional[str] = None           # "-41.500%"


@dataclass
class FilingSummary:
    tracking_number: str
    disposition_status: Optional[str] = None
    disposition_date: Optional[str] = None
    effective_date_new: Optional[str] = None
    effective_date_renewal: Optional[str] = None
    rate_data_applies: Optional[bool] = None  # SERFF filer-set flag
    company_rates: list = _field(default_factory=list)
    multi_company_overall_indicated: Optional[str] = None
    multi_company_overall_impact: Optional[str] = None
    multi_company_premium_change: Optional[str] = None
    multi_company_policyholders: Optional[int] = None


# Pattern A: all 7 numeric values present
_FS_RATE_ROW_RE_A = re.compile(
    r"^(?P<name>.+?)\s+"
    r"(?P<ind>-?\d+(?:\.\d+)?)%\s+"
    r"(?P<imp>-?\d+(?:\.\d+)?)%\s+"
    r"\$\(?(?P<prem_chg>-?[\d,]+)\)?\s+"
    r"(?P<ph>[\d,]+)\s+"
    r"\$(?P<prem_for>[\d,]+)\s+"
    r"(?P<maxp>-?\d+(?:\.\d+)?)%\s+"
    r"(?P<minp>-?\d+(?:\.\d+)?)%\s*$"
)
# Pattern B: blank "Overall Indicated Change" rendered as bare `%`
_FS_RATE_ROW_RE_B = re.compile(
    r"^(?P<name>.+?)\s+%\s+"
    r"(?P<imp>-?\d+(?:\.\d+)?)%\s+"
    r"\$\(?(?P<prem_chg>-?[\d,]+)\)?\s+"
    r"(?P<ph>[\d,]+)\s+"
    r"\$(?P<prem_for>[\d,]+)\s+"
    r"(?P<maxp>-?\d+(?:\.\d+)?)%\s+"
    r"(?P<minp>-?\d+(?:\.\d+)?)%\s*$"
)
# Pattern C: only ind% and impact% present; rest blank ("name ind% imp% % %")
_FS_RATE_ROW_RE_C = re.compile(
    r"^(?P<name>.+?)\s+"
    r"(?P<ind>-?\d+(?:\.\d+)?)%\s+"
    r"(?P<imp>-?\d+(?:\.\d+)?)%\s+"
    r"%\s+%\s*$"
)
# Pattern D: blank indicated + blank max/min, has prem_chg + ph + prem_for
# Seen in OR State Farm filings: "name % imp% $prem_chg ph $prem_for % %"
_FS_RATE_ROW_RE_D = re.compile(
    r"^(?P<name>.+?)\s+%\s+"
    r"(?P<imp>-?\d+(?:\.\d+)?)%\s+"
    r"\$\(?(?P<prem_chg>-?[\d,]+)\)?\s+"
    r"(?P<ph>[\d,]+)\s+"
    r"\$(?P<prem_for>[\d,]+)\s+"
    r"%\s+%\s*$"
)
# Pattern E: blank indicated + blank max/min + omitted prem_chg (premium-neutral)
# Seen in OR State Farm 0% filings: "name % 0.000% ph $prem_for % %"
_FS_RATE_ROW_RE_E = re.compile(
    r"^(?P<name>.+?)\s+%\s+"
    r"(?P<imp>-?\d+(?:\.\d+)?)%\s+"
    r"(?P<ph>[\d,]+)\s+"
    r"\$(?P<prem_for>[\d,]+)\s+"
    r"%\s+%\s*$"
)
_FS_MULTI_INDICATED_RE = re.compile(r"Overall Percentage Rate Indicated For This Filing\s+(-?\d+(?:\.\d+)?)%")
_FS_MULTI_IMPACT_RE    = re.compile(r"Overall Percentage Rate Impact For This Filing\s+(-?\d+(?:\.\d+)?)%")
_FS_MULTI_PREMCHG_RE   = re.compile(r"Effect of Rate Filing[-\s]+Written Premium Change For This Program\s+\$\(?(-?[\d,]+)\)?")
_FS_MULTI_PH_RE        = re.compile(r"Effect of Rate Filing\s*[-–]\s*Number of Policyholders Affected\s+([\d,]+)")
_FS_EFF_NEW_RE     = re.compile(r"Effective Date\s+(\d{1,2}/\d{1,2}/\d{2,4})\s*\n\s*Requested\s*\(New\)")
_FS_EFF_RENEWAL_RE = re.compile(r"Effective Date\s+(\d{1,2}/\d{1,2}/\d{2,4})\s*\n\s*Requested\s*\(Renewal\)")
_FS_DISP_DATE_RE = re.compile(r"Disposition Date:\s*(\d{1,2}/\d{1,2}/\d{2,4})")
# anchor to end-of-line so empty "Disposition Status:" doesn't eat letters from later lines.
# allow mixed-case (e.g., WA "Approved", "Re-Open Processed") in addition to ID's all-caps.
_FS_DISP_STATUS_RE = re.compile(r"Disposition Status:\s*([A-Za-z][A-Za-z\- ]+?)\s*$", re.MULTILINE)
_FS_STATE_STATUS_RE = re.compile(r"State Status:\s*([A-Za-z][A-Za-z\- ]+?)\s*$", re.MULTILINE)
_FS_RATE_DATA_APPLIES_RE = re.compile(r"Rate data\s+(does NOT apply|applies)\s+to filing\.", re.IGNORECASE)
_FS_CONT_STOP = re.compile(
    r"(Overall|Schedule|Rate|Effective|D\s*isposition|Status|Comment|"
    r"PDF Pipeline|SERFF Tracking|Generated|Filing Method|Project Name|"
    r"State:|TOI/Sub-TOI|Product Name|Company Rate Information)"
)


def _fs_normalize_money(s: str) -> str:
    s = s.replace(",", "").strip()
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    return s


def parse_filing_summary_pdf(pdf_path: Path, tracking_number: str = "") -> FilingSummary:
    """Parse the SERFF system-generated Filing Summary PDF.

    Extracts the Disposition / Company Rate Information table and the
    `rate_data_applies` flag. Per-company rate rows handle three sparseness
    patterns observed in real filings (full, blank-indicated, near-empty).
    """
    import pdfplumber  # local import to avoid forcing dependency on this whole module
    fs = FilingSummary(tracking_number=tracking_number, company_rates=[])
    with pdfplumber.open(str(pdf_path)) as pdf:
        full = "\n".join((pg.extract_text() or "") for pg in pdf.pages)

    if m := _FS_DISP_DATE_RE.search(full): fs.disposition_date = m.group(1)
    if m := _FS_DISP_STATUS_RE.search(full):
        fs.disposition_status = m.group(1)
    elif m := _FS_STATE_STATUS_RE.search(full):
        fs.disposition_status = m.group(1).strip()
    if m := _FS_RATE_DATA_APPLIES_RE.search(full):
        fs.rate_data_applies = (m.group(1).lower() == "applies")
    if m := _FS_EFF_NEW_RE.search(full): fs.effective_date_new = m.group(1)
    if m := _FS_EFF_RENEWAL_RE.search(full): fs.effective_date_renewal = m.group(1)
    if m := _FS_MULTI_INDICATED_RE.search(full): fs.multi_company_overall_indicated = m.group(1) + "%"
    if m := _FS_MULTI_IMPACT_RE.search(full):    fs.multi_company_overall_impact    = m.group(1) + "%"
    if m := _FS_MULTI_PREMCHG_RE.search(full):   fs.multi_company_premium_change    = _fs_normalize_money(m.group(1))
    if m := _FS_MULTI_PH_RE.search(full):        fs.multi_company_policyholders     = int(m.group(1).replace(",", ""))

    # restrict row scan to Disposition + Company-Rate-Information sections
    section_text = []
    capture = False
    for ln in full.splitlines():
        if re.search(r"\b(D\s*isposition|Company Rate Information)\b", ln):
            capture = True
        if re.search(r"^Schedule\s+Schedule Item", ln) or re.search(r"^R\s*ate/Rule Schedule", ln):
            capture = False
        if capture:
            section_text.append(ln)
    lines = "\n".join(section_text).splitlines()

    # Dedup by normalized company name. PDFs with multiple Disposition
    # sections (one per amendment) repeat each subsidiary's row with stale
    # values from earlier dispositions; the most recent Disposition section
    # appears first in the PDF, so first-seen wins.
    seen_names: set[str] = set()
    i = 0
    while i < len(lines):
        ln = lines[i].strip()
        m = _FS_RATE_ROW_RE_A.match(ln)
        if not m: m = _FS_RATE_ROW_RE_B.match(ln)
        if not m: m = _FS_RATE_ROW_RE_D.match(ln)
        if not m: m = _FS_RATE_ROW_RE_E.match(ln)
        if not m: m = _FS_RATE_ROW_RE_C.match(ln)
        if not m:
            i += 1; continue
        gd = m.groupdict()
        name_parts = [gd["name"].strip()]
        j = i + 1
        while j < len(lines):
            nxt = lines[j].strip()
            if not nxt or "%" in nxt or "$" in nxt or _FS_CONT_STOP.search(nxt):
                break
            name_parts.append(nxt); j += 1
        full_name = " ".join(name_parts).strip()
        name_key = re.sub(r"\s+", " ", full_name.lower())
        if name_key in seen_names:
            i = j; continue
        seen_names.add(name_key)
        ind = gd.get("ind"); imp = gd.get("imp")
        fs.company_rates.append(CompanyRateRow(
            company_name=full_name,
            overall_indicated_change=(ind + "%") if ind is not None else None,
            overall_rate_impact=(imp + "%") if imp is not None else None,
            written_premium_change=_fs_normalize_money(gd["prem_chg"]) if gd.get("prem_chg") else None,
            policyholders_affected=int(gd["ph"].replace(",", "")) if gd.get("ph") else None,
            written_premium_for_program=_fs_normalize_money(gd["prem_for"]) if gd.get("prem_for") else None,
            maximum_pct_change=(gd["maxp"] + "%") if gd.get("maxp") else None,
            minimum_pct_change=(gd["minp"] + "%") if gd.get("minp") else None,
        ))
        i = j
    return fs
