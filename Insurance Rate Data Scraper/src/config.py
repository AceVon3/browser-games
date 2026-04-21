"""Configuration for the Insurance Rate Data Scraper (Phase 1: one-time collection)."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
PDF_DIR = OUTPUT_DIR / "pdfs"
LOG_FILE = OUTPUT_DIR / "scraper.log"

TARGET_COMPANIES = [
    "State Farm",
    "GEICO",
    "Progressive",
    "Allstate",
    "Travelers",
    "Liberty Mutual",
]

# Lines of business we care about for the final product: personal lines only.
# Matched case-insensitive partial against `type_of_insurance` and
# `sub_type_of_insurance`. Out-of-scope lines (Commercial Auto, Personal
# Umbrella, CMP, Professional Liability, Renters, VIP, etc.) stay in the raw
# dataset but are flagged via `Filing.in_target_lines = False`.
TARGET_LINES = [
    "personal auto",
    "private passenger auto",
    "homeowners",
]

STATES = ["WA", "ID", "CO"]

DATE_FROM = "01/01/2025"
DATE_TO = "04/17/2026"

REQUEST_DELAY = 2.5

DOWNLOAD_PDFS = True
PARSE_PDFS = True

SERFF_BASE = "https://filingaccess.serff.com/sfa"
SERFF_HOME_URL = SERFF_BASE + "/home/{state}"
SERFF_DETAIL_URL = SERFF_BASE + "/filingSummary.xhtml?filingId={filing_id}"

HEADLESS = True
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
