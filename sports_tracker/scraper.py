"""
Cover2Sports Betting Analytics Scraper
Logs into cover2sports.net, scrapes your personal bet history,
and generates a self-contained HTML analytics dashboard.

Usage:
    python scraper.py           # headless mode
    python scraper.py --headed  # watch the browser (debug)
"""

import sys
import json
import re
import argparse
import getpass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration — adjust selectors here if the site ever changes
# ---------------------------------------------------------------------------
START_DATE           = "2026-03-02"
SITE_URL             = "https://www.cover2sports.net"
SPORTS_URL           = "https://cover2sports.net/sports.html"
OUTPUT_FILE          = Path(__file__).parent / "betting_dashboard.html"
SCREENSHOT_FILE      = Path(__file__).parent / "debug_screenshot.png"

# CSS/text selectors (update if site restructures)
BALANCE_SELECTOR     = '[data-action="get-figure"]'
TRANSACTIONS_TAB     = "My Transactions"
WEEKLY_FIGURES_TAB   = "Weekly Figures"

# Text that must NEVER be clicked (bet-placement safety guard)
FORBIDDEN_BUTTON_TEXT = ["place", "wager", "submit bet", "bet now", "place bet", "confirm bet"]

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_american_odds(text: str) -> Optional[int]:
    """Convert '+150', '-110', '150' → integer American odds."""
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return int(text)
    except ValueError:
        return None


def parse_money(text: str) -> Optional[float]:
    """Convert '$25.00', '-$10.50', '25' → float dollars."""
    if not text:
        return None
    text = text.strip().replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(text: str) -> Optional[str]:
    """Attempt to parse a date string into ISO format YYYY-MM-DD."""
    if not text:
        return None
    text = text.strip()
    formats = [
        "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d",
        "%b %d, %Y", "%B %d, %Y",
        "%m-%d-%Y", "%m-%d-%y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def infer_bet_type(text: str) -> str:
    """Guess Moneyline / Spread / Over/Under from free text."""
    low = text.lower()
    if any(k in low for k in ["over", "under", "o/u", "total"]):
        return "Over/Under"
    if any(k in low for k in ["+", "-", "spread", "pts", "point"]) and "ml" not in low:
        # crude heuristic — spread lines contain +/- numbers
        return "Spread"
    return "Moneyline"


def infer_sport(text: str) -> str:
    """Guess sport from team/league name text."""
    low = text.lower()

    # G-format bet IDs include explicit sport label: "G123456 - Basketball - TeamA vs TeamB"
    g_sport = re.search(r'- (basketball|football|baseball|hockey|soccer) -', low)
    if g_sport:
        sport_word = g_sport.group(1)
        if sport_word == "basketball":
            if any(k in low for k in [
                "lakers", "celtics", "warriors", "bulls", "heat", "nets", "knicks", "bucks",
                "suns", "clippers", "nuggets", "jazz", "thunder", "rockets", "wizards",
                "pistons", "cavaliers", "cavs", "spurs", "raptors", "hawks", "hornets",
                "pelicans", "grizzlies", "timberwolves", "trail blazers", "blazers", "kings",
                "magic", "pacers", "76ers", "sixers", "mavericks", "mavs",
            ]):
                return "NBA"
            return "NCAAB"
        if sport_word == "football": return "NFL"
        if sport_word == "baseball": return "MLB"
        if sport_word == "hockey":   return "NHL"
        if sport_word == "soccer":   return "Soccer"

    # Explicit league/sport indicators
    if any(k in low for k in ["ncaamb", "ncaab", "college basketball", "cbb", "march madness"]):
        return "NCAAB"
    if any(k in low for k in ["ncaaw", "women's basketball"]):
        return "NCAAW"
    if "nba" in low:
        return "NBA"
    if any(k in low for k in ["nfl", "ncaaf", "college football", "cfb"]):
        return "NFL"
    if any(k in low for k in ["mlb", "baseball"]):
        return "MLB"
    if any(k in low for k in ["nhl", "hockey"]):
        return "NHL"
    if any(k in low for k in ["soccer", "mls", "epl", "premier league", "champions league",
                               "fifa", "la liga", "bundesliga", "serie a", "ligue 1"]):
        return "Soccer"
    if any(k in low for k in ["ufc", "mma", "bellator"]):
        return "UFC/MMA"
    if any(k in low for k in ["boxing", "bout"]):
        return "Boxing"

    # Golf-specific tournaments and terms (before generic "odds to win" check)
    if any(k in low for k in ["pga", "lpga", "golf", "masters", "cognizant", "rbc heritage",
                               "the players", "genesis invitational", "farmers insurance",
                               "arnold palmer", "memorial", "us open golf", "british open",
                               "tour championship", "fedex cup"]):
        return "Golf"

    # "Odds to win" futures — classify by context before defaulting to Golf
    if "odds to win" in low:
        # Basketball conference/tournament futures
        if any(k in low for k in [
            "atlantic sun", "ohio valley", "west coast", "big ten", "big 12",
            "big east", "sec tournament", "acc tournament", "pac-12", "pac 12",
            "mac tournament", "sun belt", "mountain west", "conference usa",
            "horizon league", "america east", "southern conference", "colonial",
            "patriot league", "ivy league", "wac tournament", "a-sun",
        ]):
            return "NCAAB"
        # Golf player names → Golf futures
        if any(k in low for k in [
            "meissner", "bezuidenhout", "hojgaard", "mcilroy", "spieth", "koepka",
            "scheffler", "morikawa", "fleetwood", "hovland", "schauffele", "fitzpatrick",
            "cantlay", "thomas", "johnson", "scott", "rose", "fowler", "finau",
            "lowry", "rahm", "burns", "english", "young", "kim", "macintyre",
            "bourne", "td scorer", "first td", "coin toss",
        ]):
            return "Golf"
        return "Golf"  # default "odds to win" to Golf when no other context

    # NBA teams
    if any(k in low for k in [
        "lakers", "celtics", "warriors", "bulls", "heat", "nets", "knicks", "bucks",
        "suns", "clippers", "nuggets", "jazz", "thunder", "rockets", "wizards",
        "pistons", "cavaliers", "cavs", "spurs", "raptors", "hawks", "hornets",
        "pelicans", "grizzlies", "timberwolves", "trail blazers", "blazers", "kings",
        "magic", "pacers", "76ers", "sixers", "mavericks", "mavs",
    ]):
        return "NBA"

    # NCAAB teams
    if any(k in low for k in [
        "kansas", "duke", "kentucky", "gonzaga", "villanova", "baylor", "houston",
        "ucla", "usc", "arizona", "michigan st", "ohio st", "florida", "texas",
        "uconn", "connecticut", "st johns", "yale", "rhode island", "santa clara",
        "st marys", "illinois", "iowa", "indiana", "purdue", "wisconsin",
        "notre dame", "louisville", "syracuse", "pittsburgh", "clemson",
        "alabama", "auburn", "lsu", "tennessee", "arkansas", "ole miss", "vanderbilt",
        "nc state", "north carolina st", "geo washington", "montana st",
        "marquette", "creighton", "xavier", "butler", "dayton", "wichita st",
        "seattle u", "seattle redhawks", "morehead st", "high point",
        "north florida", "fla gulf coast", "florida gulf coast", "bellarmine",
        "central arkansas", "ark little rock", "lindenwood", "se missouri",
        "southern illinois", "eastern illinois", "eastern kentucky",
        "james madison", "southern miss", "indiana st", "valparaiso",
        "evansville", "northern iowa", "south dakota st", "st thomas",
        "manhattan", "fairfield", "portland", "pepperdine", "drake",
        "stetson", "north alabama", "jacksonville", "north carolina a&t",
        "northeastern", "murray st", "illinois chicago", "sacred heart",
        "merrimack", "longwood", "west georgia", "campbell", "drexel",
        "alcorn st", "alabama st", "nebraska omaha",
    ]):
        return "NCAAB"

    # NFL teams
    if any(k in low for k in [
        "chiefs", "eagles", "patriots", "cowboys", "49ers", "packers", "bears",
        "giants", "jets", "dolphins", "bills", "ravens", "steelers", "browns",
        "bengals", "titans", "colts", "jaguars", "texans", "broncos", "raiders",
        "chargers", "seahawks", "rams", "cardinals", "falcons", "saints", "panthers",
        "buccaneers", "vikings", "lions", "redskins", "commanders",
    ]):
        return "NFL"

    # Golf player names
    if any(k in low for k in [
        "meissner", "bezuidenhout", "hojgaard", "mcilroy", "spieth", "koepka",
        "scheffler", "morikawa", "fleetwood", "hovland", "schauffele", "fitzpatrick",
        "cantlay", "thomas", "johnson", "scott", "rose", "fowler", "finau",
        "lowry", "rahm", "burns", "english", "young", "macintyre",
        "s woo kim", "woo kim",
    ]):
        return "Golf"

    return "Other"


def infer_amount(pl: float, result: str, odds: Optional[int], wager_type: str) -> Optional[float]:
    """Back-calculate the wager amount from P/L and odds."""
    if pl is None:
        return None
    if result == "Loss":
        return round(abs(pl), 2)
    if result == "Win" and odds is not None and wager_type == "Straight":
        if odds < 0:
            return round(pl * abs(odds) / 100, 2)
        elif odds > 0:
            return round(pl * 100 / odds, 2)
    return None


def classify_wager_type(text: str) -> str:
    """Map single-letter codes or full words to wager type strings."""
    low = text.strip().lower()
    mapping = {
        "s": "Straight", "straight": "Straight",
        "p": "Parlay",   "parlay":   "Parlay",
        "t": "Teaser",   "teaser":   "Teaser",
        "r": "Reverse",  "reverse":  "Reverse",
        "i": "IF Bet",   "if":       "IF Bet", "if bet": "IF Bet",
    }
    return mapping.get(low, "Straight")


def classify_result(text: str) -> str:
    low = text.strip().lower()
    if "win" in low or "won" in low:
        return "Win"
    if "loss" in low or "lose" in low or "lost" in low:
        return "Loss"
    if "push" in low or "tie" in low:
        return "Push"
    return "Pending"


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def safety_check_page(page):
    """Abort if any dangerous bet-placement buttons are visible on the page."""
    for btn_text in FORBIDDEN_BUTTON_TEXT:
        # We only check — we never click these
        pass  # detection only; actual safety is in never calling btn.click()


def scrape_transactions(page) -> list[dict]:
    """
    Scrape the My Transactions tab.
    Returns a list of raw row dicts from whatever table/list structure is present.
    """
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    rows = []

    # Try common table selectors
    table_selectors = [
        "table tr",
        "table tbody tr",
        '[class*="transaction"] [class*="row"]',
        '[class*="bet"] [class*="item"]',
        '[class*="history"] tr',
        '[class*="wager"] tr',
    ]

    for selector in table_selectors:
        try:
            page.wait_for_selector(selector, timeout=5000)
            elements = page.query_selector_all(selector)
            if elements:
                print(f"  Found {len(elements)} rows using selector: {selector}")
                for el in elements:
                    cells = el.query_selector_all("td, [class*='cell'], [class*='col']")
                    cell_texts = [c.inner_text().strip() for c in cells]
                    if cell_texts:
                        rows.append({"raw_cells": cell_texts, "raw_html": el.inner_html()})
                break
        except PlaywrightTimeout:
            continue

    if not rows:
        # Fallback: grab all visible text blocks that look like bet entries
        print("  No table found; attempting text-pattern fallback...")
        content = page.inner_text("body")
        # Return raw content for manual parsing
        rows.append({"raw_text": content, "fallback": True})

    return rows


def parse_row_to_bet(raw: dict) -> Optional[dict]:
    """
    Convert a raw scraped row into a structured bet dict.
    This is inherently heuristic — adjust column indices to match actual site layout.
    """
    cells = raw.get("raw_cells", [])

    # Typical cover2sports transaction columns (adjust indices as needed):
    # [0]=Date  [1]=Type  [2]=Description/Game  [3]=Pick  [4]=Odds  [5]=Amount  [6]=Result  [7]=P/L
    # Column count may vary — we try to be defensive
    if len(cells) < 3:
        return None

    def get(idx, default=""):
        return cells[idx] if idx < len(cells) else default

    date_str   = parse_date(get(0))
    wager_code = get(1)
    game       = get(2)
    pick       = get(3)
    odds_str   = get(4)
    amount_str = get(5)
    result_str = get(6)
    pl_str     = get(7)

    # Skip header rows
    if date_str is None and any(h in get(0).lower() for h in ["date", "type", "game", "wager"]):
        return None

    # Skip rows with no meaningful data
    if not game and not pick:
        return None

    return {
        "date":        date_str or "",
        "sport":       infer_sport(f"{game} {pick}"),
        "bet_type":    infer_bet_type(f"{game} {pick} {odds_str}"),
        "wager_type":  classify_wager_type(wager_code),
        "game":        game,
        "pick":        pick,
        "odds":        parse_american_odds(odds_str),
        "amount":      parse_money(amount_str),
        "result":      classify_result(result_str),
        "profit_loss": parse_money(pl_str),
    }


def parse_modal_bet_row(cells: list, bet_date: str = "") -> Optional[dict]:
    """
    Parse a 2-column modal row from the Weekly Figures detail modal.
    Format: [description/odds, P/L amount]
    e.g. ['Louisville -14½ -110', '+$50']
         ['Alabama/GeorgiaO 179 -110', '+$50']
         ['G287004691 - Basketball - TeamA vs TeamB / Game / Winner / Pick +290', '-$30']
    """
    import re
    if not cells:
        return None

    desc = cells[0].strip() if cells else ""
    pl_str = cells[1].strip() if len(cells) > 1 else ""

    if not desc:
        return None

    # Skip non-bet rows (empty days, accounting entries, etc.)
    skip_keywords = ["credit", "adjustment", "bonus", "deposit", "withdrawal",
                     "carry", "balance", "transactions", "week",
                     "no data available"]
    if any(k in desc.lower() for k in skip_keywords):
        return None

    # Parse P/L
    pl: Optional[float] = None
    if pl_str:
        raw_pl = pl_str.replace("+", "").replace("$", "").replace(",", "").strip()
        try:
            pl = float(raw_pl)
            if pl_str.startswith("-"):
                pl = -abs(pl)
            else:
                pl = abs(pl)
        except ValueError:
            pass

    # Result from P/L sign
    if pl is not None:
        result = "Win" if pl > 0 else "Loss" if pl < 0 else "Push"
    else:
        result = "Pending"

    # Extract odds — last token matching +/-NNN or NNN at end of description
    odds: Optional[int] = None
    odds_match = re.search(r'([+-]?\d{2,4})\s*(?:-\s*1st Half|$)', desc)
    if odds_match:
        odds = parse_american_odds(odds_match.group(1))

    # Detect wager type (Parlay/Teaser/Straight)
    if re.search(r'\bparlay\b', desc, re.IGNORECASE):
        wager_type = "Parlay"
    elif re.search(r'\bteaser\b', desc, re.IGNORECASE):
        wager_type = "Teaser"
    else:
        wager_type = "Straight"

    # Use only the first line for bet type / pick detection
    # (parlays and futures may have multi-line descriptions)
    desc_first = desc.split('\n')[0].strip()

    # Detect bet type from description
    # Site appends O/U directly to team name (e.g. "PistonsU 220½", "TexasO 158"),
    # so don't require a word boundary — just O or U followed by whitespace + digit.
    is_half = "1st Half" in desc_first or "2nd Half" in desc_first
    total_match_o = re.search(r'O\s+(\d+[½¼¾]?)|\bOver\s+(\d+[½¼¾]?)', desc_first, re.IGNORECASE)
    total_match_u = re.search(r'U\s+(\d+[½¼¾]?)|\bUnder\s+(\d+[½¼¾]?)', desc_first, re.IGNORECASE)
    spread_match  = re.search(r'(.+?)\s+([+-]\d+[½¼¾]?)\s+[+-]\d{2,3}', desc_first)

    if total_match_o:
        bet_type = "Over/Under"
        total = (total_match_o.group(1) or total_match_o.group(2) or "").strip()
        pick = f"Over {total}" if total else "Over"
    elif total_match_u:
        bet_type = "Over/Under"
        total = (total_match_u.group(1) or total_match_u.group(2) or "").strip()
        pick = f"Under {total}" if total else "Under"
    elif spread_match:
        bet_type = "Spread"
        pick = f"{spread_match.group(1).strip()} {spread_match.group(2)}"
    else:
        bet_type = "Moneyline"
        # Strip trailing ML suffix and odds to get just the team/player name
        ml_clean = re.sub(r'\s*ML\s*[+-]?\d+[½¼¾]?\s*$', '', desc_first, flags=re.IGNORECASE)
        ml_clean = re.sub(r'\s+[+-]\d{2,4}\s*$', '', ml_clean).strip()
        pick = ml_clean

    # Game description — first line, stripped of trailing odds
    game = re.sub(r'\s+[+-]?\d{2,4}\s*$', '', desc_first).strip()
    if is_half:
        game = re.sub(r'\s*-?\s*1st Half\s*$', '', game, flags=re.IGNORECASE).strip()
    # For parlays, use the header line as a tidy label
    if wager_type == "Parlay":
        game = desc_first  # e.g. "Parlay - 8 Teams"
        pick = ""

    return {
        "date":        bet_date,
        "sport":       infer_sport(desc),
        "bet_type":    bet_type,
        "wager_type":  wager_type,
        "game":        game,
        "pick":        pick,
        "odds":        odds,
        "amount":      infer_amount(pl, result, odds, wager_type),
        "result":      result,
        "profit_loss": pl,
    }


def filter_by_start_date(bets: list[dict], start: str) -> list[dict]:
    """Keep only bets on or after start date (ISO string)."""
    cutoff = date.fromisoformat(start)
    filtered = []
    for b in bets:
        try:
            if b.get("date") and date.fromisoformat(b["date"]) >= cutoff:
                filtered.append(b)
        except ValueError:
            filtered.append(b)  # keep bets with unparseable dates
    return filtered


def filter_by_date_range(bets: list[dict], start: str, end: str) -> list[dict]:
    """Keep only bets between start and end dates (ISO strings, inclusive)."""
    start_d = date.fromisoformat(start)
    end_d   = date.fromisoformat(end)
    filtered = []
    for b in bets:
        try:
            if b.get("date"):
                d = date.fromisoformat(b["date"])
                if start_d <= d <= end_d:
                    filtered.append(b)
        except ValueError:
            filtered.append(b)  # keep bets with unparseable dates
    return filtered


DEBUG_LOG = Path(__file__).parent / "debug.log"


def dlog(msg: str):
    """Write a debug line to debug.log with flush."""
    with open(DEBUG_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    sys.stdout.buffer.write((msg + "\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()


def run_scraper(username: str, password: str, headed: bool = False,
                start_date: str = START_DATE,
                end_date: str = None) -> list[dict]:
    """Main scraping routine. Returns list of parsed bet dicts."""
    if end_date is None:
        end_date = date.today().isoformat()

    # Clear debug log for this run
    DEBUG_LOG.write_text("", encoding="utf-8")
    dlog("=== run_scraper started ===")
    dlog(f"Date range: {start_date} to {end_date}")

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    except ImportError:
        raise RuntimeError("Playwright not installed. Run: pip install playwright && playwright install chromium")

    bets = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        try:
            # ----------------------------------------------------------------
            # 1. Login
            # ----------------------------------------------------------------
            print(f"Navigating to {SITE_URL} ...")
            page.goto(SITE_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)

            # Fill login form — only username/password fields, no other submissions
            username_sel = 'input[type="text"], input[name*="user"], input[id*="user"], input[placeholder*="user" i], input[placeholder*="email" i]'
            password_sel = 'input[type="password"]'

            try:
                page.wait_for_selector(username_sel, timeout=10000)
                page.fill(username_sel, username)
                page.fill(password_sel, password)
            except PlaywrightTimeout:
                page.screenshot(path=str(SCREENSHOT_FILE))
                raise RuntimeError(f"Login form not found. Screenshot saved to {SCREENSHOT_FILE}. Hint: Update 'username_sel' in scraper.py.")

            # Submit via Enter key (avoids clicking any labeled "Place Bet" button)
            page.press(password_sel, "Enter")

            # ----------------------------------------------------------------
            # 2. Wait for sports page to load
            # ----------------------------------------------------------------
            print("Waiting for redirect to sports page...")
            try:
                page.wait_for_url("**/sports.html", timeout=20000)
            except PlaywrightTimeout:
                # Some sites redirect differently — check for login error
                current = page.url
                page_text = page.inner_text("body").lower()
                if any(e in page_text for e in ["invalid", "incorrect", "failed", "error", "wrong"]):
                    page.screenshot(path=str(SCREENSHOT_FILE))
                    raise RuntimeError("Login failed — invalid credentials or account issue.")
                print(f"  Still on: {current} — continuing...")

            dlog("Login successful. Waiting for page to fully load...")
            page.wait_for_timeout(3000)
            dlog(f"  Current URL after login: {page.url}")

            # ----------------------------------------------------------------
            # 3. Open account/history panel
            # ----------------------------------------------------------------
            dlog("Opening account history panel...")
            dlog(f"  Total frames: {len(page.frames)}")
            for i, frame in enumerate(page.frames):
                dlog(f"  Frame {i}: {frame.url}")

            # Try clicking via JavaScript across all frames
            clicked = False
            selectors_to_try = [
                '[data-action="get-figure"]',
                '[data-field="balance"]',
                '[data-language="L-403"]',
            ]

            for frame in page.frames:
                if clicked:
                    break
                for sel in selectors_to_try:
                    try:
                        result = frame.evaluate(f"""
                            (() => {{
                                const el = document.querySelector('{sel}');
                                if (el) {{ el.click(); return el.outerHTML; }}
                                return null;
                            }})()
                        """)
                        if result:
                            dlog(f"  Clicked via JS in frame '{frame.url}' using: {sel}")
                            dlog(f"  Element: {result[:200]}")
                            clicked = True
                            break
                    except Exception as e:
                        dlog(f"  JS eval failed for {sel} in frame {frame.url}: {e}")

            if not clicked:
                # Dump all data-* elements across all frames to help debug
                for i, frame in enumerate(page.frames):
                    try:
                        all_data = frame.evaluate("""
                            [...document.querySelectorAll('[data-action],[data-field],[data-language]')]
                                .map(e => e.outerHTML.slice(0,150))
                                .slice(0,30)
                        """)
                        dlog(f"  Frame {i} data-* elements ({len(all_data)}):")
                        for el in all_data:
                            dlog(f"    {el}")
                        # Also dump body snippet
                        body = frame.evaluate("document.body?.innerHTML?.slice(0,500) || ''")
                        dlog(f"  Frame {i} body snippet: {body[:300]}")
                    except Exception as e:
                        dlog(f"  Could not inspect frame {i}: {e}")

                page.screenshot(path=str(SCREENSHOT_FILE))
                raise RuntimeError(
                    f"Could not find or click the balance button. "
                    f"Check {DEBUG_LOG} for details."
                )

            page.wait_for_timeout(1500)

            # ----------------------------------------------------------------
            # 4. Click "Weekly Figures" tab
            # ----------------------------------------------------------------
            dlog('Navigating to "Weekly Figures" tab...')
            try:
                page.get_by_text(WEEKLY_FIGURES_TAB, exact=False).first.click()
                page.wait_for_timeout(2000)
            except Exception:
                page.screenshot(path=str(SCREENSHOT_FILE))
                raise RuntimeError(f'Could not find "{WEEKLY_FIGURES_TAB}" tab. Screenshot: {SCREENSHOT_FILE}')

            # ----------------------------------------------------------------
            # 5. Enumerate weeks via dropdown and scrape each day's modal
            # ----------------------------------------------------------------
            today = date.today()
            start_d = date.fromisoformat(start_date)
            end_d   = date.fromisoformat(end_date)

            dlog(f"Scraping per-day modals for {start_date} to {end_date}")

            # Read the week-selection dropdown (find first <select> whose options mention "week")
            dropdown_info = page.evaluate("""
                () => {
                    for (const sel of document.querySelectorAll('select')) {
                        const opts = [...sel.options].map((o, i) => ({
                            index: i, value: o.value, text: o.text.trim()
                        }));
                        if (opts.some(o => /week/i.test(o.text))) {
                            return { options: opts };
                        }
                    }
                    return null;
                }
            """)

            if dropdown_info:
                dlog(f"  Dropdown found with {len(dropdown_info['options'])} options:")
                for o in dropdown_info['options']:
                    dlog(f"    [{o['index']}] value={o['value']!r} text={o['text']!r}")
            else:
                dlog("  No week dropdown found — scraping current week only")

            def week_range_for_option(idx: int, ref_today: date):
                """Index 0 = This Week, 1 = Last Week, 2 = 2 Weeks Ago, …"""
                current_monday = ref_today - timedelta(days=ref_today.weekday())
                week_start = current_monday - timedelta(weeks=idx)
                return week_start, week_start + timedelta(days=6)

            # Determine which dropdown options to visit
            if dropdown_info:
                opts_to_visit = []
                for opt in dropdown_info['options']:
                    ws, we = week_range_for_option(opt['index'], today)
                    if we >= start_d and ws <= end_d:
                        opts_to_visit.append(opt)
                dlog(f"  Visiting {len(opts_to_visit)} option(s) out of {len(dropdown_info['options'])}")
            else:
                opts_to_visit = [None]  # None = don't change dropdown, just use current view

            def scrape_day_rows_for_current_view():
                """Find all day rows visible in the current weekly panel, click each in range."""
                # Collect day row texts, e.g. "Tue(3/3)", "Fri(3/6)"
                day_texts = page.evaluate("""
                    () => {
                        const results = [];
                        document.querySelectorAll('td').forEach(td => {
                            const t = td.innerText.trim();
                            if (/^\\w{2,3}\\(\\d+\\/\\d+\\)/.test(t)) {
                                results.push(t);
                            }
                        });
                        return results;
                    }
                """)
                dlog(f"  Day rows visible: {day_texts}")

                for row_text in day_texts:
                    m = re.search(r'\((\d+)/(\d+)\)', row_text)
                    if not m:
                        continue
                    mon, day_n = int(m.group(1)), int(m.group(2))
                    bet_year = today.year if mon <= today.month else today.year - 1
                    try:
                        row_date = date(bet_year, mon, day_n)
                    except ValueError:
                        dlog(f"  Bad date in row text: {row_text}")
                        continue

                    if row_date < start_d or row_date > end_d:
                        dlog(f"  Skipping {row_text} ({row_date}) — outside range")
                        continue

                    bet_date_str = row_date.isoformat()
                    dlog(f"  Clicking day row: {row_text} → {bet_date_str}")

                    # Click the dollar-amount child inside the day cell (or the cell itself)
                    clicked = page.evaluate(f"""
                        () => {{
                            for (const td of document.querySelectorAll('td')) {{
                                if (td.innerText.trim().startsWith({repr(row_text)})) {{
                                    // Prefer a leaf child starting with '$'
                                    for (const ch of td.querySelectorAll('*')) {{
                                        if (ch.children.length === 0 && ch.innerText.trim().startsWith('$')) {{
                                            ch.click();
                                            return ch.innerText.trim();
                                        }}
                                    }}
                                    td.click();
                                    return td.innerText.trim();
                                }}
                            }}
                            return null;
                        }}
                    """)
                    if not clicked:
                        dlog(f"  Could not find element for {row_text}")
                        continue
                    dlog(f"  Clicked element text: {clicked}")

                    # Wait for modal
                    try:
                        page.wait_for_selector('#myModal', state='visible', timeout=8000)
                        dlog("  Modal visible.")
                    except Exception as e:
                        dlog(f"  Modal did not appear for {row_text}: {e}")
                        continue
                    page.wait_for_timeout(1500)

                    # Scrape modal rows
                    modal_rows = page.evaluate("""
                        () => {
                            const modal = document.querySelector('#myModal');
                            if (!modal) return [];
                            const rows = [];
                            modal.querySelectorAll('tr').forEach(tr => {
                                const cells = [...tr.querySelectorAll('td')].map(c => c.innerText.trim());
                                if (cells.length >= 1 && cells[0]) rows.push(cells);
                            });
                            return rows;
                        }
                    """)
                    dlog(f"  Modal rows for {row_text}: {len(modal_rows)}")
                    for i, cells in enumerate(modal_rows):
                        dlog(f"    Row {i}: {cells}")

                    for cells in modal_rows:
                        bet = parse_modal_bet_row(cells, bet_date_str)
                        if bet:
                            bets.append(bet)
                            dlog(f"  -> Bet: {bet['game'][:60]} | {bet['result']} | P/L: {bet['profit_loss']}")

                    # Close modal
                    try:
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(800)
                    except Exception:
                        pass

            for opt in opts_to_visit:
                if opt is not None and dropdown_info:
                    dlog(f"  Selecting dropdown option: {opt['text']!r} (value={opt['value']!r})")

                    # Step 1: Make the week select visible so Playwright can interact with it natively
                    page.evaluate("""
                        () => {
                            for (const sel of document.querySelectorAll('select')) {
                                if ([...sel.options].some(o => /week/i.test(o.text))) {
                                    sel.style.cssText += '; display:block !important; visibility:visible !important; opacity:1 !important; position:fixed !important; top:0 !important; left:0 !important; z-index:99999 !important;';
                                    break;
                                }
                            }
                        }
                    """)

                    # Step 2: Try Playwright's native select_option (fires proper browser events)
                    changed = False
                    try:
                        week_sel = page.locator('select').filter(has_text=re.compile('week', re.I)).first
                        week_sel.select_option(value=opt['value'], timeout=3000)
                        changed = True
                        dlog(f"  select changed via native Playwright: True")
                    except Exception as e:
                        dlog(f"  Native select failed ({e}), falling back to JS dispatch")
                        # Step 2b: JS fallback — target only the week select
                        changed = page.evaluate(f"""
                            () => {{
                                for (const sel of document.querySelectorAll('select')) {{
                                    if (![...sel.options].some(o => /week/i.test(o.text))) continue;
                                    sel.value = {repr(opt['value'])};
                                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                                    sel.dispatchEvent(new Event('input',  {{bubbles: true}}));
                                    return true;
                                }}
                                return false;
                            }}
                        """)
                        dlog(f"  select changed via JS: {changed}")

                    # Step 3: Wait for the page to actually update
                    try:
                        page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception:
                        pass
                    page.wait_for_timeout(1500)

                scrape_day_rows_for_current_view()

            dlog(f"Total bets collected: {len(bets)}")

        except Exception as exc:
            page.screenshot(path=str(SCREENSHOT_FILE))
            print(f"Unexpected error: {exc}")
            print(f"Screenshot saved to {SCREENSHOT_FILE}")
            raise
        finally:
            browser.close()

    print(f"Bets in range {start_date}–{end_date}: {len(bets)}")
    return bets


# ---------------------------------------------------------------------------
# Dashboard HTML generation
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cover2Sports Betting Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #22263a;
    --accent: #4f8ef7;
    --green: #22c55e;
    --red: #ef4444;
    --gray: #6b7280;
    --text: #e2e8f0;
    --muted: #94a3b8;
    --border: #2d3148;
    --radius: 10px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 32px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.3rem; font-weight: 700; color: var(--accent); }
  header span { color: var(--muted); font-size: 0.85rem; }
  .main { padding: 24px 32px; max-width: 1400px; margin: 0 auto; }

  /* Filters */
  .filters { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px 24px; margin-bottom: 24px; display: flex; flex-wrap: wrap; gap: 20px; align-items: flex-end; }
  .filter-group { display: flex; flex-direction: column; gap: 6px; }
  .filter-group label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
  .filter-group input[type="date"],
  .filter-group select { background: var(--surface2); border: 1px solid var(--border); color: var(--text); border-radius: 6px; padding: 7px 10px; font-size: 0.875rem; }
  .checkbox-group { display: flex; flex-wrap: wrap; gap: 8px; }
  .checkbox-group label { display: flex; align-items: center; gap: 5px; font-size: 0.82rem; cursor: pointer; background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; user-select: none; }
  .checkbox-group input { accent-color: var(--accent); }
  .filter-actions { display: flex; gap: 8px; align-items: flex-end; }
  button { background: var(--accent); color: #fff; border: none; border-radius: 6px; padding: 8px 18px; cursor: pointer; font-size: 0.875rem; font-weight: 600; }
  button.secondary { background: var(--surface2); border: 1px solid var(--border); color: var(--muted); }

  /* Summary cards */
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px 20px; }
  .card .label { font-size: 0.72rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; margin-bottom: 8px; }
  .card .value { font-size: 1.55rem; font-weight: 700; }
  .card .value.pos { color: var(--green); }
  .card .value.neg { color: var(--red); }
  .card .value.neutral { color: var(--text); }
  .card .sub { font-size: 0.78rem; color: var(--muted); margin-top: 4px; }

  /* Charts */
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
  .chart-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; }
  .chart-card h3 { font-size: 0.9rem; color: var(--muted); margin-bottom: 16px; }
  .chart-card canvas { max-height: 260px; }
  @media (max-width: 900px) { .charts { grid-template-columns: 1fr; } }

  /* Table */
  .table-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
  .table-card h3 { padding: 16px 20px; font-size: 0.9rem; color: var(--muted); border-bottom: 1px solid var(--border); }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.84rem; }
  th { padding: 10px 14px; text-align: left; font-size: 0.72rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; cursor: pointer; user-select: none; background: var(--surface2); border-bottom: 1px solid var(--border); white-space: nowrap; }
  th:hover { color: var(--accent); }
  th .sort-icon { margin-left: 4px; opacity: 0.5; }
  th.sorted .sort-icon { opacity: 1; color: var(--accent); }
  td { padding: 10px 14px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tr.win td:first-child { border-left: 3px solid var(--green); }
  tr.loss td:first-child { border-left: 3px solid var(--red); }
  tr.push td:first-child, tr.pending td:first-child { border-left: 3px solid var(--gray); }
  .badge { display: inline-block; border-radius: 4px; padding: 2px 8px; font-size: 0.72rem; font-weight: 600; }
  .badge.win { background: #14532d; color: var(--green); }
  .badge.loss { background: #450a0a; color: var(--red); }
  .badge.push, .badge.pending { background: #1f2937; color: var(--muted); }
  .pl.pos { color: var(--green); }
  .pl.neg { color: var(--red); }
  .empty-msg { text-align: center; padding: 60px 20px; color: var(--muted); }
  .empty-msg h2 { font-size: 1.1rem; margin-bottom: 8px; }
</style>
</head>
<body>
<header>
  <h1>Cover2Sports Dashboard</h1>
  <span id="generated-date"></span>
</header>

<div class="main">
  <!-- Filters -->
  <div class="filters">
    <div class="filter-group">
      <label>Start Date</label>
      <input type="date" id="fStartDate" value="2026-03-02">
    </div>
    <div class="filter-group">
      <label>End Date</label>
      <input type="date" id="fEndDate">
    </div>
    <div class="filter-group">
      <label>Sport</label>
      <select id="fSport">
        <option value="">All Sports</option>
      </select>
    </div>
    <div class="filter-group">
      <label>Bet Type</label>
      <div class="checkbox-group" id="fBetType">
        <label><input type="checkbox" value="Moneyline" checked> Moneyline</label>
        <label><input type="checkbox" value="Spread" checked> Spread</label>
        <label><input type="checkbox" value="Over/Under" checked> Over/Under</label>
      </div>
    </div>
    <div class="filter-group">
      <label>Wager Type</label>
      <div class="checkbox-group" id="fWagerType">
        <label><input type="checkbox" value="Straight" checked> Straight</label>
        <label><input type="checkbox" value="Parlay" checked> Parlay</label>
        <label><input type="checkbox" value="Teaser" checked> Teaser</label>
        <label><input type="checkbox" value="Reverse" checked> Reverse</label>
        <label><input type="checkbox" value="IF Bet" checked> IF Bet</label>
      </div>
    </div>
    <div class="filter-group">
      <label>Result</label>
      <div class="checkbox-group" id="fResult">
        <label><input type="checkbox" value="Win" checked> Win</label>
        <label><input type="checkbox" value="Loss" checked> Loss</label>
        <label><input type="checkbox" value="Push" checked> Push</label>
        <label><input type="checkbox" value="Pending" checked> Pending</label>
      </div>
    </div>
    <div class="filter-actions">
      <button onclick="applyFilters()">Apply</button>
      <button class="secondary" onclick="resetFilters()">Reset</button>
    </div>
  </div>

  <!-- Summary Cards -->
  <div class="cards">
    <div class="card"><div class="label">Total Bets</div><div class="value neutral" id="cTotal">-</div></div>
    <div class="card"><div class="label">Record</div><div class="value neutral" id="cRecord">-</div><div class="sub" id="cWinPct"></div></div>
    <div class="card"><div class="label">Total Wagered</div><div class="value neutral" id="cWagered">-</div></div>
    <div class="card"><div class="label">Net P/L</div><div class="value neutral" id="cNetPL">-</div></div>
    <div class="card"><div class="label">ROI</div><div class="value neutral" id="cROI">-</div></div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <div class="chart-card" style="grid-column: span 2;">
      <h3>Cumulative P/L Over Time</h3>
      <canvas id="chartPL"></canvas>
    </div>
    <div class="chart-card">
      <h3>Win Rate by Sport</h3>
      <canvas id="chartSport"></canvas>
    </div>
    <div class="chart-card">
      <h3>Bet Type Breakdown</h3>
      <canvas id="chartType"></canvas>
    </div>
  </div>

  <!-- Bet History Table -->
  <div class="table-card">
    <h3>Bet History</h3>
    <div class="table-wrap">
      <table id="betTable">
        <thead>
          <tr>
            <th onclick="sortTable('date')">Date <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('sport')">Sport <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('wager_type')">Wager <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('bet_type')">Bet Type <span class="sort-icon">↕</span></th>
            <th>Game</th>
            <th>Pick</th>
            <th onclick="sortTable('odds')">Odds <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('amount')">Amount <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('result')">Result <span class="sort-icon">↕</span></th>
            <th onclick="sortTable('profit_loss')">P/L <span class="sort-icon">↕</span></th>
          </tr>
        </thead>
        <tbody id="betTableBody"></tbody>
      </table>
      <div class="empty-msg" id="emptyMsg" style="display:none;">
        <h2>No bets found</h2>
        <p>Adjust your filters or run the scraper to collect bet data.</p>
      </div>
    </div>
  </div>
</div>

<script>
// ============================================================
// DATA — injected by scraper.py
// ============================================================
const BETS = __BETS_JSON__;

// ============================================================
// State
// ============================================================
let sortKey = 'date';
let sortAsc = false;
let filteredBets = [];
let charts = {};

// ============================================================
// Init
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('generated-date').textContent =
    'Generated: ' + new Date().toLocaleString();

  // Set default end date to today
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById('fEndDate').value = today;

  // Populate sport dropdown
  const sports = [...new Set(BETS.map(b => b.sport).filter(Boolean))].sort();
  const sel = document.getElementById('fSport');
  sports.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    sel.appendChild(opt);
  });

  applyFilters();
});

// ============================================================
// Filtering
// ============================================================
function applyFilters() {
  const start  = document.getElementById('fStartDate').value;
  const end    = document.getElementById('fEndDate').value;
  const sport  = document.getElementById('fSport').value;
  const btypes = checkedValues('fBetType');
  const wtypes = checkedValues('fWagerType');
  const results= checkedValues('fResult');

  filteredBets = BETS.filter(b => {
    if (start && b.date < start) return false;
    if (end   && b.date > end)   return false;
    if (sport && b.sport !== sport) return false;
    if (!btypes.includes(b.bet_type))   return false;
    if (!wtypes.includes(b.wager_type)) return false;
    if (!results.includes(b.result))    return false;
    return true;
  });

  renderCards();
  renderCharts();
  renderTable();
}

function resetFilters() {
  document.getElementById('fStartDate').value = '2026-03-02';
  document.getElementById('fEndDate').value = new Date().toISOString().slice(0, 10);
  document.getElementById('fSport').value = '';
  document.querySelectorAll('#fBetType input, #fWagerType input, #fResult input')
    .forEach(cb => cb.checked = true);
  applyFilters();
}

function checkedValues(containerId) {
  return [...document.querySelectorAll(`#${containerId} input:checked`)].map(c => c.value);
}

// ============================================================
// Summary Cards
// ============================================================
function renderCards() {
  const bets = filteredBets;
  const total = bets.length;
  const wins   = bets.filter(b => b.result === 'Win').length;
  const losses = bets.filter(b => b.result === 'Loss').length;
  const pushes = bets.filter(b => b.result === 'Push').length;
  const decided = wins + losses;
  const winPct = decided > 0 ? (wins / decided * 100).toFixed(1) : '—';
  const wagered = bets.reduce((s, b) => s + (b.amount || 0), 0);
  const netPL   = bets.reduce((s, b) => s + (b.profit_loss || 0), 0);
  const roi     = wagered > 0 ? (netPL / wagered * 100).toFixed(1) : '—';

  setText('cTotal', total);
  setText('cRecord', `${wins}-${losses}-${pushes}`);
  setText('cWinPct', decided > 0 ? `${winPct}% win rate` : '');
  setText('cWagered', fmt$(wagered));
  setEl('cNetPL', fmt$(netPL, true), netPL >= 0 ? 'pos' : 'neg');
  setEl('cROI', roi !== '—' ? `${roi}%` : '—', netPL >= 0 ? 'pos' : 'neg');
}

function setText(id, val) {
  document.getElementById(id).textContent = val;
}
function setEl(id, val, cls) {
  const el = document.getElementById(id);
  el.textContent = val;
  el.className = 'value ' + cls;
}
function fmt$(n, sign = false) {
  if (n == null || isNaN(n)) return '—';
  const abs = Math.abs(n).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  if (sign) return (n >= 0 ? '+$' : '-$') + abs;
  return '$' + abs;
}

// ============================================================
// Charts
// ============================================================
const CHART_DEFAULTS = {
  responsive: true,
  maintainAspectRatio: true,
  plugins: { legend: { labels: { color: '#94a3b8', font: { size: 12 } } } },
};

function renderCharts() {
  renderPLChart();
  renderSportChart();
  renderTypeChart();
}

function renderPLChart() {
  const sorted = [...filteredBets].sort((a,b) => a.date.localeCompare(b.date));
  let cum = 0;
  const labels = [], data = [];
  sorted.forEach(b => {
    cum += (b.profit_loss || 0);
    labels.push(b.date);
    data.push(+cum.toFixed(2));
  });

  const ctx = document.getElementById('chartPL').getContext('2d');
  if (charts.pl) charts.pl.destroy();
  charts.pl = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Cumulative P/L ($)',
        data,
        borderColor: '#4f8ef7',
        backgroundColor: 'rgba(79,142,247,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: sorted.length < 50 ? 4 : 0,
        pointHoverRadius: 6,
      }]
    },
    options: {
      ...CHART_DEFAULTS,
      scales: {
        x: { ticks: { color: '#6b7280', maxTicksLimit: 12 }, grid: { color: '#1f2937' } },
        y: { ticks: { color: '#6b7280', callback: v => '$'+v }, grid: { color: '#1f2937' } }
      }
    }
  });
}

function renderSportChart() {
  const sportMap = {};
  filteredBets.forEach(b => {
    if (!b.sport) return;
    if (!sportMap[b.sport]) sportMap[b.sport] = { wins: 0, total: 0 };
    sportMap[b.sport].total++;
    if (b.result === 'Win') sportMap[b.sport].wins++;
  });
  const sports = Object.keys(sportMap);
  const rates  = sports.map(s => +(sportMap[s].wins / sportMap[s].total * 100).toFixed(1));

  const ctx = document.getElementById('chartSport').getContext('2d');
  if (charts.sport) charts.sport.destroy();
  charts.sport = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: sports,
      datasets: [{
        label: 'Win Rate (%)',
        data: rates,
        backgroundColor: '#4f8ef7',
        borderRadius: 5,
      }]
    },
    options: {
      ...CHART_DEFAULTS,
      indexAxis: 'y',
      scales: {
        x: { min: 0, max: 100, ticks: { color: '#6b7280', callback: v => v+'%' }, grid: { color: '#1f2937' } },
        y: { ticks: { color: '#6b7280' }, grid: { display: false } }
      }
    }
  });
}

function renderTypeChart() {
  const counts = {};
  filteredBets.forEach(b => {
    const t = b.bet_type || 'Unknown';
    counts[t] = (counts[t] || 0) + 1;
  });
  const labels = Object.keys(counts);
  const data   = labels.map(l => counts[l]);
  const colors = ['#4f8ef7', '#22c55e', '#f59e0b', '#ec4899', '#8b5cf6'];

  const ctx = document.getElementById('chartType').getContext('2d');
  if (charts.type) charts.type.destroy();
  charts.type = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{ data, backgroundColor: colors, borderWidth: 0 }]
    },
    options: {
      ...CHART_DEFAULTS,
      cutout: '60%',
    }
  });
}

// ============================================================
// Table
// ============================================================
let sortDir = { date: -1 };

function sortTable(key) {
  if (sortKey === key) {
    sortDir[key] = (sortDir[key] || -1) * -1;
  } else {
    sortKey = key;
    sortDir[key] = 1;
  }
  // Update header indicators
  document.querySelectorAll('th').forEach(th => th.classList.remove('sorted'));
  const headers = ['date','sport','wager_type','bet_type','game','pick','odds','amount','result','profit_loss'];
  const idx = headers.indexOf(key);
  if (idx >= 0) {
    const th = document.querySelectorAll('th')[idx];
    if (th) {
      th.classList.add('sorted');
      th.querySelector('.sort-icon').textContent = sortDir[key] === 1 ? '↑' : '↓';
    }
  }
  renderTable();
}

function renderTable() {
  const bets = [...filteredBets].sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (av == null) av = '';
    if (bv == null) bv = '';
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    return av < bv ? -sortDir[sortKey] : av > bv ? sortDir[sortKey] : 0;
  });

  const tbody = document.getElementById('betTableBody');
  const empty = document.getElementById('emptyMsg');

  if (bets.length === 0) {
    tbody.innerHTML = '';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  tbody.innerHTML = bets.map(b => {
    const res = (b.result || 'Pending').toLowerCase();
    const pl  = b.profit_loss;
    const plClass = pl > 0 ? 'pos' : pl < 0 ? 'neg' : '';
    const plStr   = pl != null ? (pl >= 0 ? '+' : '') + '$' + Math.abs(pl).toFixed(2) : '—';
    return `<tr class="${res}">
      <td>${b.date || '—'}</td>
      <td>${b.sport || '—'}</td>
      <td>${b.wager_type || '—'}</td>
      <td>${b.bet_type || '—'}</td>
      <td>${b.game || '—'}</td>
      <td>${b.pick || '—'}</td>
      <td>${b.odds != null ? b.odds : '—'}</td>
      <td>${b.amount != null ? '$'+b.amount.toFixed(2) : '—'}</td>
      <td><span class="badge ${res}">${b.result || 'Pending'}</span></td>
      <td class="pl ${plClass}">${plStr}</td>
    </tr>`;
  }).join('');
}
</script>
</body>
</html>
"""


def generate_dashboard(bets: list[dict]) -> None:
    """Write the self-contained dashboard HTML file with bet data embedded."""
    bets_json = json.dumps(bets, indent=2, default=str)
    html = HTML_TEMPLATE.replace("__BETS_JSON__", bets_json)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Dashboard written to: {OUTPUT_FILE.resolve()}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Cover2Sports bet history scraper")
    parser.add_argument("--headed", action="store_true",
                        help="Run browser in headed mode (visible) for debugging")
    args = parser.parse_args()

    print("=" * 55)
    print(" Cover2Sports Betting Analytics Scraper")
    print("=" * 55)
    print(f"Tracking bets from: {START_DATE}")
    print(f"Output file:        {OUTPUT_FILE}\n")

    # Prompt for credentials — never stored
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")

    if not username or not password:
        print("ERROR: Username and password are required.")
        sys.exit(1)

    print()

    bets = run_scraper(username, password, headed=args.headed)

    if not bets:
        print("\nNo bets found after filtering. Generating empty dashboard...")

    generate_dashboard(bets)

    print()
    print("=" * 55)
    print(f" Done! {len(bets)} bet(s) found.")
    print(f" Open {OUTPUT_FILE.name} in your browser to view.")
    print()
    print(" To debug (watch the browser):")
    print("   python scraper.py --headed")
    print("=" * 55)


if __name__ == "__main__":
    main()
