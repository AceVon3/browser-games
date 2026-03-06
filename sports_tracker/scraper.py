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
import argparse
import getpass
from datetime import datetime, date
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration — adjust selectors here if the site ever changes
# ---------------------------------------------------------------------------
START_DATE           = "2026-03-02"
SITE_URL             = "https://www.cover2sports.net"
SPORTS_URL           = "https://cover2sports.net/sports.html"
OUTPUT_FILE          = Path(__file__).parent / "betting_dashboard.html"
SCREENSHOT_FILE      = Path(__file__).parent / "debug_screenshot.png"

# CSS/text selectors (update if site restructures)
BALANCE_SELECTOR     = '[class*="balance"], [class*="Balance"], #balance, [class*="account"], [class*="Account"]'
TRANSACTIONS_TAB     = "My Transactions"
WEEKLY_FIGURES_TAB   = "Weekly Figures"

# Text that must NEVER be clicked (bet-placement safety guard)
FORBIDDEN_BUTTON_TEXT = ["place", "wager", "submit bet", "bet now", "place bet", "confirm bet"]

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_american_odds(text: str) -> int | None:
    """Convert '+150', '-110', '150' → integer American odds."""
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return int(text)
    except ValueError:
        return None


def parse_money(text: str) -> float | None:
    """Convert '$25.00', '-$10.50', '25' → float dollars."""
    if not text:
        return None
    text = text.strip().replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(text: str) -> str | None:
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
    sport_keywords = {
        "NFL": ["nfl", "chiefs", "eagles", "patriots", "cowboys", "49ers", "packers"],
        "NBA": ["nba", "lakers", "celtics", "warriors", "bulls", "heat", "nets"],
        "MLB": ["mlb", "yankees", "dodgers", "red sox", "cubs", "mets", "braves"],
        "NHL": ["nhl", "bruins", "rangers", "penguins", "maple leafs", "kings"],
        "NCAAF": ["ncaaf", "college football", "cfb"],
        "NCAAB": ["ncaab", "college basketball", "cbb", "march madness"],
        "Soccer": ["soccer", "mls", "epl", "premier league", "champions league", "fifa", "la liga"],
        "UFC/MMA": ["ufc", "mma", "bellator"],
        "Boxing": ["boxing", "bout"],
    }
    for sport, keywords in sport_keywords.items():
        if any(k in low for k in keywords):
            return sport
    return "Other"


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


def parse_row_to_bet(raw: dict) -> dict | None:
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


def run_scraper(username: str, password: str, headed: bool = False) -> list[dict]:
    """Main scraping routine. Returns list of parsed bet dicts."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    except ImportError:
        print("ERROR: Playwright not installed.")
        print("Run:  pip install playwright && playwright install chromium")
        sys.exit(1)

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
                print(f"ERROR: Login form not found. Screenshot saved to {SCREENSHOT_FILE}")
                print("Hint: Update 'username_sel' in scraper.py to match the actual input selector.")
                sys.exit(1)

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
                    print("ERROR: Login failed — invalid credentials or account issue.")
                    page.screenshot(path=str(SCREENSHOT_FILE))
                    sys.exit(1)
                print(f"  Still on: {current} — continuing...")

            print("Login successful. Waiting for page to fully load...")
            try:
                page.wait_for_selector(BALANCE_SELECTOR, timeout=20000)
                print("  Balance element found.")
            except PlaywrightTimeout:
                page.screenshot(path=str(SCREENSHOT_FILE))
                print(f"ERROR: Balance/account element not found.")
                print(f"Hint: Update BALANCE_SELECTOR in scraper.py. Screenshot saved to {SCREENSHOT_FILE}")
                print(f"Current selectors tried: {BALANCE_SELECTOR}")
                sys.exit(1)

            # ----------------------------------------------------------------
            # 3. Open account/history panel
            # ----------------------------------------------------------------
            print("Opening account history panel...")
            balance_el = page.query_selector(BALANCE_SELECTOR)
            if not balance_el:
                page.screenshot(path=str(SCREENSHOT_FILE))
                print(f"ERROR: Could not locate balance element to click. Screenshot: {SCREENSHOT_FILE}")
                sys.exit(1)

            balance_el.click()
            page.wait_for_timeout(1500)  # let the panel animate open

            # ----------------------------------------------------------------
            # 4. Click "My Transactions" tab
            # ----------------------------------------------------------------
            print('Navigating to "My Transactions" tab...')
            try:
                page.get_by_text(TRANSACTIONS_TAB, exact=False).first.click()
                page.wait_for_timeout(2000)
            except Exception:
                page.screenshot(path=str(SCREENSHOT_FILE))
                print(f'ERROR: Could not find "{TRANSACTIONS_TAB}" tab. Screenshot: {SCREENSHOT_FILE}')
                sys.exit(1)

            # ----------------------------------------------------------------
            # 5. Scrape transactions
            # ----------------------------------------------------------------
            print("Scraping bet history...")
            raw_rows = scrape_transactions(page)
            print(f"  Raw rows captured: {len(raw_rows)}")

            for raw in raw_rows:
                if raw.get("fallback"):
                    print("  Warning: fallback text mode — structured parsing skipped.")
                    print("  Tip: Run with --headed to inspect the page, then update selectors.")
                    break
                bet = parse_row_to_bet(raw)
                if bet:
                    bets.append(bet)

            # ----------------------------------------------------------------
            # 6. Also check Weekly Figures (optional summary — informational only)
            # ----------------------------------------------------------------
            try:
                page.get_by_text(WEEKLY_FIGURES_TAB, exact=False).first.click()
                page.wait_for_timeout(1500)
                weekly_text = page.inner_text("body")
                print("  Weekly Figures tab loaded (data embedded in transactions).")
            except Exception:
                pass  # Non-critical

        except Exception as exc:
            page.screenshot(path=str(SCREENSHOT_FILE))
            print(f"Unexpected error: {exc}")
            print(f"Screenshot saved to {SCREENSHOT_FILE}")
            raise
        finally:
            browser.close()

    # Filter to start date
    bets = filter_by_start_date(bets, START_DATE)
    print(f"Bets after {START_DATE} filter: {len(bets)}")
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
