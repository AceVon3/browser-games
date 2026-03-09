"""
Cover2Sports Betting Analytics — Web Server

Usage:
    pip install flask werkzeug
    python server.py
    Open http://localhost:5001
"""

import os
import sys
import json
import secrets
from datetime import date
from functools import wraps
from flask import Flask, request, jsonify, redirect, url_for, session
from werkzeug.security import generate_password_hash, check_password_hash

sys.path.insert(0, os.path.dirname(__file__))

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')


def load_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, 'w') as f:
        json.dump(cfg, f, indent=2)


app = Flask(__name__)
app.secret_key = secrets.token_hex(32)  # temporary until config loads


@app.before_request
def before_each_request():
    # Load stable key from config so sessions survive server restarts
    cfg = load_config()
    if cfg and cfg.get('secret_key'):
        app.secret_key = cfg['secret_key']
    # Redirect to first-time setup when no config exists
    if request.endpoint not in ('setup', 'login', 'logout', 'static') and cfg is None:
        return redirect(url_for('setup'))


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cover2Sports Dashboard</title>
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
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 32px; display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 1.3rem; font-weight: 700; color: var(--accent); }
  .header-right { display: flex; align-items: center; gap: 16px; }
  header span { color: var(--muted); font-size: 0.85rem; }
  .sign-out-link { color: var(--muted); font-size: 0.85rem; text-decoration: none; }
  .sign-out-link:hover { color: var(--text); }
  .main { padding: 24px 32px; max-width: 1400px; margin: 0 auto; }

  /* Tabs */
  .tab-nav { display: flex; gap: 0; border-bottom: 2px solid var(--border); margin-bottom: 24px; }
  .tab-btn { background: none; border: none; border-bottom: 2px solid transparent; margin-bottom: -2px; padding: 10px 22px; font-size: 0.9rem; font-weight: 600; color: var(--muted); cursor: pointer; transition: color 0.15s, border-color 0.15s; }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }

  /* Profile tab */
  .profile-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 24px; }
  .profile-card h2 { font-size: 1rem; color: var(--text); margin-bottom: 20px; }
  .profile-section { margin-bottom: 28px; }
  .profile-section h3 { font-size: 0.78rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; margin-bottom: 14px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
  .profile-body { display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end; }
  .profile-body .field { display: flex; flex-direction: column; gap: 6px; }
  .profile-body .field label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
  .profile-body input[type="text"],
  .profile-body input[type="password"] { background: var(--surface2); border: 1px solid var(--border); color: var(--text); border-radius: 6px; padding: 8px 12px; font-size: 0.875rem; width: 240px; }
  .profile-body input:focus { outline: 1px solid var(--accent); }
  .profile-badge { color: var(--green); font-size: 0.82rem; font-weight: 600; }
  .profile-status { font-size: 0.83rem; color: var(--muted); }

  /* Credentials */
  .creds-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px 24px; margin-bottom: 20px; display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end; }
  .creds-card .field { display: flex; flex-direction: column; gap: 6px; }
  .creds-card .field label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
  .creds-card input[type="text"],
  .creds-card input[type="password"],
  .creds-card input[type="date"] { background: var(--surface2); border: 1px solid var(--border); color: var(--text); border-radius: 6px; padding: 8px 12px; font-size: 0.875rem; width: 210px; }
  .creds-card input:focus { outline: 1px solid var(--accent); }
  .headed-label { display: flex; align-items: center; gap: 6px; font-size: 0.82rem; color: var(--muted); cursor: pointer; padding-bottom: 2px; }
  .headed-label input { accent-color: var(--accent); }
  .btn-primary { background: var(--accent); color: #fff; border: none; border-radius: 6px; padding: 9px 22px; cursor: pointer; font-size: 0.9rem; font-weight: 600; white-space: nowrap; }
  .btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-secondary { background: var(--surface2); border: 1px solid var(--border); color: var(--muted); border-radius: 6px; padding: 8px 18px; cursor: pointer; font-size: 0.875rem; font-weight: 600; }

  /* Status bar */
  .status { border-radius: var(--radius); padding: 12px 18px; margin-bottom: 20px; font-size: 0.875rem; display: flex; align-items: center; gap: 10px; }
  .status.loading { background: #1e293b; border: 1px solid #334155; color: var(--muted); }
  .status.success { background: #14532d33; border: 1px solid #22c55e55; color: var(--green); }
  .status.error   { background: #450a0a33; border: 1px solid #ef444455; color: var(--red); }
  .spinner { width: 16px; height: 16px; border: 2px solid #334155; border-top-color: var(--accent); border-radius: 50%; animation: spin 0.7s linear infinite; flex-shrink: 0; }
  @keyframes spin { to { transform: rotate(360deg); } }

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
  <div class="header-right">
    <span id="lastUpdated"></span>
    <a href="/logout" class="sign-out-link">Sign out</a>
  </div>
</header>

<div class="main">
  <!-- Tab navigation -->
  <div class="tab-nav">
    <button class="tab-btn active" id="tabBtnDashboard" onclick="switchTab('dashboard')">Dashboard</button>
    <button class="tab-btn" id="tabBtnProfile" onclick="switchTab('profile')">Profile &amp; Settings <span id="profileSavedBadge" class="profile-badge"></span></button>
  </div>

  <!-- Profile tab -->
  <div id="tab-profile" class="tab-panel" style="display:none">
    <div class="profile-card">
      <div class="profile-section">
        <h3>Cover2Sports Credentials</h3>
        <div class="profile-body">
          <div class="field">
            <label>Username</label>
            <input type="text" id="profileC2SUser" placeholder="Cover2Sports username" autocomplete="off">
          </div>
          <div class="field">
            <label>Password</label>
            <input type="password" id="profileC2SPass" placeholder="Cover2Sports password" autocomplete="off">
          </div>
          <div style="display:flex;flex-direction:column;gap:8px;justify-content:flex-end;">
            <button class="btn-primary" onclick="saveProfile()">Save Credentials</button>
            <span id="profileStatus" class="profile-status"></span>
          </div>
        </div>
      </div>
      <div class="profile-section">
        <h3>Change Dashboard Password</h3>
        <div class="profile-body">
          <div class="field">
            <label>New Password</label>
            <input type="password" id="profileDashPass" placeholder="Leave blank to keep current">
          </div>
          <div style="display:flex;align-items:flex-end;">
            <button class="btn-primary" onclick="saveDashPassword()">Update Password</button>
          </div>
          <span id="profileDashStatus" class="profile-status" style="align-self:flex-end;"></span>
        </div>
      </div>
    </div>
  </div>

  <!-- Dashboard tab -->
  <div id="tab-dashboard" class="tab-panel">
  <!-- Credentials / Scrape -->
  <div class="creds-card">
    <div id="credFields" style="display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;">
      <div class="field">
        <label>Username</label>
        <input type="text" id="username" placeholder="Cover2Sports username" autocomplete="username">
      </div>
      <div class="field">
        <label>Password</label>
        <input type="password" id="password" placeholder="Password" autocomplete="current-password">
      </div>
    </div>
    <label class="headed-label">
      <input type="checkbox" id="headed"> Headed mode
    </label>
    <div class="field">
      <label>From Date</label>
      <input type="date" id="scrapeStart" value="2026-03-02">
    </div>
    <div class="field">
      <label>To Date</label>
      <input type="date" id="scrapeEnd">
    </div>
    <button class="btn-primary" id="scrapeBtn" onclick="runScraper()">Scrape &amp; Refresh</button>
  </div>

  <!-- Status bar -->
  <div class="status" id="statusBar" style="display:none"></div>

  <!-- Dashboard — hidden until first successful scrape -->
  <div id="dashSection" style="display:none">

    <!-- Filters -->
    <div class="filters">
      <div class="filter-group">
        <label>Start Date</label>
        <input type="date" id="fStartDate">
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
        <button class="btn-primary" onclick="applyFilters()">Apply</button>
        <button class="btn-secondary" onclick="resetFilters()">Reset</button>
      </div>
    </div>

    <!-- Summary cards -->
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

    <!-- Bet history table -->
    <div class="table-card">
      <h3>Bet History</h3>
      <div class="table-wrap">
        <table>
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
        <div class="empty-msg" id="emptyMsg" style="display:none">
          <h2>No bets found</h2>
          <p>Adjust your filters or click Scrape &amp; Refresh.</p>
        </div>
      </div>
    </div>

  </div><!-- /dashSection -->
  </div><!-- /tab-dashboard -->
</div><!-- /main -->

<script>
// ============================================================
// State
// ============================================================
let BETS = [];
let sortKey = 'date';
let sortDir = { date: -1 };
let filteredBets = [];
let charts = {};
let hasSavedCreds = false;

// ============================================================
// Init
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  const todayISO = new Date().toISOString().slice(0, 10);
  document.getElementById('fEndDate').value = todayISO;
  document.getElementById('scrapeEnd').value = todayISO;
  ['username', 'password'].forEach(id => {
    document.getElementById(id).addEventListener('keydown', e => {
      if (e.key === 'Enter') runScraper();
    });
  });
  loadProfile();
});

// ============================================================
// Tabs
// ============================================================
function switchTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.style.display = 'none');
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).style.display = '';
  document.getElementById('tabBtn' + name.charAt(0).toUpperCase() + name.slice(1)).classList.add('active');
}

// ============================================================
// Profile
// ============================================================
async function loadProfile() {
  const resp = await fetch('/api/profile');
  if (!resp.ok) return;
  const data = await resp.json();
  hasSavedCreds = !!data.has_cover2sports_creds;
  document.getElementById('credFields').style.display = hasSavedCreds ? 'none' : '';
  document.getElementById('profileSavedBadge').textContent = hasSavedCreds ? '\u2713' : '';
}

async function saveProfile() {
  const u = document.getElementById('profileC2SUser').value.trim();
  const p = document.getElementById('profileC2SPass').value.trim();
  const statusEl = document.getElementById('profileStatus');
  if (!u && !p) { statusEl.textContent = 'Nothing to save.'; return; }
  const payload = {};
  if (u) payload.cover2sports_username = u;
  if (p) payload.cover2sports_password = p;
  const resp = await fetch('/api/save-profile', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
  });
  const json = await resp.json();
  statusEl.textContent = json.ok ? 'Saved!' : (json.error || 'Error');
  if (json.ok) {
    document.getElementById('profileC2SUser').value = '';
    document.getElementById('profileC2SPass').value = '';
    await loadProfile();
  }
}

async function saveDashPassword() {
  const d = document.getElementById('profileDashPass').value;
  const statusEl = document.getElementById('profileDashStatus');
  if (!d) { statusEl.textContent = 'Enter a new password first.'; return; }
  const resp = await fetch('/api/save-profile', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({new_password: d})
  });
  const json = await resp.json();
  statusEl.textContent = json.ok ? 'Password updated!' : (json.error || 'Error');
  if (json.ok) document.getElementById('profileDashPass').value = '';
}

// ============================================================
// Scraper
// ============================================================
async function runScraper() {
  const username    = document.getElementById('username').value.trim();
  const password    = document.getElementById('password').value.trim();
  const headed      = document.getElementById('headed').checked;
  const scrapeStart = document.getElementById('scrapeStart').value;
  const scrapeEnd   = document.getElementById('scrapeEnd').value;

  if (!hasSavedCreds && (!username || !password)) {
    showStatus('error', 'Please enter your username and password.');
    return;
  }

  const btn = document.getElementById('scrapeBtn');
  btn.disabled = true;
  showStatus('loading', 'Launching browser and scraping bet history\u2026 this may take 30\u201360 seconds.');

  try {
    const resp = await fetch('/api/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, headed, start_date: scrapeStart, end_date: scrapeEnd }),
    });
    const json = await resp.json();

    if (!resp.ok || json.error) {
      showStatus('error', json.error || 'Unknown server error.');
      return;
    }

    BETS = json.bets || [];
    const now = new Date().toLocaleString();
    document.getElementById('lastUpdated').textContent = 'Updated: ' + now;
    showStatus('success', `Loaded ${BETS.length} bet(s) \u2014 last updated ${now}`);
    document.getElementById('dashSection').style.display = '';

    // Sync filter date range to the scraped range so all bets are visible
    document.getElementById('fStartDate').value = scrapeStart;
    document.getElementById('fEndDate').value = scrapeEnd;

    // Rebuild sport dropdown
    const sports = [...new Set(BETS.map(b => b.sport).filter(Boolean))].sort();
    const sel = document.getElementById('fSport');
    sel.innerHTML = '<option value="">All Sports</option>';
    sports.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s; opt.textContent = s;
      sel.appendChild(opt);
    });

    applyFilters();
  } catch (e) {
    showStatus('error', 'Network error: ' + e.message);
  } finally {
    btn.disabled = false;
  }
}

function showStatus(type, msg) {
  const bar = document.getElementById('statusBar');
  bar.style.display = '';
  bar.className = 'status ' + type;
  bar.innerHTML = type === 'loading'
    ? `<div class="spinner"></div><span>${msg}</span>`
    : `<span>${msg}</span>`;
}

// ============================================================
// Filtering
// ============================================================
function applyFilters() {
  const start   = document.getElementById('fStartDate').value;
  const end     = document.getElementById('fEndDate').value;
  const sport   = document.getElementById('fSport').value;
  const btypes  = checkedValues('fBetType');
  const wtypes  = checkedValues('fWagerType');
  const results = checkedValues('fResult');

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
  document.getElementById('fStartDate').value = '';
  document.getElementById('fEndDate').value = '';
  document.getElementById('fSport').value = '';
  document.querySelectorAll('#fBetType input, #fWagerType input, #fResult input')
    .forEach(cb => cb.checked = true);
  applyFilters();
}

function checkedValues(id) {
  return [...document.querySelectorAll(`#${id} input:checked`)].map(c => c.value);
}

// ============================================================
// Summary Cards
// ============================================================
function renderCards() {
  const bets    = filteredBets;
  const wins    = bets.filter(b => b.result === 'Win').length;
  const losses  = bets.filter(b => b.result === 'Loss').length;
  const pushes  = bets.filter(b => b.result === 'Push').length;
  const decided = wins + losses;
  const winPct  = decided > 0 ? (wins / decided * 100).toFixed(1) : '\u2014';
  const wagered = bets.reduce((s, b) => s + (b.amount || 0), 0);
  const netPL   = bets.reduce((s, b) => s + (b.profit_loss || 0), 0);
  const roi     = wagered > 0 ? (netPL / wagered * 100).toFixed(1) : '\u2014';

  setText('cTotal',   bets.length);
  setText('cRecord',  `${wins}-${losses}-${pushes}`);
  setText('cWinPct',  decided > 0 ? `${winPct}% win rate` : '');
  setText('cWagered', fmt$(wagered));
  setEl('cNetPL', fmt$(netPL, true), netPL >= 0 ? 'pos' : 'neg');
  setEl('cROI',   roi !== '\u2014' ? `${roi}%` : '\u2014', netPL >= 0 ? 'pos' : 'neg');
}

function setText(id, val) { document.getElementById(id).textContent = val; }
function setEl(id, val, cls) {
  const el = document.getElementById(id);
  el.textContent = val;
  el.className = 'value ' + cls;
}
function fmt$(n, sign = false) {
  if (n == null || isNaN(n)) return '\u2014';
  const abs = Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return sign ? (n >= 0 ? '+$' : '-$') + abs : '$' + abs;
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
  const sorted = [...filteredBets].sort((a, b) => a.date.localeCompare(b.date));
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
        y: { ticks: { color: '#6b7280', callback: v => '$' + v }, grid: { color: '#1f2937' } }
      }
    }
  });
}

function renderSportChart() {
  const m = {};
  filteredBets.forEach(b => {
    if (!b.sport) return;
    if (!m[b.sport]) m[b.sport] = { wins: 0, total: 0 };
    m[b.sport].total++;
    if (b.result === 'Win') m[b.sport].wins++;
  });
  const sports = Object.keys(m);
  const rates  = sports.map(s => +(m[s].wins / m[s].total * 100).toFixed(1));

  const ctx = document.getElementById('chartSport').getContext('2d');
  if (charts.sport) charts.sport.destroy();
  charts.sport = new Chart(ctx, {
    type: 'bar',
    data: { labels: sports, datasets: [{ label: 'Win Rate (%)', data: rates, backgroundColor: '#4f8ef7', borderRadius: 5 }] },
    options: {
      ...CHART_DEFAULTS,
      indexAxis: 'y',
      scales: {
        x: { min: 0, max: 100, ticks: { color: '#6b7280', callback: v => v + '%' }, grid: { color: '#1f2937' } },
        y: { ticks: { color: '#6b7280' }, grid: { display: false } }
      }
    }
  });
}

function renderTypeChart() {
  const counts = {};
  filteredBets.forEach(b => { const t = b.bet_type || 'Unknown'; counts[t] = (counts[t] || 0) + 1; });
  const labels = Object.keys(counts);
  const data   = labels.map(l => counts[l]);
  const colors = ['#4f8ef7', '#22c55e', '#f59e0b', '#ec4899', '#8b5cf6'];

  const ctx = document.getElementById('chartType').getContext('2d');
  if (charts.type) charts.type.destroy();
  charts.type = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 0 }] },
    options: { ...CHART_DEFAULTS, cutout: '60%' }
  });
}

// ============================================================
// Table
// ============================================================
function sortTable(key) {
  sortDir[key] = sortKey === key ? (sortDir[key] || -1) * -1 : 1;
  sortKey = key;
  document.querySelectorAll('th').forEach(th => th.classList.remove('sorted'));
  const headers = ['date', 'sport', 'wager_type', 'bet_type', 'game', 'pick', 'odds', 'amount', 'result', 'profit_loss'];
  const th = document.querySelectorAll('th')[headers.indexOf(key)];
  if (th) { th.classList.add('sorted'); th.querySelector('.sort-icon').textContent = sortDir[key] === 1 ? '\u2191' : '\u2193'; }
  renderTable();
}

function renderTable() {
  const bets = [...filteredBets].sort((a, b) => {
    let av = a[sortKey] ?? '', bv = b[sortKey] ?? '';
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    return av < bv ? -sortDir[sortKey] : av > bv ? sortDir[sortKey] : 0;
  });

  const tbody = document.getElementById('betTableBody');
  const empty = document.getElementById('emptyMsg');

  if (!bets.length) { tbody.innerHTML = ''; empty.style.display = ''; return; }
  empty.style.display = 'none';

  tbody.innerHTML = bets.map(b => {
    const res     = (b.result || 'Pending').toLowerCase();
    const pl      = b.profit_loss;
    const plClass = pl > 0 ? 'pos' : pl < 0 ? 'neg' : '';
    const plStr   = pl != null ? (pl >= 0 ? '+' : '') + '$' + Math.abs(pl).toFixed(2) : '\u2014';
    return `<tr class="${res}">
      <td>${b.date || '\u2014'}</td>
      <td>${b.sport || '\u2014'}</td>
      <td>${b.wager_type || '\u2014'}</td>
      <td>${b.bet_type || '\u2014'}</td>
      <td>${b.game || '\u2014'}</td>
      <td>${b.pick || '\u2014'}</td>
      <td>${b.odds != null ? b.odds : '\u2014'}</td>
      <td>${b.amount != null ? '$' + b.amount.toFixed(2) : '\u2014'}</td>
      <td><span class="badge ${res}">${b.result || 'Pending'}</span></td>
      <td class="pl ${plClass}">${plStr}</td>
    </tr>`;
  }).join('');
}
</script>
</body>
</html>"""


LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cover2Sports &mdash; Sign In</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f1117; color: #e2e8f0; font-family: system-ui, sans-serif; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
  .card {{ background: #1a1d27; border: 1px solid #2d3148; border-radius: 10px; padding: 36px 40px; width: 360px; }}
  h1 {{ color: #5b8dee; font-size: 1.3rem; margin-bottom: 24px; text-align: center; }}
  label {{ display: block; font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }}
  input[type="password"] {{ width: 100%; background: #22263a; border: 1px solid #2d3148; color: #e2e8f0; border-radius: 6px; padding: 9px 12px; font-size: 0.9rem; margin-bottom: 18px; }}
  input:focus {{ outline: 1px solid #5b8dee; }}
  button {{ width: 100%; background: #5b8dee; color: #fff; border: none; border-radius: 6px; padding: 10px; font-size: 0.95rem; font-weight: 600; cursor: pointer; }}
  .error {{ color: #ef4444; font-size: 0.85rem; margin-bottom: 14px; }}
</style>
</head>
<body>
<div class="card">
  <h1>Cover2Sports Dashboard</h1>
  {error}
  <form method="POST" action="/login">
    <label>Password</label>
    <input type="password" name="password" placeholder="Dashboard password" autofocus>
    <button type="submit">Sign In</button>
  </form>
</div>
</body>
</html>"""


SETUP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cover2Sports &mdash; Create Account</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f1117; color: #e2e8f0; font-family: system-ui, sans-serif; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
  .card {{ background: #1a1d27; border: 1px solid #2d3148; border-radius: 10px; padding: 36px 40px; width: 400px; }}
  h1 {{ color: #5b8dee; font-size: 1.3rem; margin-bottom: 8px; text-align: center; }}
  .desc {{ color: #94a3b8; font-size: 0.83rem; text-align: center; margin-bottom: 24px; }}
  label {{ display: block; font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }}
  input[type="password"] {{ width: 100%; background: #22263a; border: 1px solid #2d3148; color: #e2e8f0; border-radius: 6px; padding: 9px 12px; font-size: 0.9rem; margin-bottom: 18px; }}
  input:focus {{ outline: 1px solid #5b8dee; }}
  button {{ width: 100%; background: #5b8dee; color: #fff; border: none; border-radius: 6px; padding: 10px; font-size: 0.95rem; font-weight: 600; cursor: pointer; }}
  .error {{ color: #ef4444; font-size: 0.85rem; margin-bottom: 14px; }}
</style>
</head>
<body>
<div class="card">
  <h1>Create Dashboard Account</h1>
  <p class="desc">Choose a password to protect your dashboard. You only do this once.</p>
  {error}
  <form method="POST" action="/setup">
    <label>Password</label>
    <input type="password" name="password" placeholder="Choose a password" autofocus>
    <label>Confirm Password</label>
    <input type="password" name="confirm_password" placeholder="Repeat password">
    <button type="submit">Create Account</button>
  </form>
</div>
</body>
</html>"""


@app.route('/')
@login_required
def index():
    return DASHBOARD_HTML


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        cfg = load_config()
        pw  = request.form.get('password', '')
        if cfg and check_password_hash(cfg['dashboard_password_hash'], pw):
            session['logged_in'] = True
            return redirect(url_for('index'))
        return LOGIN_HTML.format(error='<p class="error">Incorrect password.</p>')
    return LOGIN_HTML.format(error='')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/setup', methods=['GET', 'POST'])
def setup():
    if load_config():
        return redirect(url_for('index'))
    if request.method == 'POST':
        pw  = request.form.get('password', '')
        pw2 = request.form.get('confirm_password', '')
        if not pw:
            return SETUP_HTML.format(error='<p class="error">Password cannot be empty.</p>')
        if pw != pw2:
            return SETUP_HTML.format(error='<p class="error">Passwords do not match.</p>')
        cfg = {
            'secret_key': secrets.token_hex(32),
            'dashboard_password_hash': generate_password_hash(pw),
            'cover2sports_username': '',
            'cover2sports_password': '',
        }
        save_config(cfg)
        session['logged_in'] = True
        return redirect(url_for('index'))
    return SETUP_HTML.format(error='')


@app.route('/api/profile')
@login_required
def api_profile():
    cfg = load_config() or {}
    return jsonify({'has_cover2sports_creds': bool(cfg.get('cover2sports_username'))})


@app.route('/api/save-profile', methods=['POST'])
@login_required
def api_save_profile():
    data = request.get_json(force=True)
    cfg  = load_config() or {}
    if data.get('cover2sports_username') is not None:
        cfg['cover2sports_username'] = data['cover2sports_username'].strip()
    if data.get('cover2sports_password') is not None:
        cfg['cover2sports_password'] = data['cover2sports_password'].strip()
    if data.get('new_password'):
        cfg['dashboard_password_hash'] = generate_password_hash(data['new_password'])
    save_config(cfg)
    return jsonify({'ok': True})


@app.route('/api/scrape', methods=['POST'])
@login_required
def api_scrape():
    print("=== /api/scrape called ===", flush=True)
    try:
        import importlib
        import scraper
        print("Reloading scraper...", flush=True)
        importlib.reload(scraper)
        run_scraper = scraper.run_scraper
        print("Scraper loaded OK.", flush=True)
    except Exception as e:
        print(f"Failed to load scraper: {e}", flush=True)
        return jsonify({'error': f'Failed to load scraper: {e}'}), 500

    data       = request.get_json(force=True)
    username   = (data.get('username')   or '').strip()
    password   = (data.get('password')   or '').strip()
    headed     = bool(data.get('headed', False))
    start_date = (data.get('start_date') or '').strip() or '2026-03-02'
    end_date   = (data.get('end_date')   or '').strip() or date.today().isoformat()

    if not username or not password:
        cfg = load_config() or {}
        username = cfg.get('cover2sports_username', '').strip()
        password = cfg.get('cover2sports_password', '').strip()

    if not username or not password:
        return jsonify({'error': 'Username and password are required.'}), 400

    try:
        bets = run_scraper(username, password, headed=headed,
                           start_date=start_date, end_date=end_date)
        return jsonify({'bets': bets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print('=' * 55)
    print(' Cover2Sports Dashboard Server')
    print('=' * 55)
    print(' Open http://localhost:5001 in your browser')
    print(' Press Ctrl+C to stop')
    print('=' * 55)
    app.run(host='127.0.0.1', port=5001, debug=False)
