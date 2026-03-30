"""
fetch.py — Data fetching module.

Pulls data from Statcast (pybaseball), MLB Stats API, The Odds API,
and OpenWeatherMap. All external I/O lives here.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import requests
import statsapi
import pandas as pd
from pybaseball import statcast_pitcher, statcast_batter
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("pipeline")

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Hardcoded park factors (FanGraphs scale: 100 = neutral, converted to decimal).
# pybaseball's fg_park_factors is broken (Lahman zip download fails), so we
# maintain these manually. Update once per season from FanGraphs.
PARK_FACTORS_DATA = {
    "AZ":  1.01,  "ATL": 1.01,  "BAL": 1.01,  "BOS": 1.04,
    "CHC": 1.03,  "CWS": 1.01,  "CIN": 1.05,  "CLE": 0.97,
    "COL": 1.14,  "DET": 0.98,  "HOU": 1.00,  "KC":  0.99,
    "LAA": 0.97,  "LAD": 0.97,  "MIA": 0.96,  "MIL": 1.02,
    "MIN": 1.00,  "NYM": 0.97,  "NYY": 1.04,  "OAK": 0.96,
    "PHI": 1.02,  "PIT": 0.97,  "SD":  0.95,  "SF":  0.95,
    "SEA": 0.96,  "STL": 0.98,  "TB":  0.97,  "TEX": 1.00,
    "TOR": 1.02,  "WSH": 1.00,
}

# Map venue names to team abbreviations for park factor lookup
VENUE_TO_TEAM = {
    "Chase Field": "AZ", "Truist Park": "ATL",
    "Oriole Park at Camden Yards": "BAL", "Fenway Park": "BOS",
    "Wrigley Field": "CHC", "Guaranteed Rate Field": "CWS",
    "Great American Ball Park": "CIN", "Progressive Field": "CLE",
    "Coors Field": "COL", "Comerica Park": "DET",
    "Minute Maid Park": "HOU", "Kauffman Stadium": "KC",
    "Angel Stadium": "LAA", "Dodger Stadium": "LAD",
    "LoanDepot Park": "MIA", "loanDepot park": "MIA",
    "American Family Field": "MIL", "Target Field": "MIN",
    "Citi Field": "NYM", "Yankee Stadium": "NYY",
    "Oakland Coliseum": "OAK", "Citizens Bank Park": "PHI",
    "PNC Park": "PIT", "Petco Park": "SD",
    "Oracle Park": "SF", "T-Mobile Park": "SEA",
    "Busch Stadium": "STL", "Tropicana Field": "TB",
    "Globe Life Field": "TEX", "Rogers Centre": "TOR",
    "Nationals Park": "WSH",
}


# ---------------------------------------------------------------------------
# MLB Stats API helpers
# ---------------------------------------------------------------------------

def fetch_schedule(date_str: str) -> List[dict]:
    """Return list of games for a given date (YYYY-MM-DD).

    Uses hydrated API to get pitcher IDs and team abbreviations in one call.
    Each dict has keys: game_id, home_team, away_team, home_abbrev, away_abbrev,
    home_starter_id, away_starter_id, home_starter_name, away_starter_name,
    game_time, venue, status.
    """
    try:
        data = statsapi.get(
            "schedule",
            {"date": date_str, "sportId": 1, "hydrate": "probablePitcher,team"},
        )
    except Exception as e:
        logger.error("[ALERT] MLB Stats API unavailable — %s", e)
        raise RuntimeError(
            f"[ALERT] MLB Stats API unavailable — pipeline cannot continue "
            f"without lineup and starter data. Error: {e}"
        )

    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            teams = g.get("teams", {})
            home = teams.get("home", {})
            away = teams.get("away", {})
            home_team_info = home.get("team", {})
            away_team_info = away.get("team", {})
            home_pp = home.get("probablePitcher", {})
            away_pp = away.get("probablePitcher", {})

            game = {
                "game_id": str(g.get("gamePk", "")),
                "home_team": home_team_info.get("name", ""),
                "away_team": away_team_info.get("name", ""),
                "home_abbrev": home_team_info.get("abbreviation", ""),
                "away_abbrev": away_team_info.get("abbreviation", ""),
                "home_id": home_team_info.get("id"),
                "away_id": away_team_info.get("id"),
                "home_starter_id": str(home_pp["id"]) if home_pp.get("id") else None,
                "away_starter_id": str(away_pp["id"]) if away_pp.get("id") else None,
                "home_starter_name": home_pp.get("fullName", "TBD"),
                "away_starter_name": away_pp.get("fullName", "TBD"),
                "game_time": g.get("gameDate", ""),
                "venue": g.get("venue", {}).get("name", ""),
                "status": g.get("status", {}).get("detailedState", ""),
                "game_type": g.get("gameType", ""),
            }
            games.append(game)
    return games


def fetch_lineup(game_id: str) -> dict:
    """Fetch lineups for a game from boxscore data.

    Returns dict with keys: home_lineup, away_lineup (lists of batter ID strings),
    home_lineup_confirmed, away_lineup_confirmed.
    """
    result = {
        "home_lineup": [],
        "away_lineup": [],
        "home_lineup_confirmed": False,
        "away_lineup_confirmed": False,
    }
    try:
        boxscore = statsapi.boxscore_data(int(game_id))
        for side in ("home", "away"):
            batters = boxscore.get(f"{side}Batters", [])
            # Extract starters in batting order (battingOrder 100-900)
            # Skip header row (personId=0) and substitutions (battingOrder ending in 01+)
            lineup = []
            for b in batters:
                if not isinstance(b, dict):
                    continue
                pid = b.get("personId", 0)
                batting_order = b.get("battingOrder", "")
                if pid == 0:
                    continue  # header row
                # Starters have battingOrder "100","200"..."900"
                # Substitutes have "101","201" etc.
                if batting_order and str(batting_order).endswith("00"):
                    lineup.append(str(pid))

            result[f"{side}_lineup"] = lineup[:9]
            result[f"{side}_lineup_confirmed"] = len(lineup) >= 9
    except Exception as e:
        logger.warning("[ALERT] Could not fetch lineup for game %s — %s", game_id, e)
    return result


def fetch_probable_starters(date_str: str) -> Dict[str, dict]:
    """Return dict mapping game_id -> {home_starter_id, away_starter_id, confirmed}."""
    schedule = fetch_schedule(date_str)
    starters = {}
    for g in schedule:
        starters[g["game_id"]] = {
            "home_starter_id": g.get("home_starter_id"),
            "away_starter_id": g.get("away_starter_id"),
            "home_starter_name": g.get("home_starter_name", "TBD"),
            "away_starter_name": g.get("away_starter_name", "TBD"),
            "starter_confirmed": g.get("status", "") in (
                "Final", "In Progress", "Pre-Game", "Game Over",
            ),
        }
    return starters


# ---------------------------------------------------------------------------
# Statcast (pybaseball)
# ---------------------------------------------------------------------------

def fetch_statcast_pitcher(pitcher_id: int, start_dt: str, end_dt: str) -> Optional[pd.DataFrame]:
    """Fetch raw pitch-level Statcast data for a pitcher."""
    try:
        df = statcast_pitcher(start_dt, end_dt, pitcher_id)
        if df is not None and not df.empty:
            return df
        logger.warning(
            "[ALERT] No Statcast data for pitcher %s (%s to %s)",
            pitcher_id, start_dt, end_dt,
        )
        return None
    except Exception as e:
        logger.error("[ALERT] Statcast pull failed for pitcher %s — %s", pitcher_id, e)
        return None


def fetch_statcast_batter(batter_id: int, start_dt: str, end_dt: str) -> Optional[pd.DataFrame]:
    """Fetch raw pitch-level Statcast data for a batter."""
    try:
        df = statcast_batter(start_dt, end_dt, batter_id)
        if df is not None and not df.empty:
            return df
        logger.warning(
            "[ALERT] No Statcast data for batter %s (%s to %s)",
            batter_id, start_dt, end_dt,
        )
        return None
    except Exception as e:
        logger.error("[ALERT] Statcast pull failed for batter %s — %s", batter_id, e)
        return None


# ---------------------------------------------------------------------------
# Park Factors
# ---------------------------------------------------------------------------

def fetch_park_factors(season: Optional[int] = None) -> dict:
    """Return park factors as dict mapping team abbreviation -> decimal factor.

    Uses hardcoded values since pybaseball's fg_park_factors is broken.
    Caches to data/park_factors.json for consistency.
    """
    season = season or CURRENT_SEASON
    cache_path = os.path.join(DATA_DIR, "park_factors.json")

    # Write cache if it doesn't exist
    if not os.path.exists(cache_path):
        payload = {"season": season, "factors": PARK_FACTORS_DATA}
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(payload, f, indent=2)

    return dict(PARK_FACTORS_DATA)


def lookup_park_factor(venue: str) -> float:
    """Look up park factor for a venue name. Returns 1.0 (neutral) if not found."""
    team = VENUE_TO_TEAM.get(venue)
    if team:
        return PARK_FACTORS_DATA.get(team, 1.0)
    # Fuzzy match
    venue_lower = venue.lower()
    for v, t in VENUE_TO_TEAM.items():
        if v.lower() in venue_lower or venue_lower in v.lower():
            return PARK_FACTORS_DATA.get(t, 1.0)
    return 1.0


# ---------------------------------------------------------------------------
# Odds API
# ---------------------------------------------------------------------------

def fetch_odds(sport: str = "baseball_mlb") -> List[dict]:
    """Fetch current odds for all MLB games from The Odds API.

    Returns list of game odds dicts with moneyline, spread, and totals.
    Fetches h2h, spreads, and totals in a single API call to conserve quota.
    """
    if not ODDS_API_KEY or ODDS_API_KEY == "your_key_here":
        logger.error(
            "[ALERT] Odds API key not configured — value edge check "
            "skipped for all games. Edge score signals shown without value confirmation."
        )
        return []

    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return _parse_odds_response(resp.json())
    except Exception as e:
        logger.error(
            "[ALERT] Odds API unavailable — value edge check skipped for "
            "all games. Edge score signals shown without value confirmation. Error: %s",
            e,
        )
        return []


def _parse_odds_response(data: list[dict]) -> List[dict]:
    """Parse Odds API response into simplified game-level odds dicts."""
    results = []
    for game in data:
        parsed = {
            "odds_game_id": game.get("id"),
            "home_team": game.get("home_team", ""),
            "away_team": game.get("away_team", ""),
            "commence_time": game.get("commence_time", ""),
            "home_moneyline": None,
            "away_moneyline": None,
            "home_run_line": None,
            "away_run_line": None,
            "home_spread_point": None,
            "away_spread_point": None,
            "ou_line": None,
            "ou_over_odds": None,
            "ou_under_odds": None,
        }
        # Use first available bookmaker
        for bookmaker in game.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                key = market["key"]
                outcomes = {o["name"]: o for o in market.get("outcomes", [])}

                if key == "h2h":
                    home = outcomes.get(game.get("home_team", ""), {})
                    away = outcomes.get(game.get("away_team", ""), {})
                    if parsed["home_moneyline"] is None:
                        parsed["home_moneyline"] = home.get("price")
                        parsed["away_moneyline"] = away.get("price")

                elif key == "spreads":
                    home = outcomes.get(game.get("home_team", ""), {})
                    away = outcomes.get(game.get("away_team", ""), {})
                    if parsed["home_run_line"] is None:
                        parsed["home_run_line"] = home.get("price")
                        parsed["away_run_line"] = away.get("price")
                        parsed["home_spread_point"] = home.get("point")
                        parsed["away_spread_point"] = away.get("point")

                elif key == "totals":
                    over = outcomes.get("Over", {})
                    under = outcomes.get("Under", {})
                    if parsed["ou_line"] is None:
                        parsed["ou_line"] = over.get("point")
                        parsed["ou_over_odds"] = over.get("price")
                        parsed["ou_under_odds"] = under.get("price")

            # Stop after first bookmaker with data
            if parsed["home_moneyline"] is not None:
                break

        results.append(parsed)
    return results


# ---------------------------------------------------------------------------
# Bullpen game logs (recent workload)
# ---------------------------------------------------------------------------

def fetch_reliever_workload(team_abbrev: str, days: int = 3) -> float:
    """Calculate total reliever innings pitched in the last N days.

    Returns total IP as a float.
    """
    try:
        teams = statsapi.lookup_team(team_abbrev)
        if not teams:
            return 0.0
        team_id = teams[0]["id"]

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        games = statsapi.schedule(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            team=team_id,
        )

        total_reliever_ip = 0.0
        for game in games:
            if game.get("status") != "Final":
                continue
            try:
                box = statsapi.boxscore_data(game["game_id"])
                side = "home" if game.get("home_id") == team_id else "away"
                pitchers = box.get(f"{side}Pitchers", [])
                # First entry is header row (personId=0), second is starter
                starter_found = False
                for p in pitchers:
                    if not isinstance(p, dict):
                        continue
                    if p.get("personId", 0) == 0:
                        continue  # header row
                    if not starter_found:
                        starter_found = True
                        continue  # skip starter
                    ip_str = p.get("ip", "0")
                    total_reliever_ip += _parse_ip(ip_str)
            except Exception:
                continue
        return total_reliever_ip
    except Exception as e:
        logger.warning("[ALERT] Reliever workload fetch failed for %s — %s", team_abbrev, e)
        return 0.0


def _parse_ip(ip_str: str) -> float:
    """Parse innings pitched string (e.g. '6.1' = 6⅓) to float."""
    try:
        parts = str(ip_str).split(".")
        innings = int(parts[0])
        thirds = int(parts[1]) if len(parts) > 1 else 0
        return innings + thirds / 3.0
    except (ValueError, IndexError):
        return 0.0


# ---------------------------------------------------------------------------
# Proxy lineup (fallback when lineups not posted)
# ---------------------------------------------------------------------------

def fetch_proxy_lineup(team_abbrev: str, num_batters: int = 9) -> list[str]:
    """Get recent starters for a team as a proxy lineup.

    Uses boxscore data from the team's most recent game to find
    the 9 position players who started.
    """
    try:
        teams = statsapi.lookup_team(team_abbrev)
        if not teams:
            return []
        team_id = teams[0]["id"]

        # Find most recent completed game
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        games = statsapi.schedule(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            team=team_id,
        )
        final_games = [g for g in games if g.get("status") == "Final"]
        if not final_games:
            return []

        # Use most recent game
        last_game = final_games[-1]
        box = statsapi.boxscore_data(last_game["game_id"])
        side = "home" if last_game.get("home_id") == team_id else "away"
        batters = box.get(f"{side}Batters", [])

        lineup = []
        for b in batters:
            if not isinstance(b, dict):
                continue
            pid = b.get("personId", 0)
            batting_order = str(b.get("battingOrder", ""))
            if pid == 0:
                continue
            if batting_order.endswith("00"):
                lineup.append(str(pid))

        return lineup[:num_batters]
    except Exception as e:
        logger.warning("[ALERT] Proxy lineup failed for %s — %s", team_abbrev, e)
        return []


# ---------------------------------------------------------------------------
# Caching helpers
# ---------------------------------------------------------------------------

def load_cached_profile(profile_type: str, player_id: str) -> Optional[dict]:
    """Load a cached profile from disk. profile_type is 'pitchers' or 'batters'."""
    path = os.path.join(DATA_DIR, profile_type, f"{player_id}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def save_cached_profile(profile_type: str, player_id: str, profile: dict) -> None:
    """Save a profile to disk cache."""
    dir_path = os.path.join(DATA_DIR, profile_type)
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, f"{player_id}.json")
    with open(path, "w") as f:
        json.dump(profile, f, indent=2)


def save_game_record(date_str: str, games: list[dict]) -> None:
    """Save daily game records to data/games/{date}.json."""
    dir_path = os.path.join(DATA_DIR, "games")
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, f"{date_str}.json")
    with open(path, "w") as f:
        json.dump(games, f, indent=2)


def load_game_record(date_str: str) -> Optional[List[dict]]:
    """Load game records for a date."""
    path = os.path.join(DATA_DIR, "games", f"{date_str}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None
