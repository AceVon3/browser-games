"""
offense.py — Team offensive strength scoring.

Fetches team-level batting stats (OPS, runs/game) from MLB Stats API
and converts to a 0-100 offensive strength score.
"""

import logging
import os
from datetime import date, datetime
from typing import Optional

import statsapi
from dotenv import load_dotenv

from src.fetch import save_cached_profile, load_cached_profile

load_dotenv()

logger = logging.getLogger("pipeline")

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

# Team abbreviation → MLB Stats API team ID
TEAM_IDS = {
    "ATH": 133, "PIT": 134, "SD": 135, "SEA": 136, "SF": 137,
    "STL": 138, "TB": 139, "TEX": 140, "TOR": 141, "MIN": 142,
    "PHI": 143, "ATL": 144, "CWS": 145, "MIA": 146, "NYY": 147,
    "MIL": 158, "LAA": 108, "AZ": 109, "BAL": 110, "BOS": 111,
    "CHC": 112, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC": 118, "LAD": 119, "WSH": 120, "NYM": 121,
}

# League benchmarks for normalization
LEAGUE_AVG_OPS = 0.710
LEAGUE_AVG_RPG = 4.45  # runs per game


def fetch_team_offense(team_abbrev: str, season: Optional[int] = None) -> dict:
    """Fetch team batting stats and return an offensive profile.

    Returns dict with ops, runs_per_game, and offense_score (0-100).
    """
    season = season or CURRENT_SEASON
    team_id = TEAM_IDS.get(team_abbrev)

    if team_id is None:
        logger.warning("Unknown team abbreviation: %s", team_abbrev)
        return _default_profile(team_abbrev)

    # Check cache first (valid for same day)
    cached = load_cached_profile("offense", team_abbrev)
    if cached and cached.get("last_updated") == date.today().isoformat():
        return cached

    try:
        data = statsapi.get(
            "team_stats",
            {"teamId": team_id, "season": season, "stats": "season", "group": "hitting"},
        )
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            logger.warning("No batting stats for %s %d", team_abbrev, season)
            return _default_profile(team_abbrev)

        stat = splits[0]["stat"]
        ops = float(stat.get("ops", LEAGUE_AVG_OPS))
        runs = int(stat.get("runs", 0))
        games = int(stat.get("gamesPlayed", 1)) or 1
        rpg = runs / games

        offense_score = _calculate_offense_score(ops, rpg)

        profile = {
            "team_id": team_abbrev,
            "ops": round(ops, 3),
            "runs_per_game": round(rpg, 2),
            "offense_score": round(offense_score, 1),
            "games_played": games,
            "last_updated": date.today().isoformat(),
        }

        save_cached_profile("offense", team_abbrev, profile)
        return profile

    except Exception as e:
        logger.warning("Failed to fetch offense for %s: %s", team_abbrev, e)
        return _default_profile(team_abbrev)


def _calculate_offense_score(ops: float, rpg: float) -> float:
    """Calculate 0-100 offensive strength score.

    Components (equal weight):
        OPS (50%): .600 = 0, .820 = 100
        Runs/game (50%): 2.5 = 0, 6.5 = 100
    """
    # OPS component: .600 = 0, .820 = 100
    ops_score = max(0, min(100, (ops - 0.600) / 0.220 * 100))

    # Runs/game component: 2.5 = 0, 6.5 = 100
    rpg_score = max(0, min(100, (rpg - 2.5) / 4.0 * 100))

    return 0.5 * ops_score + 0.5 * rpg_score


def _default_profile(team_abbrev: str) -> dict:
    """Return league-average defaults."""
    return {
        "team_id": team_abbrev,
        "ops": LEAGUE_AVG_OPS,
        "runs_per_game": LEAGUE_AVG_RPG,
        "offense_score": 50.0,
        "games_played": 0,
        "last_updated": date.today().isoformat(),
    }
