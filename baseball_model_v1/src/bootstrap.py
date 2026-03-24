"""
bootstrap.py — Cold start / opening day profile seeding.

Run once before the first game of the season. Pulls the full prior season
of Statcast data for all pitchers and batters on opening day rosters.
"""

import os
import logging
from datetime import datetime
from typing import Optional, List

import statsapi
from dotenv import load_dotenv

from src.profile import (
    build_pitcher_profile,
    build_batter_profile,
    get_league_avg_pitcher,
    get_league_avg_batter,
)
from src.fetch import save_cached_profile

load_dotenv()

logger = logging.getLogger("pipeline")

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))


def bootstrap_all_teams():
    """Bootstrap prior-season profiles for all 30 MLB teams.

    Pulls full prior season Statcast data and stores as profile JSON
    with data_source = 'prior_season'.
    """
    prior_season = CURRENT_SEASON - 1
    start_dt = f"{prior_season}-03-20"
    end_dt = f"{prior_season}-11-05"

    logger.info("Bootstrapping profiles from %d season...", prior_season)

    # Get all team IDs
    try:
        teams = statsapi.get("teams", {"sportId": 1})
        team_list = teams.get("teams", [])
    except Exception as e:
        logger.error("[ALERT] Cannot fetch team list — %s", e)
        return

    total_pitchers = 0
    total_batters = 0

    for team in team_list:
        team_id = team["id"]
        team_name = team.get("abbreviation", team.get("name", str(team_id)))
        logger.info("Processing %s...", team_name)

        try:
            roster = statsapi.roster(team_id, rosterType="40Man", season=prior_season)
        except Exception as e:
            logger.warning("Could not fetch roster for %s — %s", team_name, e)
            continue

        # Parse roster for player IDs
        players = _parse_roster(roster, team_id, prior_season)

        for player in players:
            pid = player["id"]
            name = player["name"]
            position = player["position"]

            if position in ("P", "SP", "RP", "CL", "TWP"):
                profile = _bootstrap_pitcher(pid, name, start_dt, end_dt)
                if profile:
                    total_pitchers += 1
            else:
                profile = _bootstrap_batter(pid, name, start_dt, end_dt)
                if profile:
                    total_batters += 1

    logger.info(
        "Bootstrap complete: %d pitchers, %d batters profiled from %d season",
        total_pitchers, total_batters, prior_season,
    )


def _parse_roster(roster_text: str, team_id: int, season: int) -> List[dict]:
    """Parse roster text output into player dicts."""
    players = []
    for line in roster_text.split("\n"):
        line = line.strip()
        if not line or line.startswith("-"):
            continue

        parts = line.split()
        if len(parts) < 3:
            continue

        # Format: "#NN FirstName LastName Position"
        try:
            position = parts[-1]
            name = " ".join(parts[1:-1])
            # Look up player ID
            lookup = statsapi.lookup_player(name)
            if lookup:
                players.append({
                    "id": lookup[0]["id"],
                    "name": lookup[0].get("fullName", name),
                    "position": position,
                })
        except Exception:
            continue

    return players


def _bootstrap_pitcher(pid: int, name: str, start_dt: str, end_dt: str) -> Optional[dict]:
    """Build and save a prior-season pitcher profile."""
    try:
        profile = build_pitcher_profile(pid, name, start_dt, end_dt)
        if profile:
            profile["data_source"] = "prior_season"
            save_cached_profile("pitchers", str(pid), profile)
            logger.info("  Pitcher: %s (%s)", name, pid)
            return profile
        else:
            # No data — use league average
            profile = get_league_avg_pitcher()
            profile["pitcher_id"] = str(pid)
            profile["name"] = name
            profile["data_source"] = "league_avg"
            save_cached_profile("pitchers", str(pid), profile)
            logger.info("  Pitcher: %s (%s) — league average (no prior data)", name, pid)
            return profile
    except Exception as e:
        logger.warning("  Failed to bootstrap pitcher %s — %s", name, e)
        return None


def _bootstrap_batter(pid: int, name: str, start_dt: str, end_dt: str) -> Optional[dict]:
    """Build and save a prior-season batter profile."""
    try:
        profile = build_batter_profile(pid, name, start_dt, end_dt)
        if profile:
            profile["data_source"] = "prior_season"
            save_cached_profile("batters", str(pid), profile)
            logger.info("  Batter: %s (%s)", name, pid)
            return profile
        else:
            profile = get_league_avg_batter()
            profile["batter_id"] = str(pid)
            profile["name"] = name
            profile["data_source"] = "league_avg"
            save_cached_profile("batters", str(pid), profile)
            logger.info("  Batter: %s (%s) — league average (no prior data)", name, pid)
            return profile
    except Exception as e:
        logger.warning("  Failed to bootstrap batter %s — %s", name, e)
        return None


def bootstrap_returning_player(pid: int, name: str, position: str, years_missed: int = 1):
    """Handle a returning player after long injury (1+ year).

    Uses last available season with 20% shrinkage toward league average.
    """
    prior_season = CURRENT_SEASON - years_missed - 1
    start_dt = f"{prior_season}-03-20"
    end_dt = f"{prior_season}-11-05"

    if position in ("P", "SP", "RP", "CL"):
        profile = build_pitcher_profile(pid, name, start_dt, end_dt)
        if profile:
            league_avg = get_league_avg_pitcher()
            # Apply 20% shrinkage toward league average
            profile = _shrink_toward_average(profile, league_avg, shrinkage=0.20)
            profile["data_source"] = "prior_season"
            save_cached_profile("pitchers", str(pid), profile)
    else:
        profile = build_batter_profile(pid, name, start_dt, end_dt)
        if profile:
            league_avg = get_league_avg_batter()
            profile = _shrink_toward_average(profile, league_avg, shrinkage=0.20)
            profile["data_source"] = "prior_season"
            save_cached_profile("batters", str(pid), profile)


def _shrink_toward_average(profile: dict, league_avg: dict, shrinkage: float) -> dict:
    """Apply shrinkage toward league average for returning players."""
    for key in profile:
        p_val = profile[key]
        avg_val = league_avg.get(key)
        if isinstance(p_val, (int, float)) and isinstance(avg_val, (int, float)):
            profile[key] = round(p_val * (1 - shrinkage) + avg_val * shrinkage, 4)
    return profile


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                os.path.join(os.path.dirname(__file__), "..", "logs", "pipeline.log"),
                mode="a",
            ),
        ],
    )
    bootstrap_all_teams()
