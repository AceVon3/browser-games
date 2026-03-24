"""
run_daily.py — Morning pass orchestrator.

Entry point for the daily pipeline. Fetches probable starters,
pulls targeted profiles + bullpen scores, scores all matchups,
and writes morning signals.
"""

import os
import sys
import logging
from datetime import date, datetime
from typing import Optional, List

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from src.fetch import (
    fetch_schedule,
    fetch_lineup,
    fetch_odds,
    fetch_proxy_lineup,
    fetch_park_factors,
    load_cached_profile,
    save_game_record,
)
from src.profile import (
    build_pitcher_profile,
    build_batter_profile,
    get_league_avg_pitcher,
    get_league_avg_batter,
)
from src.bullpen import build_bullpen_profile
from src.weather import fetch_weather
from src.score import score_matchup
from src.signal import evaluate_game
from src.notify import print_report
from src.closing_line import log_signal

load_dotenv()

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

# Set up logging
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log"), mode="a"),
    ],
)
logger = logging.getLogger("pipeline")


def morning_pass(date_str: Optional[str] = None):
    """Run the morning pass: fetch, profile, score, signal.

    Args:
        date_str: Override date (YYYY-MM-DD). Defaults to today.
    """
    date_str = date_str or date.today().isoformat()
    logger.info("=" * 60)
    logger.info("MORNING PASS — %s", date_str)
    logger.info("=" * 60)

    # 1. Fetch schedule
    logger.info("Fetching schedule...")
    try:
        schedule = fetch_schedule(date_str)
    except RuntimeError as e:
        logger.error(str(e))
        print(str(e))
        return

    if not schedule:
        print("No games scheduled today.")
        return

    logger.info("Found %d games", len(schedule))

    # 2. Fetch odds (single API call for all games)
    logger.info("Fetching odds...")
    all_odds = fetch_odds()
    odds_lookup = _build_odds_lookup(all_odds)

    # 3. Fetch park factors
    logger.info("Loading park factors...")
    park_factors = fetch_park_factors(CURRENT_SEASON)

    # 4. Process each game
    game_results = []
    for game in schedule:
        logger.info("Processing: %s @ %s", game["away_abbrev"], game["home_abbrev"])
        result = _process_game(game, odds_lookup, park_factors, date_str)
        if result:
            game_results.append(result)

    # 5. Save game records
    save_game_record(date_str, game_results)

    # 6. Log signals to results_log.csv
    for g in game_results:
        if g.get("bet_signal") not in (None, "NO BET"):
            log_signal(g, "ML", pass_version="morning")
            if g.get("rl_alert"):
                log_signal(g, "RL_ALERT", pass_version="morning")
        if g.get("ou_signal") not in (None, "NO BET"):
            log_signal(g, g["ou_signal"], pass_version="morning")

    # 7. Print report
    print_report(date_str, game_results, pass_version="morning")


def _process_game(
    game: dict,
    odds_lookup: dict,
    park_factors: dict,
    date_str: str,
) -> Optional[dict]:
    """Process a single game: fetch profiles, score, evaluate signals."""
    game_id = game["game_id"]

    # Fetch pitcher profiles
    home_pitcher = _get_pitcher_profile(
        game.get("home_starter_id"), game.get("home_starter_name", "TBD")
    )
    away_pitcher = _get_pitcher_profile(
        game.get("away_starter_id"), game.get("away_starter_name", "TBD")
    )

    # Fetch lineups (or proxy)
    lineup_data = fetch_lineup(game_id)
    home_lineup = _get_lineup_profiles(
        lineup_data.get("home_lineup", []),
        game.get("home_abbrev", ""),
        lineup_data.get("home_lineup_confirmed", False),
    )
    away_lineup = _get_lineup_profiles(
        lineup_data.get("away_lineup", []),
        game.get("away_abbrev", ""),
        lineup_data.get("away_lineup_confirmed", False),
    )

    lineup_confirmed = (
        lineup_data.get("home_lineup_confirmed", False)
        and lineup_data.get("away_lineup_confirmed", False)
    )

    # Bullpen profiles
    home_bp = build_bullpen_profile(game.get("home_abbrev", ""))
    away_bp = build_bullpen_profile(game.get("away_abbrev", ""))

    # Weather
    weather = fetch_weather(game.get("venue", ""))

    # Park factor
    park_factor = _lookup_park_factor(game.get("venue", ""), park_factors)

    # Score matchup
    scoring = score_matchup(
        home_pitcher=home_pitcher,
        away_pitcher=away_pitcher,
        home_lineup=home_lineup,
        away_lineup=away_lineup,
        home_bullpen_score=home_bp["bullpen_score"],
        away_bullpen_score=away_bp["bullpen_score"],
        park_factor=park_factor,
        weather=weather,
    )

    # Get odds for this game
    odds = _match_odds(game, odds_lookup)

    # Evaluate signals
    signals = evaluate_game(
        game_scoring=scoring,
        odds=odds,
        players={
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "home_lineup": home_lineup,
            "away_lineup": away_lineup,
        },
    )

    # Combine everything into game result
    result = {
        **game,
        **scoring,
        **odds,
        **signals,
        "date": date_str,
        "lineup_confirmed": lineup_confirmed,
        "starter_confirmed": bool(game.get("home_starter_id") and game.get("away_starter_id")),
        "signal_version": "morning",
    }

    return result


def _get_pitcher_profile(pitcher_id: Optional[str], name: str) -> dict:
    """Get or build a pitcher profile."""
    if not pitcher_id:
        logger.warning("No pitcher ID for %s — using league average", name)
        profile = get_league_avg_pitcher()
        profile["name"] = name
        return profile

    # Try cached first
    cached = load_cached_profile("pitchers", str(pitcher_id))
    if cached:
        return cached

    # Build fresh
    profile = build_pitcher_profile(int(pitcher_id), name)
    if profile:
        return profile

    logger.warning("Could not build profile for %s — using league average", name)
    profile = get_league_avg_pitcher()
    profile["pitcher_id"] = str(pitcher_id)
    profile["name"] = name
    return profile


def _get_lineup_profiles(
    batter_ids: List[str],
    team_abbrev: str,
    confirmed: bool,
) -> List[dict]:
    """Get batter profiles for a lineup. Falls back to proxy if needed."""
    if not batter_ids and not confirmed:
        # Use proxy lineup
        logger.info("  Lineup not posted for %s — using proxy", team_abbrev)
        batter_ids = fetch_proxy_lineup(team_abbrev)

    profiles = []
    for bid in batter_ids[:9]:
        cached = load_cached_profile("batters", str(bid))
        if cached:
            profiles.append(cached)
            continue

        profile = build_batter_profile(int(bid))
        if profile:
            profiles.append(profile)
        else:
            avg = get_league_avg_batter()
            avg["batter_id"] = str(bid)
            profiles.append(avg)

    # Pad to 9 if short
    while len(profiles) < 9:
        profiles.append(get_league_avg_batter())

    return profiles


def _build_odds_lookup(all_odds: List[dict]) -> dict:
    """Build a lookup dict from odds data keyed by team names."""
    lookup = {}
    for odds in all_odds:
        home = odds.get("home_team", "")
        away = odds.get("away_team", "")
        if home and away:
            lookup[(home, away)] = odds
    return lookup


def _match_odds(game: dict, odds_lookup: dict) -> dict:
    """Match a game to its odds data."""
    home = game.get("home_team", "")
    away = game.get("away_team", "")

    odds = odds_lookup.get((home, away), {})
    if not odds:
        # Try abbreviation matching
        for key, val in odds_lookup.items():
            if home in key[0] or key[0] in home:
                if away in key[1] or key[1] in away:
                    odds = val
                    break

    if not odds:
        logger.warning(
            "[ALERT] No odds found for %s vs %s — value edge check skipped",
            away, home,
        )

    return {
        "home_moneyline": odds.get("home_moneyline"),
        "away_moneyline": odds.get("away_moneyline"),
        "home_run_line": odds.get("home_run_line"),
        "away_run_line": odds.get("away_run_line"),
        "ou_line": odds.get("ou_line"),
        "ou_over_odds": odds.get("ou_over_odds"),
        "ou_under_odds": odds.get("ou_under_odds"),
    }


def _lookup_park_factor(venue: str, park_factors: dict) -> float:
    """Look up park factor for a venue. Returns 1.0 (neutral) if not found."""
    if not park_factors:
        return 1.0

    # Direct match
    if venue in park_factors:
        return park_factors[venue]

    # Try partial matching
    venue_lower = venue.lower()
    for team, factor in park_factors.items():
        if team.lower() in venue_lower or venue_lower in team.lower():
            return factor

    return 1.0


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    morning_pass(target_date)
