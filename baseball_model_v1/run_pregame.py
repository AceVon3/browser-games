"""
run_pregame.py — Pregame pass: re-fetch, diff, re-score.

Runs ~75-90 min before first pitch. Re-fetches confirmed starters +
lineups, diffs against morning data, re-scores changed games, and
emits final signals.
"""

import os
import sys
import logging
from datetime import date, datetime
from typing import Optional, List

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))

from src.fetch import (
    fetch_schedule,
    fetch_lineup,
    fetch_odds,
    fetch_park_factors,
    load_game_record,
    save_game_record,
    load_cached_profile,
)
from src.profile import build_batter_profile, get_league_avg_batter
from src.bullpen import build_bullpen_profile
from src.weather import fetch_weather
from src.score import score_matchup
from src.signal import evaluate_game
from src.notify import print_report
from src.closing_line import log_signal

load_dotenv()

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

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


def pregame_pass(date_str: Optional[str] = None):
    """Run the pregame pass: re-fetch, diff, re-score, final signals."""
    date_str = date_str or date.today().isoformat()
    logger.info("=" * 60)
    logger.info("PREGAME PASS — %s", date_str)
    logger.info("=" * 60)

    # Load morning data
    morning_games = load_game_record(date_str)
    if not morning_games:
        logger.warning("No morning data found — running full morning pass first")
        from run_daily import morning_pass
        morning_pass(date_str)
        morning_games = load_game_record(date_str)
        if not morning_games:
            print("No games to process.")
            return

    # Re-fetch confirmed data
    logger.info("Re-fetching confirmed starters and lineups...")
    confirmed = fetch_confirmed(date_str)

    # Fetch fresh odds
    logger.info("Fetching updated odds...")
    all_odds = fetch_odds()
    odds_lookup = _build_odds_lookup(all_odds)

    # Park factors (cached)
    park_factors = fetch_park_factors(CURRENT_SEASON)

    # Diff and re-score
    game_results = []
    for morning_game in morning_games:
        game_id = morning_game.get("game_id", "")
        logger.info("Checking: %s @ %s",
                     morning_game.get("away_abbrev", ""),
                     morning_game.get("home_abbrev", ""))

        # Check for changes
        changes = diff_check(morning_game, confirmed.get(game_id, {}))

        if changes.get("needs_rescore"):
            logger.info("  Re-scoring: %s", ", ".join(changes.get("reasons", [])))
            result = _rescore_game(morning_game, confirmed.get(game_id, {}),
                                   odds_lookup, park_factors, date_str)
        else:
            # Re-check odds even if no starter/lineup change
            result = _update_odds_only(morning_game, odds_lookup)

        result["signal_version"] = "final"
        result["starter_changed"] = changes.get("starter_changed", False)
        game_results.append(result)

    # Save updated records
    save_game_record(date_str, game_results)

    # Log final signals
    for g in game_results:
        if g.get("bet_signal") not in (None, "NO BET"):
            log_signal(g, "ML", pass_version="final")
            if g.get("rl_alert"):
                log_signal(g, "RL_ALERT", pass_version="final")
        if g.get("ou_signal") not in (None, "NO BET"):
            log_signal(g, g["ou_signal"], pass_version="final")

    # Print final report
    print_report(date_str, game_results, pass_version="final")


def fetch_confirmed(date_str: str) -> dict:
    """Re-fetch starters + lineups + updated bullpen workload.

    Returns dict mapping game_id -> confirmed data.
    """
    result = {}
    try:
        schedule = fetch_schedule(date_str)
    except RuntimeError:
        return result

    for game in schedule:
        game_id = game["game_id"]
        lineup = fetch_lineup(game_id)
        result[game_id] = {
            **game,
            **lineup,
        }
    return result


def diff_check(morning: dict, confirmed: dict) -> dict:
    """Flag changes between morning and confirmed data.

    Returns dict with needs_rescore, reasons, and specific change flags.
    """
    changes = {
        "needs_rescore": False,
        "reasons": [],
        "starter_changed": False,
        "lineup_changed": False,
        "line_moved": False,
    }

    if not confirmed:
        return changes

    # Starter swap
    for side in ("home", "away"):
        morning_sp = morning.get(f"{side}_starter_id")
        confirmed_sp = confirmed.get(f"{side}_starter_id")
        if morning_sp and confirmed_sp and morning_sp != confirmed_sp:
            changes["needs_rescore"] = True
            changes["starter_changed"] = True
            changes["reasons"].append(
                f"{side} starter changed: {morning.get(f'{side}_starter_name')} → "
                f"{confirmed.get(f'{side}_starter_name')}"
            )

    # Lineup changes
    for side in ("home", "away"):
        morning_lineup = set(morning.get(f"{side}_lineup", []))
        confirmed_lineup = set(confirmed.get(f"{side}_lineup", []))
        if morning_lineup and confirmed_lineup and morning_lineup != confirmed_lineup:
            changes["needs_rescore"] = True
            changes["lineup_changed"] = True
            changes["reasons"].append(f"{side} lineup changed")

    return changes


def _rescore_game(
    morning: dict,
    confirmed: dict,
    odds_lookup: dict,
    park_factors: dict,
    date_str: str,
) -> dict:
    """Full re-score of a changed game."""
    from run_daily import _get_pitcher_profile, _get_lineup_profiles, _match_odds, _lookup_park_factor

    # Use confirmed data where available, fall back to morning
    home_starter_id = confirmed.get("home_starter_id") or morning.get("home_starter_id")
    away_starter_id = confirmed.get("away_starter_id") or morning.get("away_starter_id")
    home_starter_name = confirmed.get("home_starter_name") or morning.get("home_starter_name", "TBD")
    away_starter_name = confirmed.get("away_starter_name") or morning.get("away_starter_name", "TBD")

    home_pitcher = _get_pitcher_profile(home_starter_id, home_starter_name)
    away_pitcher = _get_pitcher_profile(away_starter_id, away_starter_name)

    home_lineup_ids = confirmed.get("home_lineup") or morning.get("home_lineup", [])
    away_lineup_ids = confirmed.get("away_lineup") or morning.get("away_lineup", [])

    home_confirmed = confirmed.get("home_lineup_confirmed", False)
    away_confirmed = confirmed.get("away_lineup_confirmed", False)

    home_lineup = _get_lineup_profiles(home_lineup_ids, morning.get("home_abbrev", ""), home_confirmed)
    away_lineup = _get_lineup_profiles(away_lineup_ids, morning.get("away_abbrev", ""), away_confirmed)

    # Refresh bullpen with updated workload
    home_bp = build_bullpen_profile(morning.get("home_abbrev", ""))
    away_bp = build_bullpen_profile(morning.get("away_abbrev", ""))

    weather = fetch_weather(morning.get("venue", ""))
    park_factor = _lookup_park_factor(morning.get("venue", ""), park_factors)

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

    odds = _match_odds(morning, odds_lookup)

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

    lineup_confirmed = home_confirmed and away_confirmed

    result = {
        **morning,
        **scoring,
        **odds,
        **signals,
        "date": date_str,
        "home_starter_id": home_starter_id,
        "away_starter_id": away_starter_id,
        "home_starter_name": home_starter_name,
        "away_starter_name": away_starter_name,
        "lineup_confirmed": lineup_confirmed,
        "starter_confirmed": bool(home_starter_id and away_starter_id),
    }
    return result


def _update_odds_only(morning: dict, odds_lookup: dict) -> dict:
    """Update a game with fresh odds without re-scoring."""
    from run_daily import _match_odds

    odds = _match_odds(morning, odds_lookup)

    # Check for significant line moves (> 10 pts)
    for ml_key in ("home_moneyline", "away_moneyline"):
        old = morning.get(ml_key)
        new = odds.get(ml_key)
        if old is not None and new is not None and abs(new - old) > 10:
            logger.info("  Line move: %s %s → %s", ml_key, old, new)

    # Re-evaluate signals with new odds
    from src.signal import evaluate_game
    signals = evaluate_game(
        game_scoring={
            "home_edge_score": morning.get("home_edge_score", 0),
            "away_edge_score": morning.get("away_edge_score", 0),
            "ou_score": morning.get("ou_score", 50),
            "ou_model_total": morning.get("ou_model_total", 9.0),
        },
        odds=odds,
        players={
            "home_pitcher": load_cached_profile("pitchers", str(morning.get("home_starter_id", ""))) or {},
            "away_pitcher": load_cached_profile("pitchers", str(morning.get("away_starter_id", ""))) or {},
            "home_lineup": [],
            "away_lineup": [],
        },
    )

    result = {**morning, **odds, **signals}
    return result


def _build_odds_lookup(all_odds: List[dict]) -> dict:
    lookup = {}
    for odds in all_odds:
        home = odds.get("home_team", "")
        away = odds.get("away_team", "")
        if home and away:
            lookup[(home, away)] = odds
    return lookup


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    pregame_pass(target_date)
