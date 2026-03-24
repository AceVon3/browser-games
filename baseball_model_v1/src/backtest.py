"""
backtest.py — Historical validation and calibration.

Runs the model over a full historical season day-by-day:
1. Reconstruct daily lineups, starters, bullpen workloads
2. Score each game using the model
3. Compare signals to actual results
4. Build calibration curves for ML, O/U, and RL
"""

import os
import sys
import csv
import logging
from datetime import datetime, timedelta, date
from collections import defaultdict
from typing import Optional, List

import pandas as pd
import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.fetch import fetch_park_factors
from src.profile import build_pitcher_profile, build_batter_profile, get_league_avg_pitcher, get_league_avg_batter
from src.bullpen import build_bullpen_profile
from src.weather import fetch_historical_weather, STADIUM_COORDS
from src.score import score_matchup
from src.signal import evaluate_side_signal, evaluate_ou_signal, edge_to_win_prob

load_dotenv()

logger = logging.getLogger("pipeline")

BACKTEST_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "backtest")


def run_backtest(season: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Run the full backtest for a season.

    Args:
        season: MLB season year to backtest
        start_date: Optional start date (YYYY-MM-DD), defaults to season opener
        end_date: Optional end date, defaults to last regular season game
    """
    import statsapi

    start_date = start_date or f"{season}-03-28"
    end_date = end_date or f"{season}-09-29"

    logger.info("=" * 60)
    logger.info("BACKTEST — %d season (%s to %s)", season, start_date, end_date)
    logger.info("=" * 60)

    # Load park factors for the season
    park_factors = fetch_park_factors(season)

    # Results tracking
    results = []
    os.makedirs(BACKTEST_DIR, exist_ok=True)

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        logger.info("Processing %s...", date_str)

        try:
            schedule = statsapi.schedule(date=date_str)
        except Exception as e:
            logger.warning("Could not fetch schedule for %s — %s", date_str, e)
            current += timedelta(days=1)
            continue

        for game in schedule:
            if game.get("status") != "Final":
                continue

            try:
                result = _backtest_game(game, season, park_factors, date_str)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning("Error processing game %s — %s", game.get("game_id"), e)

        current += timedelta(days=1)

    # Write results
    _write_backtest_results(results, season)

    # Build calibration curves
    _build_calibration_curves(results, season)

    logger.info("Backtest complete: %d games processed", len(results))


def _backtest_game(game: dict, season: int, park_factors: dict, date_str: str) -> Optional[dict]:
    """Process a single historical game for backtesting."""
    import statsapi

    game_id = game["game_id"]

    # Get actual result
    home_score = game.get("home_score", 0)
    away_score = game.get("away_score", 0)
    total_runs = home_score + away_score
    home_won = home_score > away_score

    # Get starters (use boxscore data)
    try:
        box = statsapi.boxscore_data(game_id)
    except Exception:
        return None

    # Build simplified profiles from cached data
    home_pitcher = _get_cached_or_avg("pitchers", game.get("home_probable_pitcher", ""))
    away_pitcher = _get_cached_or_avg("pitchers", game.get("away_probable_pitcher", ""))

    # Use league average lineups for speed (full lineup reconstruction is slow)
    home_lineup = [get_league_avg_batter() for _ in range(9)]
    away_lineup = [get_league_avg_batter() for _ in range(9)]

    # Bullpen scores (simplified — use league average for backtest speed)
    home_bp_score = 55.0
    away_bp_score = 55.0

    # Historical weather
    venue = game.get("venue_name", "")
    coords = STADIUM_COORDS.get(venue, (40.0, -74.0))
    weather = fetch_historical_weather(coords[0], coords[1], date_str)

    # Park factor
    park_factor = 1.0
    for team, factor in park_factors.items():
        if team.lower() in venue.lower():
            park_factor = factor
            break

    # Score
    scoring = score_matchup(
        home_pitcher=home_pitcher,
        away_pitcher=away_pitcher,
        home_lineup=home_lineup,
        away_lineup=away_lineup,
        home_bullpen_score=home_bp_score,
        away_bullpen_score=away_bp_score,
        park_factor=park_factor,
        weather=weather,
    )

    # Determine signals (without odds for backtest)
    side = evaluate_side_signal(
        home_edge=scoring["home_edge_score"],
        away_edge=scoring["away_edge_score"],
        home_moneyline=None,
        away_moneyline=None,
    )

    ou = evaluate_ou_signal(
        ou_score=scoring["ou_score"],
        model_total=scoring["ou_model_total"],
        ou_line=None,
        ou_over_odds=None,
        ou_under_odds=None,
    )

    return {
        "date": date_str,
        "game_id": str(game_id),
        "home_team": game.get("home_name", ""),
        "away_team": game.get("away_name", ""),
        "venue": venue,
        "home_edge_score": scoring["home_edge_score"],
        "away_edge_score": scoring["away_edge_score"],
        "ou_score": scoring["ou_score"],
        "model_total": scoring["ou_model_total"],
        "actual_total": total_runs,
        "home_score": home_score,
        "away_score": away_score,
        "home_won": home_won,
        "bet_signal": side.get("bet_signal", "NO BET"),
        "bet_side": side.get("bet_side"),
        "model_win_prob": side.get("model_win_prob"),
        "ou_signal": ou.get("ou_signal", "NO BET"),
        "ml_result": _get_ml_result(side, home_won),
        "ou_result": _get_ou_result(ou, total_runs, scoring["ou_model_total"]),
    }


def _get_cached_or_avg(profile_type: str, name: str) -> dict:
    """Try to load cached profile, fall back to league average."""
    from src.fetch import load_cached_profile
    import statsapi

    if name and name != "TBD":
        try:
            lookup = statsapi.lookup_player(name)
            if lookup:
                pid = str(lookup[0]["id"])
                cached = load_cached_profile(profile_type, pid)
                if cached:
                    return cached
        except Exception:
            pass

    if profile_type == "pitchers":
        return get_league_avg_pitcher()
    return get_league_avg_batter()


def _get_ml_result(side: dict, home_won: bool) -> str:
    """Determine ML result."""
    bet_side = side.get("bet_side")
    if not bet_side or side.get("bet_signal") == "NO BET":
        return "NO BET"
    if bet_side == "HOME" and home_won:
        return "WIN"
    elif bet_side == "AWAY" and not home_won:
        return "WIN"
    return "LOSS"


def _get_ou_result(ou: dict, actual_total: int, model_total: float) -> str:
    """Determine O/U result."""
    signal = ou.get("ou_signal", "NO BET")
    if signal == "NO BET":
        return "NO BET"
    # Without a book line in backtest, use model total as the reference
    if signal == "OVER" and actual_total > model_total:
        return "WIN"
    elif signal == "UNDER" and actual_total < model_total:
        return "WIN"
    elif actual_total == round(model_total):
        return "PUSH"
    return "LOSS"


def _write_backtest_results(results: List[dict], season: int):
    """Write backtest results to CSV."""
    path = os.path.join(BACKTEST_DIR, f"backtest_{season}.csv")
    if not results:
        return

    fieldnames = list(results[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info("Results written to %s", path)


def _build_calibration_curves(results: List[dict], season: int):
    """Build calibration curves from backtest results.

    Three curves:
    1. ML: edge score bucket → observed win rate
    2. O/U: model total discrepancy → observed over/under rate
    3. RL: edge score 75+ → win-by-2+ rate
    """
    # ML calibration
    ml_buckets = defaultdict(lambda: {"total": 0, "wins": 0})
    for r in results:
        if r["bet_signal"] == "NO BET":
            continue
        edge = max(r["home_edge_score"], r["away_edge_score"])
        bucket = int(edge // 5) * 5  # 5-point buckets
        ml_buckets[bucket]["total"] += 1
        if r["ml_result"] == "WIN":
            ml_buckets[bucket]["wins"] += 1

    # O/U calibration
    ou_buckets = defaultdict(lambda: {"total": 0, "correct": 0})
    for r in results:
        if r["ou_signal"] == "NO BET":
            continue
        diff = r["model_total"] - r["actual_total"]
        bucket = round(diff * 2) / 2  # 0.5-run buckets
        ou_buckets[bucket]["total"] += 1
        if r["ou_result"] == "WIN":
            ou_buckets[bucket]["correct"] += 1

    # RL calibration (edge >= 75)
    rl_data = {"total": 0, "win_by_2": 0}
    for r in results:
        edge = max(r["home_edge_score"], r["away_edge_score"])
        if edge < 75:
            continue
        rl_data["total"] += 1
        margin = abs(r["home_score"] - r["away_score"])
        if r["ml_result"] == "WIN" and margin >= 2:
            rl_data["win_by_2"] += 1

    # Print summary
    print("\n" + "=" * 60)
    print(f"CALIBRATION CURVES — {season} Season Backtest")
    print("=" * 60)

    print("\nML Win Rate by Edge Score Bucket:")
    print(f"  {'Bucket':>8}  {'Games':>6}  {'Win Rate':>8}")
    for bucket in sorted(ml_buckets.keys()):
        data = ml_buckets[bucket]
        rate = data["wins"] / data["total"] if data["total"] > 0 else 0
        print(f"  {bucket:>5}-{bucket + 4:<3}  {data['total']:>6}  {rate:>7.1%}")

    print("\nO/U Accuracy by Model Total Discrepancy:")
    print(f"  {'Diff':>8}  {'Games':>6}  {'Accuracy':>8}")
    for bucket in sorted(ou_buckets.keys()):
        data = ou_buckets[bucket]
        rate = data["correct"] / data["total"] if data["total"] > 0 else 0
        print(f"  {bucket:>+7.1f}  {data['total']:>6}  {rate:>7.1%}")

    if rl_data["total"] > 0:
        rl_rate = rl_data["win_by_2"] / rl_data["total"]
        print(f"\nRL Win-by-2+ Rate (edge >= 75): {rl_rate:.1%} ({rl_data['win_by_2']}/{rl_data['total']})")
    else:
        print("\nRL: No games at edge >= 75")

    # Save calibration data
    cal_path = os.path.join(BACKTEST_DIR, f"calibration_{season}.json")
    import json
    cal_data = {
        "season": season,
        "ml_buckets": {str(k): v for k, v in ml_buckets.items()},
        "ou_buckets": {str(k): v for k, v in ou_buckets.items()},
        "rl_data": rl_data,
        "total_games": len(results),
    }
    with open(cal_path, "w") as f:
        json.dump(cal_data, f, indent=2)
    logger.info("Calibration data saved to %s", cal_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    season = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year - 1
    start = sys.argv[2] if len(sys.argv) > 2 else None
    end = sys.argv[3] if len(sys.argv) > 3 else None
    run_backtest(season, start, end)
