"""
backtest.py — Historical validation and calibration.

Runs the model over a full historical season day-by-day:
1. Reconstruct actual lineups from boxscore data
2. Build pitcher/batter profiles from prior season (no look-ahead bias)
3. Score each game using the full model pipeline
4. Compare signals to actual results
5. Build calibration curves for ML, DIFF, O/U, and RL
"""

import os
import sys
import csv
import json
import logging
import time
import signal as _signal
from datetime import datetime, timedelta, date
from collections import defaultdict
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import pandas as pd
import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.fetch import fetch_park_factors, fetch_lineup, fetch_statcast_pitcher, fetch_statcast_batter
from src.profile import _aggregate_pitcher_data, _aggregate_batter_data, get_league_avg_pitcher, get_league_avg_batter
from src.bullpen import build_bullpen_profile
from src.weather import fetch_historical_weather, STADIUM_COORDS
from src.score import score_matchup
from src.signal import evaluate_game, edge_to_win_prob

load_dotenv()

logger = logging.getLogger("pipeline")

BACKTEST_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "backtest")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _get_backtest_cache_dir(season: int) -> str:
    """Get the backtest profile cache directory for a given season."""
    cache_dir = os.path.join(BACKTEST_DIR, f"profiles_{season}")
    os.makedirs(os.path.join(cache_dir, "pitchers"), exist_ok=True)
    os.makedirs(os.path.join(cache_dir, "batters"), exist_ok=True)
    return cache_dir


def _load_bt_profile(cache_dir: str, profile_type: str, player_id: str) -> Optional[dict]:
    """Load a profile from the backtest cache."""
    path = os.path.join(cache_dir, profile_type, f"{player_id}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def _save_bt_profile(cache_dir: str, profile_type: str, player_id: str, profile: dict) -> None:
    """Save a profile to the backtest cache."""
    path = os.path.join(cache_dir, profile_type, f"{player_id}.json")
    with open(path, "w") as f:
        json.dump(profile, f, indent=2)


STATCAST_TIMEOUT = 45  # seconds per player fetch


def _fetch_with_timeout(fetch_fn, player_id, start_dt, end_dt):
    """Run a Statcast fetch with a timeout to avoid rate-limit stalls."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fetch_fn, player_id, start_dt, end_dt)
        try:
            return future.result(timeout=STATCAST_TIMEOUT)
        except (FuturesTimeout, Exception):
            return None


def _build_bt_pitcher(cache_dir: str, pitcher_id: int, name: str, prior_season: int) -> dict:
    """Build or load a pitcher profile for backtest using prior season data.

    Fetches Statcast data for the prior season only — no fallback to other
    seasons, no blending, no writing to the live cache. Times out after 45s.
    """
    pid = str(pitcher_id)
    cached = _load_bt_profile(cache_dir, "pitchers", pid)
    if cached:
        return cached

    start_dt = f"{prior_season}-03-01"
    end_dt = f"{prior_season}-11-01"
    try:
        df = _fetch_with_timeout(fetch_statcast_pitcher, pitcher_id, start_dt, end_dt)
        if df is not None and not df.empty and len(df) >= 50:
            profile = _aggregate_pitcher_data(df, pitcher_id, name)
            profile["data_source"] = "backtest_prior_season"
            _save_bt_profile(cache_dir, "pitchers", pid, profile)
            return profile
    except Exception as e:
        logger.debug("Could not build pitcher profile %s — %s", pid, e)

    # No data — use league average but still cache it to avoid re-fetching
    avg = get_league_avg_pitcher()
    avg["name"] = name
    avg["pitcher_id"] = pid
    avg["data_source"] = "league_avg"
    _save_bt_profile(cache_dir, "pitchers", pid, avg)
    return avg


def _build_bt_batter(cache_dir: str, batter_id: int, prior_season: int) -> dict:
    """Build or load a batter profile for backtest using prior season data.

    Fetches Statcast data for the prior season only — no fallback, no blending.
    Times out after 45s.
    """
    bid = str(batter_id)
    cached = _load_bt_profile(cache_dir, "batters", bid)
    if cached:
        return cached

    start_dt = f"{prior_season}-03-01"
    end_dt = f"{prior_season}-11-01"
    try:
        df = _fetch_with_timeout(fetch_statcast_batter, batter_id, start_dt, end_dt)
        if df is not None and not df.empty and len(df) >= 50:
            profile = _aggregate_batter_data(df, batter_id, "")
            profile["data_source"] = "backtest_prior_season"
            _save_bt_profile(cache_dir, "batters", bid, profile)
            return profile
    except Exception as e:
        logger.debug("Could not build batter profile %s — %s", bid, e)

    # No data — cache league avg to avoid re-fetching
    avg = get_league_avg_batter()
    avg["batter_id"] = bid
    avg["data_source"] = "league_avg"
    _save_bt_profile(cache_dir, "batters", bid, avg)
    return avg


def _prewarm_profiles(season: int, prior_season: int, cache_dir: str,
                      start_date: str, end_date: str):
    """Pre-warm the profile cache by scanning the full schedule for player IDs.

    Collects all unique pitcher and batter IDs from boxscore data, then
    batch-fetches profiles. This avoids rate-limit stalls during scoring.
    """
    import statsapi

    logger.info("PRE-WARM: Scanning schedule for player IDs...")

    pitcher_names = set()
    batter_ids = set()
    # Cache lineups and schedule so we don't re-fetch during scoring
    lineup_cache_path = os.path.join(cache_dir, "lineup_cache.json")
    schedule_cache_path = os.path.join(cache_dir, "schedule_cache.json")

    # Try to load existing caches
    lineup_cache = {}
    schedule_cache = {}
    if os.path.exists(lineup_cache_path):
        with open(lineup_cache_path) as f:
            lineup_cache = json.load(f)
    if os.path.exists(schedule_cache_path):
        with open(schedule_cache_path) as f:
            schedule_cache = json.load(f)

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        if date_str in schedule_cache:
            # Use cached schedule
            for game in schedule_cache[date_str]:
                game_id = str(game["game_id"])
                for side in ("home", "away"):
                    name = game.get(f"{side}_probable_pitcher", "")
                    if name and name != "TBD":
                        pitcher_names.add(name)
                if game_id in lineup_cache:
                    for bid in lineup_cache[game_id].get("home_lineup", []):
                        batter_ids.add(int(bid))
                    for bid in lineup_cache[game_id].get("away_lineup", []):
                        batter_ids.add(int(bid))
            current += timedelta(days=1)
            continue

        try:
            schedule = statsapi.schedule(date=date_str)
        except Exception:
            current += timedelta(days=1)
            continue

        day_games = []
        for game in schedule:
            if game.get("status") != "Final":
                continue
            game_id = str(game["game_id"])
            day_games.append(game)

            # Collect pitcher names
            for side in ("home", "away"):
                name = game.get(f"{side}_probable_pitcher", "")
                if name and name != "TBD":
                    pitcher_names.add(name)

            # Collect batter IDs from boxscore
            if game_id not in lineup_cache:
                try:
                    lineup_data = fetch_lineup(game_id)
                    lineup_cache[game_id] = lineup_data
                except Exception:
                    pass

            if game_id in lineup_cache:
                for bid in lineup_cache[game_id].get("home_lineup", []):
                    batter_ids.add(int(bid))
                for bid in lineup_cache[game_id].get("away_lineup", []):
                    batter_ids.add(int(bid))

        schedule_cache[date_str] = day_games
        current += timedelta(days=1)

    # Save caches for future runs
    logger.info("PRE-WARM: Saving lineup and schedule caches...")
    with open(lineup_cache_path, "w") as f:
        json.dump(lineup_cache, f)
    with open(schedule_cache_path, "w") as f:
        json.dump(schedule_cache, f, default=str)

    # Resolve pitcher names to IDs
    pitcher_ids = {}  # name -> id
    for name in pitcher_names:
        try:
            lookup = statsapi.lookup_player(name)
            if lookup:
                pitcher_ids[name] = lookup[0]["id"]
        except Exception:
            pass

    uncached_pitchers = [(name, pid) for name, pid in pitcher_ids.items()
                         if _load_bt_profile(cache_dir, "pitchers", str(pid)) is None]
    uncached_batters = [bid for bid in batter_ids
                        if _load_bt_profile(cache_dir, "batters", str(bid)) is None]

    logger.info("PRE-WARM: Found %d pitchers (%d uncached), %d batters (%d uncached)",
                len(pitcher_ids), len(uncached_pitchers),
                len(batter_ids), len(uncached_batters))

    # Batch-build pitcher profiles
    for i, (name, pid) in enumerate(uncached_pitchers, 1):
        if i % 25 == 0:
            logger.info("PRE-WARM: Pitchers %d/%d", i, len(uncached_pitchers))
        _build_bt_pitcher(cache_dir, pid, name, prior_season)

    # Batch-build batter profiles
    for i, bid in enumerate(uncached_batters, 1):
        if i % 50 == 0:
            logger.info("PRE-WARM: Batters %d/%d", i, len(uncached_batters))
        _build_bt_batter(cache_dir, bid, prior_season)

    logger.info("PRE-WARM: Complete. All profiles cached.")

    return schedule_cache, lineup_cache


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

    prior_season = season - 1
    cache_dir = _get_backtest_cache_dir(season)

    logger.info("=" * 60)
    logger.info("BACKTEST — %d season (%s to %s)", season, start_date, end_date)
    logger.info("Using %d prior season data for profiles", prior_season)
    logger.info("Profile cache: %s", cache_dir)
    logger.info("=" * 60)

    # Phase 1: Pre-warm profile cache and collect schedule/lineups
    schedule_cache, lineup_cache = _prewarm_profiles(
        season, prior_season, cache_dir, start_date, end_date
    )

    # Load park factors for the season
    park_factors = fetch_park_factors(season)

    # Phase 2: Score all games using cached data
    logger.info("=" * 60)
    logger.info("SCORING PHASE — processing games...")
    logger.info("=" * 60)

    results = []
    game_count = 0
    skip_count = 0
    os.makedirs(BACKTEST_DIR, exist_ok=True)

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    bt_start = time.time()

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        day_games = schedule_cache.get(date_str, [])
        if not day_games:
            current += timedelta(days=1)
            continue

        logger.info("Scoring %s... (%d games done)", date_str, game_count)

        for game in day_games:
            game_id = str(game["game_id"])
            cached_lineup = lineup_cache.get(game_id, {})

            try:
                result = _backtest_game(
                    game, season, prior_season, cache_dir,
                    park_factors, date_str, cached_lineup,
                )
                if result:
                    results.append(result)
                    game_count += 1
                else:
                    skip_count += 1
            except Exception as e:
                logger.warning("Error processing game %s — %s", game_id, e)
                skip_count += 1

        current += timedelta(days=1)

    elapsed = time.time() - bt_start
    logger.info("Backtest complete: %d games processed, %d skipped, %.1f min elapsed",
                game_count, skip_count, elapsed / 60)

    # Write results
    _write_backtest_results(results, season)

    # Build calibration curves
    _build_calibration_curves(results, season)


def _backtest_game(
    game: dict, season: int, prior_season: int,
    cache_dir: str, park_factors: dict, date_str: str,
    cached_lineup: Optional[dict] = None,
) -> Optional[dict]:
    """Process a single historical game for backtesting."""
    game_id = game["game_id"]

    # Get actual result
    home_score = game.get("home_score", 0)
    away_score = game.get("away_score", 0)
    total_runs = home_score + away_score
    home_won = home_score > away_score

    # Get actual lineups (from cache or fresh fetch)
    lineup_data = cached_lineup if cached_lineup else fetch_lineup(str(game_id))
    home_batter_ids = lineup_data.get("home_lineup", [])
    away_batter_ids = lineup_data.get("away_lineup", [])

    if len(home_batter_ids) < 5 or len(away_batter_ids) < 5:
        logger.debug("Incomplete lineup for game %s — skipping", game_id)
        return None

    # Build pitcher profiles from prior season
    home_pitcher = _get_bt_pitcher(cache_dir, game, "home", prior_season)
    away_pitcher = _get_bt_pitcher(cache_dir, game, "away", prior_season)

    # Build batter profiles from prior season
    home_lineup = []
    for bid in home_batter_ids[:9]:
        home_lineup.append(_build_bt_batter(cache_dir, int(bid), prior_season))
    while len(home_lineup) < 9:
        home_lineup.append(get_league_avg_batter())

    away_lineup = []
    for bid in away_batter_ids[:9]:
        away_lineup.append(_build_bt_batter(cache_dir, int(bid), prior_season))
    while len(away_lineup) < 9:
        away_lineup.append(get_league_avg_batter())

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
    odds = {
        "home_moneyline": None, "away_moneyline": None,
        "home_run_line": None, "away_run_line": None,
        "ou_line": None, "ou_over_odds": None, "ou_under_odds": None,
    }
    players = {
        "home_pitcher": home_pitcher, "away_pitcher": away_pitcher,
        "home_lineup": home_lineup, "away_lineup": away_lineup,
    }
    signals = evaluate_game(game_scoring=scoring, odds=odds, players=players)

    # Determine diff signal result
    diff_gap = abs(scoring["home_edge_score"] - scoring["away_edge_score"])

    return {
        "date": date_str,
        "game_id": str(game_id),
        "home_team": game.get("home_name", ""),
        "away_team": game.get("away_name", ""),
        "venue": venue,
        "home_edge_score": scoring["home_edge_score"],
        "away_edge_score": scoring["away_edge_score"],
        "edge_diff": round(diff_gap, 1),
        "ou_score": scoring["ou_score"],
        "ou_convergence_boost": scoring.get("ou_convergence_boost", 0),
        "model_total": scoring["ou_model_total"],
        "actual_total": total_runs,
        "home_score": home_score,
        "away_score": away_score,
        "home_won": home_won,
        "bet_signal": signals.get("bet_signal", "NO BET"),
        "bet_side": signals.get("bet_side"),
        "model_win_prob": signals.get("model_win_prob"),
        "diff_signal": signals.get("diff_signal", "NO BET"),
        "diff_side": signals.get("diff_side"),
        "ou_signal": signals.get("ou_signal", "NO BET"),
        "ml_result": _get_ml_result(signals, home_won),
        "diff_result": _get_diff_result(signals, home_won),
        "ou_result": _get_ou_result(signals, total_runs, scoring["ou_model_total"]),
    }


def _get_bt_pitcher(cache_dir: str, game: dict, side: str, prior_season: int) -> dict:
    """Get pitcher profile for backtest. Try starter ID first, fall back to name lookup."""
    import statsapi

    starter_id = game.get(f"{side}_probable_pitcher_id")
    starter_name = game.get(f"{side}_probable_pitcher", "TBD")

    # If we have a direct ID, use it
    if starter_id:
        return _build_bt_pitcher(cache_dir, int(starter_id), starter_name, prior_season)

    # Fall back to name lookup
    if starter_name and starter_name != "TBD":
        try:
            lookup = statsapi.lookup_player(starter_name)
            if lookup:
                pid = lookup[0]["id"]
                return _build_bt_pitcher(cache_dir, pid, starter_name, prior_season)
        except Exception:
            pass

    avg = get_league_avg_pitcher()
    avg["name"] = starter_name
    return avg


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


def _get_diff_result(signals: dict, home_won: bool) -> str:
    """Determine diff signal result."""
    diff_side = signals.get("diff_side")
    if not diff_side or signals.get("diff_signal") == "NO BET":
        return "NO BET"
    if diff_side == "HOME" and home_won:
        return "WIN"
    elif diff_side == "AWAY" and not home_won:
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

    # Diff signal calibration
    diff_buckets = defaultdict(lambda: {"total": 0, "wins": 0})
    for r in results:
        if r.get("diff_signal") == "NO BET":
            continue
        gap = r.get("edge_diff", 0)
        bucket = int(gap // 4) * 4  # 4-point buckets
        diff_buckets[bucket]["total"] += 1
        if r.get("diff_result") == "WIN":
            diff_buckets[bucket]["wins"] += 1

    diff_total = sum(d["total"] for d in diff_buckets.values())
    diff_wins = sum(d["wins"] for d in diff_buckets.values())

    print(f"\nDiff Signal Win Rate by Gap Bucket:")
    print(f"  {'Bucket':>8}  {'Games':>6}  {'Win Rate':>8}")
    for bucket in sorted(diff_buckets.keys()):
        data = diff_buckets[bucket]
        rate = data["wins"] / data["total"] if data["total"] > 0 else 0
        print(f"  {bucket:>5}-{bucket + 3:<3}  {data['total']:>6}  {rate:>7.1%}")
    if diff_total > 0:
        print(f"  {'TOTAL':>8}  {diff_total:>6}  {diff_wins / diff_total:>7.1%}")
    else:
        print("  No diff signals fired")

    # Convergence boost calibration
    conv_data = {"over_total": 0, "over_correct": 0, "under_total": 0, "under_correct": 0}
    for r in results:
        boost = r.get("ou_convergence_boost", 0)
        if boost == 0:
            continue
        actual = r["actual_total"]
        model = r["model_total"]
        if boost > 0:
            conv_data["over_total"] += 1
            if actual > model:
                conv_data["over_correct"] += 1
        elif boost < 0:
            conv_data["under_total"] += 1
            if actual < model:
                conv_data["under_correct"] += 1

    print(f"\nConvergence Boost Accuracy:")
    if conv_data["over_total"] > 0:
        rate = conv_data["over_correct"] / conv_data["over_total"]
        print(f"  OVER boost:   {rate:.1%} ({conv_data['over_correct']}/{conv_data['over_total']})")
    else:
        print(f"  OVER boost:   no games")
    if conv_data["under_total"] > 0:
        rate = conv_data["under_correct"] / conv_data["under_total"]
        print(f"  UNDER boost:  {rate:.1%} ({conv_data['under_correct']}/{conv_data['under_total']})")
    else:
        print(f"  UNDER boost:  no games")

    # Save calibration data
    cal_path = os.path.join(BACKTEST_DIR, f"calibration_{season}.json")
    import json
    cal_data = {
        "season": season,
        "ml_buckets": {str(k): v for k, v in ml_buckets.items()},
        "ou_buckets": {str(k): v for k, v in ou_buckets.items()},
        "rl_data": rl_data,
        "diff_buckets": {str(k): v for k, v in diff_buckets.items()},
        "convergence_data": conv_data,
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
