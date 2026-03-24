"""
closing_line.py — Closing Line Value (CLV) tracking.

Fetches closing lines ~5 min before first pitch for ML and O/U.
Calculates CLV for results_log.csv.
"""

import os
import csv
import logging
from datetime import datetime, date
from typing import Optional, List

from src.fetch import fetch_odds
from src.signal import moneyline_to_implied_prob

logger = logging.getLogger("pipeline")

RESULTS_LOG = os.path.join(os.path.dirname(__file__), "..", "results_log.csv")

RESULTS_COLUMNS = [
    "date", "game_id", "signal_type", "bet_side",
    "home_team", "away_team", "home_starter", "away_starter",
    "edge_score", "ou_score", "home_bullpen_score", "away_bullpen_score",
    "data_confidence", "moneyline", "run_line_odds", "ou_line", "ou_odds",
    "model_prob", "line_implied_prob", "value_edge", "signal_version",
    "closing_line", "closing_line_value", "result", "profit_loss", "notes",
]


def fetch_closing_lines() -> List[dict]:
    """Fetch closing lines for all games.

    Should be called ~5 min before first pitch.
    Returns parsed odds list from The Odds API.
    """
    logger.info("Fetching closing lines...")
    return fetch_odds()


def calculate_clv(opening_line: Optional[int], closing_line: Optional[int]) -> Optional[float]:
    """Calculate Closing Line Value.

    CLV = closing_implied_prob - opening_implied_prob
    Positive = market moved in your direction (good).
    """
    if opening_line is None or closing_line is None:
        return None

    opening_prob = moneyline_to_implied_prob(opening_line)
    closing_prob = moneyline_to_implied_prob(closing_line)

    if opening_prob is None or closing_prob is None:
        return None

    return round(closing_prob - opening_prob, 4)


def log_signal(game: dict, signal_type: str, pass_version: str = "final") -> None:
    """Append a signal row to results_log.csv.

    One row per signal per game. If a game fires both ML and O/U,
    call this function twice with different signal_types.
    """
    file_exists = os.path.exists(RESULTS_LOG)

    row = {
        "date": game.get("date", date.today().isoformat()),
        "game_id": game.get("game_id", ""),
        "signal_type": signal_type,
        "bet_side": _get_bet_side(game, signal_type),
        "home_team": game.get("home_abbrev", game.get("home_team", "")),
        "away_team": game.get("away_abbrev", game.get("away_team", "")),
        "home_starter": game.get("home_starter_name", ""),
        "away_starter": game.get("away_starter_name", ""),
        "edge_score": game.get("home_edge_score", game.get("away_edge_score", "")),
        "ou_score": game.get("ou_score", "") if signal_type in ("OVER", "UNDER") else "",
        "home_bullpen_score": game.get("home_bullpen_score", ""),
        "away_bullpen_score": game.get("away_bullpen_score", ""),
        "data_confidence": game.get("data_confidence", "NORMAL"),
        "moneyline": _get_moneyline(game, signal_type),
        "run_line_odds": _get_run_line(game, signal_type),
        "ou_line": game.get("ou_line", "") if signal_type in ("OVER", "UNDER") else "",
        "ou_odds": _get_ou_odds(game, signal_type),
        "model_prob": game.get("model_win_prob", game.get("ou_model_prob", "")),
        "line_implied_prob": game.get("line_win_prob", ""),
        "value_edge": game.get("value_edge", game.get("ou_value_edge", "")),
        "signal_version": pass_version,
        "closing_line": "",
        "closing_line_value": "",
        "result": "PENDING",
        "profit_loss": "",
        "notes": _build_notes(game),
    }

    with open(RESULTS_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def update_closing_lines(date_str: str, closing_odds: List[dict]) -> None:
    """Update results_log.csv with closing lines and CLV for a given date.

    Matches games by home_team + away_team.
    """
    if not os.path.exists(RESULTS_LOG):
        return

    rows = []
    with open(RESULTS_LOG, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    # Build lookup from closing odds
    closing_lookup = {}
    for odds in closing_odds:
        key = (odds.get("home_team", ""), odds.get("away_team", ""))
        closing_lookup[key] = odds

    updated = False
    for row in rows:
        if row["date"] != date_str:
            continue
        if row.get("closing_line"):
            continue  # already filled

        key = (row["home_team"], row["away_team"])
        closing = closing_lookup.get(key)
        if not closing:
            continue

        signal_type = row["signal_type"]
        if signal_type == "ML":
            side = row["bet_side"]
            if side == row["home_team"]:
                cl = closing.get("home_moneyline")
                opening = row.get("moneyline")
            else:
                cl = closing.get("away_moneyline")
                opening = row.get("moneyline")
            row["closing_line"] = str(cl) if cl else ""
            if opening and cl:
                row["closing_line_value"] = str(
                    calculate_clv(int(opening), int(cl)) or ""
                )
            updated = True

        elif signal_type in ("OVER", "UNDER"):
            cl = closing.get("ou_line")
            row["closing_line"] = str(cl) if cl else ""
            updated = True

    if updated:
        with open(RESULTS_LOG, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RESULTS_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)


def _get_bet_side(game: dict, signal_type: str) -> str:
    if signal_type in ("OVER", "UNDER"):
        return signal_type
    return game.get("bet_side", "")


def _get_moneyline(game: dict, signal_type: str) -> str:
    if signal_type in ("OVER", "UNDER"):
        return ""
    side = game.get("bet_side", "")
    if side == "HOME":
        return str(game.get("home_moneyline", ""))
    elif side == "AWAY":
        return str(game.get("away_moneyline", ""))
    return ""


def _get_run_line(game: dict, signal_type: str) -> str:
    if signal_type != "RL_ALERT":
        return ""
    side = game.get("bet_side", "")
    if side == "HOME":
        return str(game.get("home_run_line", ""))
    elif side == "AWAY":
        return str(game.get("away_run_line", ""))
    return ""


def _get_ou_odds(game: dict, signal_type: str) -> str:
    if signal_type == "OVER":
        return str(game.get("ou_over_odds", ""))
    elif signal_type == "UNDER":
        return str(game.get("ou_under_odds", ""))
    return ""


def _build_notes(game: dict) -> str:
    notes = []
    if game.get("data_confidence") == "LOW CONFIDENCE":
        notes.append("LOW CONFIDENCE")
    if game.get("starter_changed"):
        notes.append("starter changed pregame")
    if game.get("unconfirmed"):
        notes.append("odds unavailable — UNCONFIRMED")
    return "; ".join(notes)
