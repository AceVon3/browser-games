"""
signal.py — Threshold and value check logic.

Applies ML/RL/O/U thresholds, checks value edge, and emits signals.
Handles LOW CONFIDENCE flagging for players on prior_season or league_avg data.
"""

import os
import logging
from typing import Optional, List

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("pipeline")

ML_EDGE_THRESHOLD = int(os.getenv("ML_EDGE_THRESHOLD", 65))
RL_EDGE_THRESHOLD = int(os.getenv("RL_EDGE_THRESHOLD", 75))
OU_EDGE_THRESHOLD = int(os.getenv("OU_EDGE_THRESHOLD", 65))
VALUE_EDGE_MIN = float(os.getenv("VALUE_EDGE_MIN", 0.04))


# ---------------------------------------------------------------------------
# Odds → Implied Probability
# ---------------------------------------------------------------------------

def moneyline_to_implied_prob(line: Optional[int]) -> Optional[float]:
    """Convert American moneyline to implied probability."""
    if line is None:
        return None
    if line < 0:
        return abs(line) / (abs(line) + 100)
    elif line > 0:
        return 100 / (line + 100)
    return 0.5  # even money


def implied_prob_to_moneyline(prob: float) -> int:
    """Convert implied probability to American moneyline."""
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-prob / (1 - prob) * 100)
    else:
        return round((1 - prob) / prob * 100)


# ---------------------------------------------------------------------------
# Model win probability (edge score → win prob)
# ---------------------------------------------------------------------------

def edge_to_win_prob(edge_score: float) -> float:
    """Convert edge score to estimated win probability.

    This is a placeholder mapping until backtesting produces a real
    calibration curve. Linear approximation:
        edge 0 → ~45% (slight underdog, neutral)
        edge 50 → ~55%
        edge 100 → ~70%
        edge -40 → ~35%
    """
    # Linear: prob = 0.45 + (edge / 100) * 0.25
    prob = 0.45 + (edge_score / 100) * 0.25
    return max(0.10, min(0.90, round(prob, 4)))


# ---------------------------------------------------------------------------
# Signal evaluation
# ---------------------------------------------------------------------------

def evaluate_side_signal(
    home_edge: float,
    away_edge: float,
    home_moneyline: Optional[int],
    away_moneyline: Optional[int],
    home_run_line: Optional[int] = None,
    away_run_line: Optional[int] = None,
) -> dict:
    """Evaluate moneyline and run line signals.

    Returns dict with signal details.
    """
    result = {
        "bet_signal": "NO BET",
        "bet_side": None,
        "bet_market": "NO BET",
        "model_win_prob": None,
        "line_win_prob": None,
        "value_edge": None,
        "rl_alert": False,
        "unconfirmed": False,
    }

    # Check for conflicted game (both sides >= 60)
    if home_edge >= 60 and away_edge >= 60:
        result["bet_signal"] = "NO BET"
        result["bet_market"] = "NO BET"
        return result

    # Determine which side has the edge
    # Home pitching edge = home team advantage (home pitcher dominates away lineup)
    # Away pitching edge = away team advantage
    if home_edge >= ML_EDGE_THRESHOLD:
        bet_side = "HOME"
        edge = home_edge
        ml = home_moneyline
        rl = home_run_line
    elif away_edge >= ML_EDGE_THRESHOLD:
        bet_side = "AWAY"
        edge = away_edge
        ml = away_moneyline
        rl = away_run_line
    elif home_edge <= 25:
        # Low pitching edge for home = away batting team has edge
        bet_side = "AWAY"
        edge = home_edge
        ml = away_moneyline
        rl = away_run_line
    elif away_edge <= 25:
        bet_side = "HOME"
        edge = away_edge
        ml = home_moneyline
        rl = home_run_line
    else:
        return result

    model_prob = edge_to_win_prob(edge)
    line_prob = moneyline_to_implied_prob(ml)

    result["model_win_prob"] = model_prob
    result["line_win_prob"] = line_prob
    result["bet_side"] = bet_side

    # Value check
    if line_prob is not None:
        value_edge = model_prob - line_prob
        result["value_edge"] = round(value_edge, 4)

        if edge >= ML_EDGE_THRESHOLD and value_edge >= VALUE_EDGE_MIN:
            result["bet_signal"] = bet_side
            result["bet_market"] = "ML"

            if edge >= RL_EDGE_THRESHOLD:
                result["rl_alert"] = True
                result["bet_market"] = "ML"  # ML fires; RL is alert only
        elif edge >= ML_EDGE_THRESHOLD:
            # Edge met but no value
            result["bet_signal"] = "NO BET"
            result["bet_market"] = "NO BET"
    else:
        # No odds available — show signal as UNCONFIRMED
        if edge >= ML_EDGE_THRESHOLD:
            result["bet_signal"] = bet_side
            result["bet_market"] = "ML"
            result["unconfirmed"] = True
            if edge >= RL_EDGE_THRESHOLD:
                result["rl_alert"] = True

    return result


def evaluate_ou_signal(
    ou_score: float,
    model_total: float,
    ou_line: Optional[float],
    ou_over_odds: Optional[int],
    ou_under_odds: Optional[int],
) -> dict:
    """Evaluate over/under signal.

    Returns dict with O/U signal details.
    """
    result = {
        "ou_signal": "NO BET",
        "ou_direction": None,
        "ou_model_total": model_total,
        "ou_score": ou_score,
        "ou_value_edge": None,
        "unconfirmed": False,
    }

    if ou_line is None:
        if ou_score >= OU_EDGE_THRESHOLD:
            result["ou_signal"] = "OVER" if ou_score >= 50 else "UNDER"
            result["ou_direction"] = result["ou_signal"]
            result["unconfirmed"] = True
        return result

    # Determine direction
    if model_total > ou_line:
        direction = "OVER"
        ou_odds = ou_over_odds
    else:
        direction = "UNDER"
        ou_odds = ou_under_odds

    result["ou_direction"] = direction

    # Calculate value edge
    if ou_odds is not None:
        book_prob = moneyline_to_implied_prob(ou_odds)
        # Model probability: how far model total diverges from line
        diff = abs(model_total - ou_line)
        # Rough probability estimate from total difference
        model_prob = 0.50 + diff * 0.05  # each 0.5 run diff ≈ 2.5% edge
        model_prob = min(0.80, model_prob)

        if book_prob is not None:
            value_edge = model_prob - book_prob
            result["ou_value_edge"] = round(value_edge, 4)

            if ou_score >= OU_EDGE_THRESHOLD and value_edge >= VALUE_EDGE_MIN:
                result["ou_signal"] = direction
            else:
                result["ou_signal"] = "NO BET"
        else:
            if ou_score >= OU_EDGE_THRESHOLD:
                result["ou_signal"] = direction
                result["unconfirmed"] = True
    else:
        if ou_score >= OU_EDGE_THRESHOLD:
            result["ou_signal"] = direction
            result["unconfirmed"] = True

    return result


# ---------------------------------------------------------------------------
# Confidence flagging
# ---------------------------------------------------------------------------

def check_confidence(
    home_pitcher: dict,
    away_pitcher: dict,
    home_lineup: List[dict],
    away_lineup: List[dict],
) -> str:
    """Check data confidence for a game.

    Returns 'LOW CONFIDENCE' if any starter has prior_season or league_avg data,
    otherwise 'NORMAL'.
    """
    low_conf_sources = {"prior_season", "league_avg"}

    for player in [home_pitcher, away_pitcher]:
        if player.get("data_source") in low_conf_sources:
            return "LOW CONFIDENCE"

    for lineup in [home_lineup, away_lineup]:
        for batter in lineup:
            if batter.get("data_source") in low_conf_sources:
                return "LOW CONFIDENCE"

    return "NORMAL"


# ---------------------------------------------------------------------------
# Full game signal evaluation
# ---------------------------------------------------------------------------

def evaluate_game(game_scoring: dict, odds: dict, players: dict) -> dict:
    """Evaluate all signals for a game.

    Args:
        game_scoring: Output from score.score_matchup()
        odds: Dict with moneyline, run line, O/U odds
        players: Dict with home_pitcher, away_pitcher, home_lineup, away_lineup

    Returns:
        Combined signal dict for the game.
    """
    side = evaluate_side_signal(
        home_edge=game_scoring["home_edge_score"],
        away_edge=game_scoring["away_edge_score"],
        home_moneyline=odds.get("home_moneyline"),
        away_moneyline=odds.get("away_moneyline"),
        home_run_line=odds.get("home_run_line"),
        away_run_line=odds.get("away_run_line"),
    )

    ou = evaluate_ou_signal(
        ou_score=game_scoring["ou_score"],
        model_total=game_scoring["ou_model_total"],
        ou_line=odds.get("ou_line"),
        ou_over_odds=odds.get("ou_over_odds"),
        ou_under_odds=odds.get("ou_under_odds"),
    )

    confidence = check_confidence(
        players.get("home_pitcher", {}),
        players.get("away_pitcher", {}),
        players.get("home_lineup", []),
        players.get("away_lineup", []),
    )

    return {
        **side,
        **ou,
        "data_confidence": confidence,
    }
