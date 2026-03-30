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
DIFF_EDGE_THRESHOLD = int(os.getenv("DIFF_EDGE_THRESHOLD", 12))
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
    home_spread_point: Optional[float] = None,
    away_spread_point: Optional[float] = None,
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
        "rl_plus_signal": "NO BET",
        "rl_plus_prob": None,
        "rl_plus_line_prob": None,
        "rl_plus_value_edge": None,
        "unconfirmed": False,
    }

    # Check for conflicted game (both sides >= 60)
    if home_edge >= 60 and away_edge >= 60:
        result["bet_signal"] = "NO BET"
        result["bet_market"] = "NO BET"
        return result

    # Determine which side has the edge
    # Two paths:
    #   1. High edge (>= threshold): that pitcher dominates, bet that side
    #   2. Low edge (<= 25): that pitcher is getting crushed, bet the OTHER side
    #      Invert the score so signal strength scales correctly:
    #      edge 25 → inverted 0 (barely qualifies), edge 0 → inverted 25 (max)
    inverted = False

    # Helper: resolve the +1.5 run line price for a given side.
    # The Odds API always puts -1.5 on the favorite and +1.5 on the underdog.
    # If our bet side is the underdog (spread_point > 0), their price IS the
    # +1.5 price.  If our bet side is the favorite (spread_point < 0), the
    # +1.5 price for them doesn't exist in the API — set to None so the
    # +1.5 signal won't fire (favorites on +1.5 have no real market).
    def _resolve_rl_plus(side: str) -> Optional[int]:
        if side == "HOME":
            pt = home_spread_point
            return home_run_line if pt is not None and pt > 0 else None
        else:
            pt = away_spread_point
            return away_run_line if pt is not None and pt > 0 else None

    if home_edge >= ML_EDGE_THRESHOLD:
        bet_side = "HOME"
        edge = home_edge
        ml = home_moneyline
        rl = _resolve_rl_plus("HOME")
    elif away_edge >= ML_EDGE_THRESHOLD:
        bet_side = "AWAY"
        edge = away_edge
        ml = away_moneyline
        rl = _resolve_rl_plus("AWAY")
    elif home_edge <= 25:
        # Home pitcher getting crushed → bet AWAY
        bet_side = "AWAY"
        edge = 25 - home_edge  # invert: lower score = stronger signal
        ml = away_moneyline
        rl = _resolve_rl_plus("AWAY")
        inverted = True
    elif away_edge <= 25:
        # Away pitcher getting crushed → bet HOME
        bet_side = "HOME"
        edge = 25 - away_edge
        ml = home_moneyline
        rl = _resolve_rl_plus("HOME")
        inverted = True
    else:
        # No side qualifies — compute value for the stronger side for display
        if home_edge >= away_edge and home_moneyline is not None:
            result["bet_side"] = "HOME"
            result["model_win_prob"] = edge_to_win_prob(home_edge)
            result["line_win_prob"] = moneyline_to_implied_prob(home_moneyline)
            if result["line_win_prob"] is not None:
                result["value_edge"] = round(result["model_win_prob"] - result["line_win_prob"], 4)
        elif away_moneyline is not None:
            result["bet_side"] = "AWAY"
            result["model_win_prob"] = edge_to_win_prob(away_edge)
            result["line_win_prob"] = moneyline_to_implied_prob(away_moneyline)
            if result["line_win_prob"] is not None:
                result["value_edge"] = round(result["model_win_prob"] - result["line_win_prob"], 4)
        return result

    # For inverted signals, use a separate win prob mapping:
    # edge 0 (barely bad) → ~52%, edge 25 (terrible) → ~65%
    if inverted:
        model_prob = 0.52 + (edge / 25) * 0.13
        model_prob = max(0.10, min(0.90, round(model_prob, 4)))
    else:
        model_prob = edge_to_win_prob(edge)

    line_prob = moneyline_to_implied_prob(ml)

    result["model_win_prob"] = model_prob
    result["line_win_prob"] = line_prob
    result["bet_side"] = bet_side

    # For inverted signals, any edge <= 25 qualifies (inverted edge >= 0)
    # The value edge check is the real gatekeeper
    INVERTED_THRESHOLD = 0

    # Value check
    if line_prob is not None:
        value_edge = model_prob - line_prob
        result["value_edge"] = round(value_edge, 4)

        if inverted:
            if edge >= INVERTED_THRESHOLD and value_edge >= VALUE_EDGE_MIN:
                result["bet_signal"] = bet_side
                result["bet_market"] = "ML"
        else:
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
        threshold = INVERTED_THRESHOLD if inverted else ML_EDGE_THRESHOLD
        if edge >= threshold:
            result["bet_signal"] = bet_side
            result["bet_market"] = "ML"
            result["unconfirmed"] = True
            if not inverted and edge >= RL_EDGE_THRESHOLD:
                result["rl_alert"] = True

    # +1.5 Run Line evaluation
    # When the model identifies a side, also check if +1.5 offers value.
    # +1.5 probability = win_prob + (lose_prob * one_run_loss_rate)
    # ~30% of MLB losses are by exactly 1 run historically.
    ONE_RUN_LOSS_RATE = 0.30

    if model_prob is not None and rl is not None:
        rl_plus_prob = model_prob + ((1 - model_prob) * ONE_RUN_LOSS_RATE)
        rl_plus_prob = max(0.10, min(0.95, round(rl_plus_prob, 4)))
        rl_line_prob = moneyline_to_implied_prob(rl)

        result["rl_plus_prob"] = rl_plus_prob
        result["rl_plus_line_prob"] = rl_line_prob

        if rl_line_prob is not None:
            rl_plus_value = rl_plus_prob - rl_line_prob
            result["rl_plus_value_edge"] = round(rl_plus_value, 4)

            if rl_plus_value >= VALUE_EDGE_MIN:
                result["rl_plus_signal"] = bet_side

    return result


def diff_to_win_prob(diff: float) -> float:
    """Convert edge score differential to estimated win probability.

    The differential captures a compounding advantage: one side's pitcher
    handles the opposing lineup better AND their lineup handles the opposing
    pitcher better. Conservative mapping:
        diff 12 → ~53.5%
        diff 16 → ~55%
        diff 20 → ~56.5%
        diff 30 → ~60%
    """
    prob = 0.50 + (diff / 80) * 0.20
    return max(0.50, min(0.70, round(prob, 4)))


def evaluate_diff_signal(
    home_edge: float,
    away_edge: float,
    home_moneyline: Optional[int],
    away_moneyline: Optional[int],
) -> dict:
    """Evaluate differential signal when neither side hits ML threshold.

    Fires when the gap between the two edge scores is >= DIFF_EDGE_THRESHOLD,
    indicating one side has a compounding advantage on both sides of the ball.
    Only fires if no standard ML/inverted signal already triggered.

    Returns dict with diff signal details.
    """
    result = {
        "diff_signal": "NO BET",
        "diff_side": None,
        "diff_gap": None,
        "diff_model_prob": None,
        "diff_line_prob": None,
        "diff_value_edge": None,
        "diff_unconfirmed": False,
    }

    diff = abs(home_edge - away_edge)
    result["diff_gap"] = round(diff, 1)

    if diff < DIFF_EDGE_THRESHOLD:
        return result

    # Bet the side whose pitcher has the higher edge score
    if home_edge > away_edge:
        bet_side = "HOME"
        ml = home_moneyline
    else:
        bet_side = "AWAY"
        ml = away_moneyline

    model_prob = diff_to_win_prob(diff)
    line_prob = moneyline_to_implied_prob(ml)

    result["diff_side"] = bet_side
    result["diff_model_prob"] = model_prob
    result["diff_line_prob"] = line_prob

    if line_prob is not None:
        value_edge = model_prob - line_prob
        result["diff_value_edge"] = round(value_edge, 4)

        if value_edge >= VALUE_EDGE_MIN:
            result["diff_signal"] = bet_side
    else:
        # No odds — show as unconfirmed
        result["diff_signal"] = bet_side
        result["diff_unconfirmed"] = True

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
        home_spread_point=odds.get("home_spread_point"),
        away_spread_point=odds.get("away_spread_point"),
    )

    # Differential signal — only when standard side signal didn't fire
    diff = {"diff_signal": "NO BET", "diff_side": None, "diff_gap": None,
            "diff_model_prob": None, "diff_line_prob": None,
            "diff_value_edge": None, "diff_unconfirmed": False}
    if side.get("bet_signal") in (None, "NO BET"):
        diff = evaluate_diff_signal(
            home_edge=game_scoring["home_edge_score"],
            away_edge=game_scoring["away_edge_score"],
            home_moneyline=odds.get("home_moneyline"),
            away_moneyline=odds.get("away_moneyline"),
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
        **diff,
        **ou,
        "data_confidence": confidence,
    }
