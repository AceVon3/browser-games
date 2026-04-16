"""
score.py — 4-component matchup scoring engine.

Components (raw → weighted):
  1. Zone Alignment Score (0-40 raw, weighted to 49%)
  2. Pitch Type Mismatch Score (0-30 raw, weighted to 22%)
  3. Walk Rate Interaction Score (0-15 raw, weighted to 19%)
  4. Handedness Adjustment (0-15 raw, weighted to 10%)

Plus: park/weather adjustments, bullpen modifier, O/U model total, O/U score.
"""

import logging
from datetime import date
from typing import List, Tuple

from src.bullpen import calculate_bullpen_modifier
from src.weather import calculate_weather_adjustment

logger = logging.getLogger("pipeline")

# Lineup position multipliers
POSITION_WEIGHTS = {
    1: 1.25, 2: 1.25, 3: 1.25, 4: 1.25,
    5: 1.0, 6: 1.0,
    7: 0.75, 8: 0.75, 9: 0.75,
}


# ---------------------------------------------------------------------------
# Component 1: Zone Alignment Score (0-40 pts)
# ---------------------------------------------------------------------------

def zone_alignment_score(pitcher: dict, lineup: List[dict]) -> float:
    """Score how well the pitcher's attack zones exploit batter weaknesses.

    Pitcher top zones overlap batter cold zones → positive for pitcher.
    Pitcher zones overlap batter hot zones → negative (batter edge).
    """
    pitcher_zones = set(pitcher.get("preferred_zones", []))
    if not pitcher_zones:
        return 20.0  # neutral

    total_score = 0.0
    total_weight = 0.0

    for i, batter in enumerate(lineup[:9], 1):
        weight = POSITION_WEIGHTS.get(i, 1.0)
        cold = set(batter.get("vulnerable_zones", []))
        hot = set(batter.get("preferred_hit_zones", []))

        cold_overlap = len(pitcher_zones & cold)
        hot_overlap = len(pitcher_zones & hot)

        batter_score = (cold_overlap * 3) - (hot_overlap * 3)
        total_score += batter_score * weight
        total_weight += weight

    if total_weight == 0:
        return 20.0

    # Normalize to 0-40 range
    raw = total_score / total_weight
    # Observed raw range is roughly -3 to +3 with real lineups
    # (original ±9 assumed max overlap which rarely happens)
    normalized = ((raw + 3) / 6) * 40
    return max(0, min(40, round(normalized, 2)))


# ---------------------------------------------------------------------------
# Component 2: Pitch Type Mismatch Score (0-30 pts)
# ---------------------------------------------------------------------------

def pitch_type_mismatch_score(pitcher: dict, lineup: List[dict]) -> float:
    """Score how well the pitcher's pitch mix exploits batter weaknesses.

    Weighted approach: for each pitch type the pitcher throws, multiply
    usage % by how much the batter struggles against that pitch (inverted
    wOBA). Sum across all pitch types to get a composite mismatch score
    per batter, then average across the lineup with position weights.

    League average wOBA is ~0.320. A batter with 0.200 wOBA against a pitch
    is weak; 0.450 is strong. We invert so weakness = positive for pitcher.
    """
    pitch_mix = pitcher.get("pitch_mix", {})
    if not pitch_mix:
        return 15.0  # neutral

    LEAGUE_AVG_WOBA = 0.320

    total_score = 0.0
    total_weight = 0.0

    for i, batter in enumerate(lineup[:9], 1):
        weight = POSITION_WEIGHTS.get(i, 1.0)
        perf = batter.get("pitch_type_perf", {})

        if not perf:
            # No pitch type data for this batter — neutral
            total_weight += weight
            continue

        # For each pitch the pitcher throws, score how much the batter
        # struggles against it, weighted by usage %
        batter_mismatch = 0.0
        usage_matched = 0.0

        for ptype, usage in pitch_mix.items():
            batter_woba = perf.get(ptype)
            if batter_woba is None:
                # Batter has no data against this pitch type — assume neutral
                batter_woba = LEAGUE_AVG_WOBA

            # Invert: lower batter wOBA = pitcher advantage
            # (league_avg - batter_woba) is positive when batter is weak
            advantage = LEAGUE_AVG_WOBA - batter_woba
            batter_mismatch += usage * advantage
            usage_matched += usage

        # batter_mismatch ranges roughly from -0.15 (batter crushes everything)
        # to +0.15 (batter is weak against this mix)
        total_score += batter_mismatch * weight
        total_weight += weight

    if total_weight == 0:
        return 15.0

    # Average mismatch across lineup
    avg_mismatch = total_score / total_weight
    # Observed avg_mismatch ranges roughly ±0.05 with real lineups
    # (original ±0.12 assumed extremes that rarely occur)
    normalized = 15.0 + (avg_mismatch / 0.05) * 15.0
    return max(0, min(30, round(normalized, 2)))


# ---------------------------------------------------------------------------
# Component 3: Walk Rate Interaction Score (0-15 pts)
# ---------------------------------------------------------------------------

def walk_rate_score(pitcher: dict, lineup: List[dict]) -> float:
    """Score walk rate interaction.

    Lower pitcher BB% vs higher lineup BB% = pitcher advantage.
    Observed BB% diffs are typically ±0.03, so we scale by 250 to
    spread the output across the full 0-15 range.
    """
    pitcher_bb = pitcher.get("bb_pct", 0.082)

    if not lineup:
        return 7.5  # neutral

    lineup_bb = sum(b.get("bb_pct", 0.082) for b in lineup[:9]) / min(len(lineup), 9)

    # Positive = pitcher walks less than lineup draws = pitcher advantage
    raw = (lineup_bb - pitcher_bb) * 250
    score = max(-7.5, min(7.5, raw))

    # Shift to 0-15 range
    return round(score + 7.5, 2)


# ---------------------------------------------------------------------------
# Component 4: Handedness Adjustment (0-15 pts)
# ---------------------------------------------------------------------------

def handedness_score(pitcher: dict, lineup: List[dict]) -> float:
    """Score handedness advantage.

    6+ batters facing pitcher from his dominant split side → +5 pts for pitcher.
    """
    pitcher_hand = pitcher.get("hand", "R")
    if not lineup:
        return 7.5  # neutral

    # Dominant split: RHP is better vs RHH, LHP is better vs LHH
    dominant_side = pitcher_hand
    same_side_count = sum(
        1 for b in lineup[:9]
        if b.get("hand", "R") == dominant_side
        or (b.get("hand") == "S" and dominant_side == "R")  # switch hitters bat L vs RHP
    )

    # Base score centered at 7.5
    score = 7.5
    if same_side_count >= 6:
        score += 5.0
    elif same_side_count >= 4:
        score += 2.5
    elif same_side_count <= 2:
        score -= 3.0

    return max(0, min(15, round(score, 2)))


# ---------------------------------------------------------------------------
# Combined Edge Score
# ---------------------------------------------------------------------------

def calculate_edge_score(pitcher: dict, lineup: List[dict]) -> dict:
    """Calculate the raw 4-component edge score for a pitcher vs lineup.

    Returns dict with component scores and raw total.
    Raw Edge Score range: 0 to 100.
    Higher = pitching team advantage.
    """
    # Component weights tuned via 2025 backtest ROI sweep (176k combos)
    # (zone 49%, pitch 22%, walk 19%, hand 10%)
    zone = zone_alignment_score(pitcher, lineup) * (49 / 40)
    pitch = pitch_type_mismatch_score(pitcher, lineup) * (22 / 30)
    walk = walk_rate_score(pitcher, lineup) * (19 / 15)
    hand = handedness_score(pitcher, lineup) * (10 / 15)

    raw = zone + pitch + walk + hand
    # Clamp to spec range
    raw = max(0, min(100, raw))

    return {
        "zone_alignment": zone,
        "pitch_mismatch": pitch,
        "walk_rate": walk,
        "handedness": hand,
        "raw_edge_score": round(raw, 2),
    }


# ---------------------------------------------------------------------------
# Park & Weather Adjusted Score
# ---------------------------------------------------------------------------

def apply_park_factor(raw_score: float, park_factor: float) -> float:
    """Apply park factor adjustment.

    park_factor: decimal (1.0 = neutral, 1.05 = hitter-friendly).
    Adjustment capped at ±10 pts.
    """
    adj = raw_score * (1 + (1.0 - park_factor) * 0.5)
    # Cap the adjustment itself at ±10
    adjustment = adj - raw_score
    adjustment = max(-10, min(10, adjustment))
    return round(raw_score + adjustment, 2)


def apply_weather(park_adjusted: float, weather: dict) -> Tuple[float, dict]:
    """Apply weather adjustment after park factor.

    Returns (adjusted_score, weather_adj_dict).
    """
    adj = calculate_weather_adjustment(weather)
    edge_adj = adj["edge_adj"]
    return round(park_adjusted + edge_adj, 2), adj


# ---------------------------------------------------------------------------
# Full Matchup Score
# ---------------------------------------------------------------------------

def score_matchup(
    home_pitcher: dict,
    away_pitcher: dict,
    home_lineup: List[dict],
    away_lineup: List[dict],
    home_bullpen_score: float,
    away_bullpen_score: float,
    park_factor: float,
    weather: dict,
    home_offense_score: float = 50.0,
    away_offense_score: float = 50.0,
    home_bp_workload: float = 0.0,
    away_bp_workload: float = 0.0,
) -> dict:
    """Score a full game matchup.

    Calculates edge scores for both sides, applies park/weather/bullpen
    adjustments, and computes O/U model total and O/U score.

    Returns comprehensive game scoring dict.
    """
    # Edge scores: home pitcher vs away lineup, away pitcher vs home lineup
    home_pitching = calculate_edge_score(home_pitcher, away_lineup)
    away_pitching = calculate_edge_score(away_pitcher, home_lineup)

    # Park adjustments
    home_park_adj = apply_park_factor(home_pitching["raw_edge_score"], park_factor)
    away_park_adj = apply_park_factor(away_pitching["raw_edge_score"], park_factor)

    # Weather adjustments
    home_weather_adj, weather_adj = apply_weather(home_park_adj, weather)
    away_weather_adj, _ = apply_weather(away_park_adj, weather)

    # Bullpen modifiers (net differential)
    home_bp_mod = calculate_bullpen_modifier(home_bullpen_score, away_bullpen_score)
    away_bp_mod = calculate_bullpen_modifier(away_bullpen_score, home_bullpen_score)

    # Home field advantage: +3 to home edge based on 2025 backtest
    # showing home-side bets at 57% vs away-side at 47-49%
    HOME_FIELD_ADJ = 3
    home_final = round(home_weather_adj + home_bp_mod + HOME_FIELD_ADJ, 2)
    away_final = round(away_weather_adj + away_bp_mod, 2)

    # O/U scoring
    ou = calculate_ou(
        home_final, away_final,
        park_factor, weather_adj,
        home_bullpen_score, away_bullpen_score,
        home_offense_score, away_offense_score,
        home_bp_workload, away_bp_workload,
    )

    return {
        "home_edge_score": home_final,
        "away_edge_score": away_final,
        "home_edge_components": home_pitching,
        "away_edge_components": away_pitching,
        "home_bullpen_score": home_bullpen_score,
        "away_bullpen_score": away_bullpen_score,
        "home_bullpen_modifier": home_bp_mod,
        "away_bullpen_modifier": away_bp_mod,
        "park_factor": park_factor,
        "park_weather_adjustment": weather_adj["edge_adj"],
        "weather_wind_mph": weather.get("wind_mph", 0),
        "weather_wind_dir": weather.get("wind_dir", "calm"),
        "weather_temp_f": weather.get("temp_f", 72),
        "ou_model_total": ou["model_total"],
        "ou_score": ou["ou_score"],
        "ou_convergence_boost": ou["convergence_boost"],
    }


# ---------------------------------------------------------------------------
# Over/Under Scoring
# ---------------------------------------------------------------------------

def calculate_ou(
    home_edge: float,
    away_edge: float,
    park_factor: float,
    weather_adj: dict,
    home_bullpen_score: float,
    away_bullpen_score: float,
    home_offense_score: float = 50.0,
    away_offense_score: float = 50.0,
    home_bp_workload: float = 0.0,
    away_bp_workload: float = 0.0,
) -> dict:
    """Calculate O/U model total and O/U directional score.

    Model total baseline 8.7 (2025 MLB avg ~8.9, slight conservative lean).
    O/U score centered at 50 — above = OVER lean, below = UNDER lean.
    """
    avg_edge = (home_edge + away_edge) / 2

    # --- Model Total ---
    # Baseline: 8.7 (closer to actual MLB avg than old 9.0)
    model_total = 8.7

    # Pitcher dominance: stronger pitching = fewer runs
    # avg_edge 50 = neutral, 70 = dominant (-1.0 run), 30 = weak (+1.0 run)
    model_total -= ((avg_edge - 50) / 50) * 2.5

    # Park factor
    model_total += (park_factor - 1.0) * 3.0

    # Weather
    model_total += weather_adj.get("run_adj", 0.0)

    # Bullpen: linear, centered at 50
    avg_bullpen = (home_bullpen_score + away_bullpen_score) / 2
    model_total -= ((avg_bullpen - 50) / 40) * 0.5  # 70 = -0.25, 30 = +0.25

    # Spread: lopsided matchups are volatile but NOT directionally biased.
    # Removed the old one-directional spread bonus that always added runs.

    # --- O/U Score (baseline 50) ---
    ou_score = 50.0

    # Edge score factor: asymmetric — UNDER side uses full 20, OVER side dampened to 14
    # High avg edge (dominant pitching) → UNDER, low avg edge → OVER
    raw_edge = -((avg_edge - 50) / 50)
    if raw_edge > 0:
        # OVER direction — dampen (weak pitching doesn't boost scoring linearly)
        edge_factor = raw_edge * 14
    else:
        # UNDER direction — full weight
        edge_factor = raw_edge * 20
    ou_score += edge_factor

    # Park factor: -10 to +10
    park_ou = (park_factor - 1.0) * 100
    park_ou = max(-10, min(10, park_ou))
    ou_score += park_ou

    # Wind: -8 to +8
    wind_adj = weather_adj.get("run_adj", 0.0)
    wind_ou = max(-8, min(8, wind_adj * 16))
    ou_score += wind_ou

    # Bullpen: linear centered at 50, range -10 to +10
    # 70 = strong pen = -10 (UNDER), 30 = weak pen = +10 (OVER)
    bp_ou = -((avg_bullpen - 50) / 20) * 10
    bp_ou = max(-10, min(10, bp_ou))
    ou_score += bp_ou

    # Spread factor: lopsided matchups add volatility in BOTH directions.
    # High spread with low avg edge (one bad pitcher) → mild OVER
    # High spread with high avg edge (one dominant pitcher) → mild UNDER
    # This replaces the old one-directional +0 to +5 OVER bonus.
    spread = abs(home_edge - away_edge)
    if avg_edge >= 55:
        # Dominant side drives the total down
        spread_factor = -(spread / 50) * 3  # max -3 UNDER
    elif avg_edge <= 45:
        # Weak side drives the total up
        spread_factor = (spread / 50) * 3   # max +3 OVER
    else:
        # Neutral — spread adds slight volatility but no direction
        spread_factor = 0
    ou_score += spread_factor

    # Offensive strength factor: strong lineups push OVER, weak push UNDER
    # Score centered at 50 — range ±8
    avg_offense = (home_offense_score + away_offense_score) / 2
    offense_ou = ((avg_offense - 50) / 50) * 8
    offense_ou = max(-8, min(8, offense_ou))
    ou_score += offense_ou
    # Also adjust model total: ±0.5 runs
    model_total += ((avg_offense - 50) / 50) * 0.5

    # Bullpen fatigue factor: exhausted pens push OVER
    # workload_3day >= 10 IP for both pens = significant fatigue
    avg_workload = (home_bp_workload + away_bp_workload) / 2
    if avg_workload >= 10:
        fatigue_ou = 6.0   # both pens gassed → strong OVER boost
        model_total += 0.4
    elif avg_workload >= 7:
        fatigue_ou = 3.0   # moderate fatigue
        model_total += 0.2
    else:
        fatigue_ou = 0.0
    ou_score += fatigue_ou

    # Pitcher convergence boost:
    # Both dominant → pitchers' duel (UNDER)
    # Both weak → high-scoring game (OVER)
    convergence_boost = 0.0
    low_edge = min(home_edge, away_edge)
    high_edge = max(home_edge, away_edge)

    if low_edge >= 60:
        convergence_boost = -15.0  # both dominant → strong UNDER
    elif low_edge >= 55 and high_edge >= 55:
        convergence_boost = -8.0   # both solid → mild UNDER
    elif high_edge <= 40:
        convergence_boost = 3.0    # both weak → mild OVER (dampened — weak pitchers get pulled early)
    elif high_edge <= 45 and low_edge <= 40:
        convergence_boost = 2.0    # both below avg → slight OVER

    ou_score += convergence_boost

    # Apply convergence to model total
    if convergence_boost != 0:
        model_total += convergence_boost / 15 * 0.5

    model_total = round(max(4.0, min(15.0, model_total)), 1)
    ou_score = max(0, min(100, round(ou_score, 1)))

    return {
        "model_total": model_total,
        "ou_score": ou_score,
        "convergence_boost": convergence_boost,
    }
