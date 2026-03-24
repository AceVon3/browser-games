"""
bullpen.py — Team bullpen scoring.

Builds a 0-100 composite score from ERA, WHIP, recent workload,
and high-leverage reliever availability.
"""

import os
import json
import logging
from datetime import date, datetime
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from src.fetch import fetch_reliever_workload, save_cached_profile, load_cached_profile

load_dotenv()

logger = logging.getLogger("pipeline")

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Component weights for bullpen score
WEIGHT_ERA = 0.35
WEIGHT_WHIP = 0.25
WEIGHT_WORKLOAD = 0.25
WEIGHT_HIGH_LEV = 0.15

# League-average benchmarks for normalization
LEAGUE_AVG_ERA = 4.00
LEAGUE_AVG_WHIP = 1.30
MAX_WORKLOAD_3DAY = 15.0  # innings — beyond this bullpen is exhausted


def build_bullpen_profile(
    team_abbrev: str,
    reliever_stats: Optional[pd.DataFrame] = None,
    workload_3day: Optional[float] = None,
    high_lev_available: bool = True,
) -> dict:
    """Build a bullpen profile for a team.

    Args:
        team_abbrev: Team abbreviation (e.g. 'NYY')
        reliever_stats: DataFrame with reliever-level stats (ERA, WHIP, IP).
            If None, attempts to use cached data.
        workload_3day: Pre-calculated reliever IP over last 3 days.
            If None, fetches from MLB Stats API.
        high_lev_available: Whether top relievers are available.

    Returns:
        Bullpen profile dict with composite score.
    """
    # Get workload if not provided
    if workload_3day is None:
        workload_3day = fetch_reliever_workload(team_abbrev, days=3)

    # Aggregate ERA and WHIP from reliever stats
    if reliever_stats is not None and not reliever_stats.empty:
        bullpen_era, bullpen_whip = _aggregate_reliever_stats(reliever_stats)
    else:
        # Try cached
        cached = load_cached_profile("bullpens", team_abbrev)
        if cached:
            bullpen_era = cached.get("bullpen_era", LEAGUE_AVG_ERA)
            bullpen_whip = cached.get("bullpen_whip", LEAGUE_AVG_WHIP)
        else:
            bullpen_era = LEAGUE_AVG_ERA
            bullpen_whip = LEAGUE_AVG_WHIP

    # Calculate composite score
    bullpen_score = _calculate_bullpen_score(
        bullpen_era, bullpen_whip, workload_3day, high_lev_available
    )

    profile = {
        "team_id": team_abbrev,
        "bullpen_era": round(bullpen_era, 2),
        "bullpen_whip": round(bullpen_whip, 2),
        "workload_3day": round(workload_3day, 1),
        "high_lev_available": high_lev_available,
        "bullpen_score": round(bullpen_score, 1),
        "last_updated": date.today().isoformat(),
    }

    save_cached_profile("bullpens", team_abbrev, profile)
    return profile


def _aggregate_reliever_stats(df: pd.DataFrame) -> Tuple[float, float]:
    """Aggregate ERA and WHIP from individual reliever stats, weighted by IP.

    Expects DataFrame with columns: ERA (or era), WHIP (or whip), IP (or ip).
    Filters to IP < 80 as a reliever proxy.
    """
    # Normalize column names
    col_map = {c.lower(): c for c in df.columns}
    era_col = col_map.get("era", "ERA")
    whip_col = col_map.get("whip", "WHIP")
    ip_col = col_map.get("ip", "IP")

    # Filter to relievers (IP < 80)
    relievers = df[df[ip_col] < 80].copy()
    if relievers.empty:
        return LEAGUE_AVG_ERA, LEAGUE_AVG_WHIP

    # IP-weighted averages
    total_ip = relievers[ip_col].sum()
    if total_ip == 0:
        return LEAGUE_AVG_ERA, LEAGUE_AVG_WHIP

    weighted_era = (relievers[era_col] * relievers[ip_col]).sum() / total_ip
    weighted_whip = (relievers[whip_col] * relievers[ip_col]).sum() / total_ip

    return weighted_era, weighted_whip


def _calculate_bullpen_score(
    era: float, whip: float, workload_3day: float, high_lev_available: bool
) -> float:
    """Calculate the 0-100 composite bullpen score.

    Components:
        ERA (35%): Lower ERA = higher score
        WHIP (25%): Lower WHIP = higher score
        Workload (25%): Higher recent workload = lower score
        High-leverage availability (15%): Unavailable = sharp drop
    """
    # ERA component (0-100): best ERA ~2.0 = 100, worst ~6.0 = 0
    era_score = max(0, min(100, (6.0 - era) / 4.0 * 100))

    # WHIP component (0-100): best WHIP ~0.90 = 100, worst ~1.70 = 0
    whip_score = max(0, min(100, (1.70 - whip) / 0.80 * 100))

    # Workload component (0-100): 0 IP = 100 (fully rested), MAX_WORKLOAD = 0
    workload_score = max(0, min(100, (1.0 - workload_3day / MAX_WORKLOAD_3DAY) * 100))

    # High-leverage component: available = 100, unavailable = 0
    high_lev_score = 100 if high_lev_available else 0

    composite = (
        WEIGHT_ERA * era_score
        + WEIGHT_WHIP * whip_score
        + WEIGHT_WORKLOAD * workload_score
        + WEIGHT_HIGH_LEV * high_lev_score
    )

    return max(0, min(100, composite))


def calculate_bullpen_modifier(
    pitching_team_score: float, batting_team_score: float
) -> float:
    """Calculate net bullpen modifier from differential.

    Returns a value capped at ±10 points.
    """
    modifier = (pitching_team_score - batting_team_score) / 10.0
    return max(-10, min(10, modifier))
