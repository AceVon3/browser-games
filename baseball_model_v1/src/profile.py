"""
profile.py — Aggregate raw Statcast pitch data into profile objects.

Builds pitcher and batter profiles from pitch-level data. Handles
seasonal blend logic (prior season → blended → current season).
"""

import os
import logging
from datetime import datetime, date
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.fetch import (
    fetch_statcast_pitcher,
    fetch_statcast_batter,
    load_cached_profile,
    save_cached_profile,
)

load_dotenv()

logger = logging.getLogger("pipeline")

CURRENT_SEASON = int(os.getenv("CURRENT_SEASON", datetime.now().year))

# Strike zone grid: zones 1-9 map to a 3x3 grid (heart of zone)
# Zone 11-14 are shadow/edge zones, everything else is out-of-zone
STRIKE_ZONES = list(range(1, 10))


# ---------------------------------------------------------------------------
# Seasonal blend
# ---------------------------------------------------------------------------

def _get_blend_weights(sample_size: int) -> tuple[float, float, str]:
    """Return (prior_weight, current_weight, data_source) based on sample size."""
    if sample_size < 200:
        return 1.0, 0.0, "prior_season"
    elif sample_size < 500:
        return 0.7, 0.3, "blended_early"
    elif sample_size < 1000:
        return 0.4, 0.6, "blended_late"
    else:
        return 0.0, 1.0, "current_season"


def blend_metric(prior_val: float, current_val: float, sample_size: int) -> float:
    """Blend a single metric using the sample-size curve."""
    prior_w, current_w, _ = _get_blend_weights(sample_size)
    if prior_val is None:
        return current_val
    if current_val is None:
        return prior_val
    return prior_w * prior_val + current_w * current_val


def blend_profiles(prior: dict, current: dict, sample_size: int) -> dict:
    """Blend two profile dicts metric-by-metric.

    Numeric fields are blended; non-numeric fields use current if available.
    """
    _, _, data_source = _get_blend_weights(sample_size)
    blended = {}

    all_keys = set(list(prior.keys()) + list(current.keys()))
    for key in all_keys:
        p_val = prior.get(key)
        c_val = current.get(key)

        if key in ("data_source", "last_updated", "sample_size"):
            continue

        if isinstance(c_val, (int, float)) and isinstance(p_val, (int, float)):
            blended[key] = round(blend_metric(p_val, c_val, sample_size), 4)
        elif isinstance(c_val, dict) and isinstance(p_val, dict):
            # Blend nested dicts (pitch_mix, zone_map, splits)
            blended[key] = _blend_dict(p_val, c_val, sample_size)
        elif isinstance(c_val, list) and isinstance(p_val, list):
            # For list fields (e.g. preferred_hit_zones, vulnerable_zones),
            # prefer non-empty list so prior season data isn't lost to a
            # thin current-season sample that couldn't generate zones.
            blended[key] = c_val if c_val else p_val
        else:
            blended[key] = c_val if c_val is not None else p_val

    blended["data_source"] = data_source
    # Store combined sample size (prior + current) so profile reflects total data
    prior_samples = prior.get("sample_size", 0)
    blended["sample_size"] = prior_samples + sample_size
    blended["current_season_pitches"] = sample_size
    blended["last_updated"] = date.today().isoformat()
    return blended


def _blend_dict(prior: dict, current: dict, sample_size: int) -> dict:
    """Blend two nested dicts of numeric values."""
    result = {}
    all_keys = set(list(prior.keys()) + list(current.keys()))
    for key in all_keys:
        p = prior.get(key, 0)
        c = current.get(key, 0)
        if isinstance(p, (int, float)) and isinstance(c, (int, float)):
            result[key] = round(blend_metric(p, c, sample_size), 4)
        else:
            result[key] = c if c is not None else p
    return result


# ---------------------------------------------------------------------------
# Pitcher profile aggregation
# ---------------------------------------------------------------------------

def build_pitcher_profile(
    pitcher_id: int,
    name: str = "",
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
) -> Optional[dict]:
    """Build a pitcher profile from Statcast data.

    Fetches current season data and blends with the raw prior-season
    baseline (stored separately so blending never compounds on itself).
    """
    if start_dt is None:
        start_dt = f"{CURRENT_SEASON}-03-01"
    if end_dt is None:
        end_dt = date.today().isoformat()

    df = fetch_statcast_pitcher(pitcher_id, start_dt, end_dt)
    current_pitches = len(df) if df is not None and not df.empty else 0

    # Always load/build the raw prior-season baseline for clean blending.
    # Stored in pitchers_prior/ so it never gets overwritten by blended data.
    prior_profile = load_cached_profile("pitchers_prior", str(pitcher_id))
    if prior_profile is None:
        prior_year = CURRENT_SEASON - 1
        logger.info("Fetching %d prior season data for pitcher %s (%s)",
                     prior_year, pitcher_id, name)
        prior_df = fetch_statcast_pitcher(
            pitcher_id,
            f"{prior_year}-03-20",
            f"{prior_year}-11-05",
        )
        if prior_df is not None and not prior_df.empty:
            prior_profile = _aggregate_pitcher_data(prior_df, pitcher_id, name)
            prior_profile["data_source"] = "prior_season"
            save_cached_profile("pitchers_prior", str(pitcher_id), prior_profile)
            logger.info("  Prior season: %d pitches for %s",
                         prior_profile["sample_size"], name)

    if (df is None or df.empty) and prior_profile is None:
        return None

    if df is None or df.empty:
        if prior_profile:
            save_cached_profile("pitchers", str(pitcher_id), prior_profile)
            return prior_profile
        return None

    profile = _aggregate_pitcher_data(df, pitcher_id, name)

    # Blend current against the raw prior baseline (never a previous blend)
    if prior_profile:
        sample_size = profile.get("sample_size", 0)
        profile = blend_profiles(prior_profile, profile, sample_size)
    else:
        sample_size = profile.get("sample_size", 0)
        _, _, data_source = _get_blend_weights(sample_size)
        profile["data_source"] = data_source

    save_cached_profile("pitchers", str(pitcher_id), profile)
    return profile


def _aggregate_pitcher_data(df: pd.DataFrame, pitcher_id: int, name: str) -> dict:
    """Aggregate raw pitch-level Statcast data into a pitcher profile."""
    total_pitches = len(df)
    total_pa = df["at_bat_number"].nunique() if "at_bat_number" in df.columns else total_pitches // 4

    # Pitch mix — percentage of each pitch type
    pitch_mix = {}
    if "pitch_type" in df.columns:
        counts = df["pitch_type"].value_counts()
        for ptype, count in counts.items():
            if pd.notna(ptype) and ptype != "":
                pitch_mix[ptype] = round(count / total_pitches, 4)

    primary_pitch = max(pitch_mix, key=pitch_mix.get) if pitch_mix else "FF"

    # Zone map — percentage of pitches in each zone
    zone_map = _build_zone_map(df)
    preferred_zones = sorted(
        [(z, pct) for z, pct in zone_map.items() if z in [str(i) for i in STRIKE_ZONES]],
        key=lambda x: x[1], reverse=True,
    )[:3]

    # Rate stats
    bb_pct = _calc_bb_pct(df)
    k_pct = _calc_k_pct(df)
    whiff_rate = _calc_whiff_rate(df)
    chase_rate = _calc_chase_rate(df)

    # Handedness
    hand = "R"
    if "p_throws" in df.columns:
        hand = df["p_throws"].mode().iloc[0] if not df["p_throws"].mode().empty else "R"

    # Splits
    vs_lhh = _build_splits(df, "L")
    vs_rhh = _build_splits(df, "R")

    return {
        "pitcher_id": str(pitcher_id),
        "name": name,
        "hand": hand,
        "pitch_mix": pitch_mix,
        "primary_pitch": primary_pitch,
        "zone_map": zone_map,
        "preferred_zones": [z for z, _ in preferred_zones],
        "bb_pct": bb_pct,
        "chase_rate": chase_rate,
        "whiff_rate": whiff_rate,
        "k_pct": k_pct,
        "vs_lhh_splits": vs_lhh,
        "vs_rhh_splits": vs_rhh,
        "sample_size": total_pitches,
        "last_updated": date.today().isoformat(),
        "data_source": "current_season",
    }


def _build_zone_map(df: pd.DataFrame) -> dict:
    """Build zone usage map from pitch data."""
    zone_map = {}
    if "zone" not in df.columns:
        return zone_map

    total = len(df)
    counts = df["zone"].value_counts()
    for zone, count in counts.items():
        if pd.notna(zone):
            zone_map[str(int(zone))] = round(count / total, 4)

    # Add out-of-zone aggregate
    out_of_zone = sum(
        pct for z, pct in zone_map.items()
        if z not in [str(i) for i in STRIKE_ZONES]
    )
    zone_map["out_of_zone"] = round(out_of_zone, 4)
    return zone_map


def _calc_bb_pct(df: pd.DataFrame) -> float:
    """Calculate walk rate from pitch-level data."""
    if "events" not in df.columns:
        return 0.0
    pa_events = df[df["events"].notna()]
    if len(pa_events) == 0:
        return 0.0
    walks = len(pa_events[pa_events["events"] == "walk"])
    return round(walks / len(pa_events), 4)


def _calc_k_pct(df: pd.DataFrame) -> float:
    """Calculate strikeout rate from pitch-level data."""
    if "events" not in df.columns:
        return 0.0
    pa_events = df[df["events"].notna()]
    if len(pa_events) == 0:
        return 0.0
    strikeouts = len(pa_events[pa_events["events"].isin(["strikeout", "strikeout_double_play"])])
    return round(strikeouts / len(pa_events), 4)


def _calc_whiff_rate(df: pd.DataFrame) -> float:
    """Calculate swinging strike rate (SwStr%)."""
    if "description" not in df.columns:
        return 0.0
    swings = df[df["description"].isin([
        "swinging_strike", "swinging_strike_blocked",
        "foul", "foul_tip", "hit_into_play",
        "hit_into_play_no_out", "hit_into_play_score",
    ])]
    if len(swings) == 0:
        return 0.0
    whiffs = swings[swings["description"].isin(["swinging_strike", "swinging_strike_blocked"])]
    return round(len(whiffs) / len(swings), 4)


def _calc_chase_rate(df: pd.DataFrame) -> float:
    """Calculate O-Swing% (swings at pitches outside the zone)."""
    if "zone" not in df.columns or "description" not in df.columns:
        return 0.0
    outside = df[~df["zone"].isin(STRIKE_ZONES)]
    if len(outside) == 0:
        return 0.0
    swing_descriptions = [
        "swinging_strike", "swinging_strike_blocked",
        "foul", "foul_tip", "hit_into_play",
        "hit_into_play_no_out", "hit_into_play_score",
    ]
    chases = outside[outside["description"].isin(swing_descriptions)]
    return round(len(chases) / len(outside), 4)


def _build_splits(df: pd.DataFrame, batter_hand: str) -> dict:
    """Build pitch mix and zone map against a specific batter handedness."""
    if "stand" not in df.columns:
        return {"pitch_mix": {}, "zone_map": {}}

    split_df = df[df["stand"] == batter_hand]
    if split_df.empty:
        return {"pitch_mix": {}, "zone_map": {}}

    total = len(split_df)
    pitch_mix = {}
    if "pitch_type" in split_df.columns:
        counts = split_df["pitch_type"].value_counts()
        for ptype, count in counts.items():
            if pd.notna(ptype) and ptype != "":
                pitch_mix[ptype] = round(count / total, 4)

    zone_map = _build_zone_map(split_df)

    return {"pitch_mix": pitch_mix, "zone_map": zone_map}


# ---------------------------------------------------------------------------
# Batter profile aggregation
# ---------------------------------------------------------------------------

def build_batter_profile(
    batter_id: int,
    name: str = "",
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
) -> Optional[dict]:
    """Build a batter profile from Statcast data.

    Fetches current season data and blends with the raw prior-season
    baseline (stored separately so blending never compounds on itself).
    """
    if start_dt is None:
        start_dt = f"{CURRENT_SEASON}-03-01"
    if end_dt is None:
        end_dt = date.today().isoformat()

    df = fetch_statcast_batter(batter_id, start_dt, end_dt)
    current_pitches = len(df) if df is not None and not df.empty else 0

    # Always load/build the raw prior-season baseline for clean blending.
    # Stored in batters_prior/ so it never gets overwritten by blended data.
    prior_profile = load_cached_profile("batters_prior", str(batter_id))
    if prior_profile is None:
        prior_year = CURRENT_SEASON - 1
        logger.info("Fetching %d prior season data for batter %s",
                     prior_year, batter_id)
        prior_df = fetch_statcast_batter(
            batter_id,
            f"{prior_year}-03-20",
            f"{prior_year}-11-05",
        )
        if prior_df is not None and not prior_df.empty:
            prior_profile = _aggregate_batter_data(prior_df, batter_id, name)
            prior_profile["data_source"] = "prior_season"
            save_cached_profile("batters_prior", str(batter_id), prior_profile)
            logger.info("  Prior season: %d PA for batter %s",
                         prior_profile["sample_size"], batter_id)

    if (df is None or df.empty) and prior_profile is None:
        return None

    if df is None or df.empty:
        if prior_profile:
            save_cached_profile("batters", str(batter_id), prior_profile)
            return prior_profile
        return None

    profile = _aggregate_batter_data(df, batter_id, name)

    # Blend current against the raw prior baseline (never a previous blend)
    if prior_profile:
        sample_size = profile.get("sample_size", 0)
        profile = blend_profiles(prior_profile, profile, sample_size)
    else:
        sample_size = profile.get("sample_size", 0)
        _, _, data_source = _get_blend_weights(sample_size)
        profile["data_source"] = data_source

    save_cached_profile("batters", str(batter_id), profile)
    return profile


def _aggregate_batter_data(df: pd.DataFrame, batter_id: int, name: str) -> dict:
    """Aggregate raw pitch-level Statcast data into a batter profile."""
    pa_events = df[df["events"].notna()] if "events" in df.columns else df
    total_pa = len(pa_events)
    total_pitches = len(df)

    # Batting hand
    hand = "R"
    if "stand" in df.columns:
        hand = df["stand"].mode().iloc[0] if not df["stand"].mode().empty else "R"

    # Zone performance (wOBA by zone)
    zone_woba = _calc_zone_woba(df)
    sorted_zones = sorted(zone_woba.items(), key=lambda x: x[1], reverse=True)
    hot_spots = {z: v for z, v in sorted_zones[:3]} if sorted_zones else {}
    cold_spots = {z: v for z, v in sorted_zones[-3:]} if len(sorted_zones) >= 3 else {}

    # Pitch type performance
    pitch_type_perf = _calc_pitch_type_woba(df)
    best_pitch = max(pitch_type_perf, key=pitch_type_perf.get) if pitch_type_perf else ""
    worst_pitch = min(pitch_type_perf, key=pitch_type_perf.get) if pitch_type_perf else ""

    # Rate stats
    bb_pct = _calc_bb_pct(df)
    k_pct = _calc_k_pct(df)
    chase_rate = _calc_chase_rate(df)
    contact_rate = _calc_contact_rate(df)

    return {
        "batter_id": str(batter_id),
        "name": name,
        "hand": hand,
        "zone_hot_spots": hot_spots,
        "zone_cold_spots": cold_spots,
        "preferred_hit_zones": [z for z, _ in sorted_zones[:3]],
        "vulnerable_zones": [z for z, _ in sorted_zones[-3:]] if len(sorted_zones) >= 3 else [],
        "pitch_type_perf": pitch_type_perf,
        "best_pitch_type": best_pitch,
        "worst_pitch_type": worst_pitch,
        "bb_pct": bb_pct,
        "k_pct": k_pct,
        "chase_rate": chase_rate,
        "contact_rate": contact_rate,
        "sample_size": total_pa,
        "last_updated": date.today().isoformat(),
        "data_source": "current_season",
    }


# wOBA weights (approximate linear weights)
WOBA_WEIGHTS = {
    "walk": 0.69,
    "hit_by_pitch": 0.72,
    "single": 0.88,
    "double": 1.27,
    "triple": 1.62,
    "home_run": 2.10,
    "field_out": 0.0,
    "strikeout": 0.0,
    "strikeout_double_play": 0.0,
    "grounded_into_double_play": 0.0,
    "double_play": 0.0,
    "force_out": 0.0,
    "fielders_choice": 0.0,
    "fielders_choice_out": 0.0,
    "field_error": 0.0,
    "sac_fly": 0.0,
    "sac_bunt": 0.0,
}


def _calc_woba(events: pd.Series) -> float:
    """Calculate wOBA from a series of event types."""
    if events.empty:
        return 0.0
    total_weight = 0.0
    count = 0
    for event in events:
        if pd.notna(event) and event in WOBA_WEIGHTS:
            total_weight += WOBA_WEIGHTS[event]
            count += 1
    return round(total_weight / count, 4) if count > 0 else 0.0


def _calc_zone_woba(df: pd.DataFrame) -> dict:
    """Calculate wOBA per zone for a batter."""
    if "zone" not in df.columns or "events" not in df.columns:
        return {}

    pa = df[df["events"].notna()]
    result = {}
    for zone in STRIKE_ZONES:
        zone_pa = pa[pa["zone"] == zone]
        if len(zone_pa) >= 5:  # minimum sample
            result[str(zone)] = _calc_woba(zone_pa["events"])
    return result


def _calc_pitch_type_woba(df: pd.DataFrame) -> dict:
    """Calculate wOBA vs each pitch type."""
    if "pitch_type" not in df.columns or "events" not in df.columns:
        return {}

    pa = df[df["events"].notna()]
    result = {}
    for ptype in pa["pitch_type"].unique():
        if pd.isna(ptype) or ptype == "":
            continue
        type_pa = pa[pa["pitch_type"] == ptype]
        if len(type_pa) >= 5:
            result[ptype] = _calc_woba(type_pa["events"])
    return result


def _calc_contact_rate(df: pd.DataFrame) -> float:
    """Calculate contact rate on swings."""
    if "description" not in df.columns:
        return 0.0
    swing_descriptions = [
        "swinging_strike", "swinging_strike_blocked",
        "foul", "foul_tip", "hit_into_play",
        "hit_into_play_no_out", "hit_into_play_score",
    ]
    swings = df[df["description"].isin(swing_descriptions)]
    if len(swings) == 0:
        return 0.0
    contact = swings[~swings["description"].isin(["swinging_strike", "swinging_strike_blocked"])]
    return round(len(contact) / len(swings), 4)


# ---------------------------------------------------------------------------
# League average profiles (for rookies / missing data)
# ---------------------------------------------------------------------------

LEAGUE_AVG_PITCHER = {
    "pitcher_id": "league_avg",
    "name": "League Average",
    "hand": "R",
    "pitch_mix": {"FF": 0.35, "SL": 0.20, "CH": 0.15, "CU": 0.10, "SI": 0.12, "FC": 0.08},
    "primary_pitch": "FF",
    "zone_map": {str(i): 0.08 for i in range(1, 10)},
    "preferred_zones": ["5", "2", "8"],
    "bb_pct": 0.082,
    "chase_rate": 0.30,
    "whiff_rate": 0.24,
    "k_pct": 0.22,
    "vs_lhh_splits": {"pitch_mix": {}, "zone_map": {}},
    "vs_rhh_splits": {"pitch_mix": {}, "zone_map": {}},
    "sample_size": 0,
    "last_updated": date.today().isoformat(),
    "data_source": "league_avg",
}

LEAGUE_AVG_BATTER = {
    "batter_id": "league_avg",
    "name": "League Average",
    "hand": "R",
    "zone_hot_spots": {"5": 0.380, "4": 0.360, "6": 0.350},
    "zone_cold_spots": {"1": 0.280, "3": 0.270, "7": 0.260},
    "preferred_hit_zones": ["5", "4", "6"],
    "vulnerable_zones": ["1", "3", "7"],
    "pitch_type_perf": {"FF": 0.340, "SL": 0.290, "CH": 0.310, "CU": 0.280},
    "best_pitch_type": "FF",
    "worst_pitch_type": "CU",
    "bb_pct": 0.082,
    "k_pct": 0.22,
    "chase_rate": 0.30,
    "contact_rate": 0.76,
    "sample_size": 0,
    "last_updated": date.today().isoformat(),
    "data_source": "league_avg",
}


def get_league_avg_pitcher() -> dict:
    """Return a copy of the league average pitcher profile."""
    return {**LEAGUE_AVG_PITCHER, "last_updated": date.today().isoformat()}


def get_league_avg_batter() -> dict:
    """Return a copy of the league average batter profile."""
    return {**LEAGUE_AVG_BATTER, "last_updated": date.today().isoformat()}
