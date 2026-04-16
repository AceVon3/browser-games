"""
backtest_ou_real.py — Backtest O/U formula against actual DraftKings lines.

Uses backtest_2025_with_ou.csv which has real O/U lines from The Odds API.

Usage:
    python -m src.backtest_ou_real
"""

import csv
import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "backtest")
BACKTEST_CSV = os.path.join(DATA_DIR, "backtest_2025_with_ou.csv")

# 2025 season team offensive stats (OPS, runs/game) for backtest
TEAM_OFFENSE_2025 = {
    "ATH": {"ops": 0.749, "rpg": 4.52}, "PIT": {"ops": 0.655, "rpg": 3.60},
    "SD":  {"ops": 0.711, "rpg": 4.33}, "SEA": {"ops": 0.740, "rpg": 4.73},
    "SF":  {"ops": 0.697, "rpg": 4.35}, "STL": {"ops": 0.693, "rpg": 4.25},
    "TB":  {"ops": 0.714, "rpg": 4.41}, "TEX": {"ops": 0.683, "rpg": 4.22},
    "TOR": {"ops": 0.760, "rpg": 4.93}, "MIN": {"ops": 0.707, "rpg": 4.19},
    "PHI": {"ops": 0.759, "rpg": 4.80}, "ATL": {"ops": 0.719, "rpg": 4.47},
    "CWS": {"ops": 0.675, "rpg": 3.99}, "MIA": {"ops": 0.707, "rpg": 4.38},
    "NYY": {"ops": 0.787, "rpg": 5.24}, "MIL": {"ops": 0.735, "rpg": 4.98},
    "LAA": {"ops": 0.695, "rpg": 4.15}, "AZ":  {"ops": 0.758, "rpg": 4.88},
    "BAL": {"ops": 0.699, "rpg": 4.18}, "BOS": {"ops": 0.745, "rpg": 4.85},
    "CHC": {"ops": 0.750, "rpg": 4.90}, "CIN": {"ops": 0.706, "rpg": 4.42},
    "CLE": {"ops": 0.669, "rpg": 3.97}, "COL": {"ops": 0.679, "rpg": 3.69},
    "DET": {"ops": 0.729, "rpg": 4.68}, "HOU": {"ops": 0.714, "rpg": 4.23},
    "KC":  {"ops": 0.706, "rpg": 4.02}, "LAD": {"ops": 0.768, "rpg": 5.09},
    "WSH": {"ops": 0.693, "rpg": 4.24}, "NYM": {"ops": 0.753, "rpg": 4.73},
}

# Team name → abbreviation mapping for backtest CSV
TEAM_ABBREV = {
    "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "ATH", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
}


def _offense_score(ops, rpg):
    """Calculate 0-100 offensive strength score."""
    ops_score = max(0, min(100, (ops - 0.600) / 0.220 * 100))
    rpg_score = max(0, min(100, (rpg - 2.5) / 4.0 * 100))
    return 0.5 * ops_score + 0.5 * rpg_score

HOME_FIELD_ADJ = 3
ZONE_W, PITCH_W, WALK_W, HAND_W = 49, 22, 19, 10
ORIG_ZONE, ORIG_PITCH, ORIG_WALK, ORIG_HAND = 40, 30, 15, 15


def apply_park_factor(raw, pf):
    adj = raw * (1 + (1.0 - pf) * 0.5)
    adjustment = max(-10, min(10, adj - raw))
    return raw + adjustment


def reweight_edges(r):
    zm = ZONE_W / ORIG_ZONE
    pm = PITCH_W / ORIG_PITCH
    wm = WALK_W / ORIG_WALK
    hm = HAND_W / ORIG_HAND
    hr = max(0, min(100,
        r["home_zone"] * zm + r["home_pitch"] * pm +
        r["home_walk"] * wm + r["home_hand"] * hm))
    ar = max(0, min(100,
        r["away_zone"] * zm + r["away_pitch"] * pm +
        r["away_walk"] * wm + r["away_hand"] * hm))
    hp = apply_park_factor(hr, r["park_factor"])
    ap = apply_park_factor(ar, r["park_factor"])
    he = hp + r["park_weather_adj"] + r["home_bp_mod"] + HOME_FIELD_ADJ
    ae = ap + r["park_weather_adj"] + r["away_bp_mod"]
    return he, ae


def calculate_ou(home_edge, away_edge, park_factor, home_bp, away_bp,
                 home_offense_score=50.0, away_offense_score=50.0):
    """O/U formula with dampened OVER side + offensive strength."""
    avg_edge = (home_edge + away_edge) / 2
    avg_bullpen = (home_bp + away_bp) / 2

    # Model total
    mt = 8.7
    mt -= ((avg_edge - 50) / 50) * 2.5
    mt += (park_factor - 1.0) * 3.0
    mt -= ((avg_bullpen - 50) / 40) * 0.5

    # O/U score
    ou = 50.0

    # Asymmetric edge factor
    raw_edge = -((avg_edge - 50) / 50)
    if raw_edge > 0:
        ou += raw_edge * 14  # OVER side dampened
    else:
        ou += raw_edge * 20  # UNDER side full weight

    park_ou = max(-10, min(10, (park_factor - 1.0) * 100))
    ou += park_ou

    bp_ou = max(-10, min(10, -((avg_bullpen - 50) / 20) * 10))
    ou += bp_ou

    spread = abs(home_edge - away_edge)
    if avg_edge >= 55:
        spread_factor = -(spread / 50) * 3
    elif avg_edge <= 45:
        spread_factor = (spread / 50) * 3
    else:
        spread_factor = 0
    ou += spread_factor

    # Offensive strength factor: strong lineups push OVER, weak push UNDER
    avg_offense = (home_offense_score + away_offense_score) / 2
    offense_ou = ((avg_offense - 50) / 50) * 8
    offense_ou = max(-8, min(8, offense_ou))
    ou += offense_ou
    mt += ((avg_offense - 50) / 50) * 0.5

    # Convergence — dampened OVER boosts
    low_e = min(home_edge, away_edge)
    high_e = max(home_edge, away_edge)
    conv = 0
    if low_e >= 60:
        conv = -15
    elif low_e >= 55 and high_e >= 55:
        conv = -8
    elif high_e <= 40:
        conv = 3   # dampened
    elif high_e <= 45 and low_e <= 40:
        conv = 2   # dampened
    ou += conv
    mt += conv / 15 * 0.5

    mt = round(max(4.0, min(15.0, mt)), 1)
    ou = max(0, min(100, round(ou, 1)))

    return mt, ou, conv


def decimal_to_american(dec):
    """Convert decimal odds to American odds."""
    if dec >= 2.0:
        return int(round((dec - 1) * 100))
    else:
        return int(round(-100 / (dec - 1)))


def profit_from_odds(won, american_odds):
    """Calculate profit/loss for a $100 risk bet at given American odds."""
    if american_odds > 0:
        return american_odds if won else -100
    else:
        return round(100 / abs(american_odds) * 100) if won else -100


def run():
    # Load data
    rows = []
    with open(BACKTEST_CSV, newline="") as f:
        for r in csv.DictReader(f):
            # Skip games without actual O/U lines
            if not r.get("actual_ou_line"):
                continue
            for k in ("home_zone", "home_pitch", "home_walk", "home_hand",
                       "away_zone", "away_pitch", "away_walk", "away_hand",
                       "park_factor", "park_weather_adj", "home_bp_mod", "away_bp_mod",
                       "home_bullpen_score", "away_bullpen_score",
                       "home_score", "away_score"):
                r[k] = float(r.get(k) or 0)
            r["actual_total"] = r["home_score"] + r["away_score"]
            r["ou_line"] = float(r["actual_ou_line"])
            # Convert decimal odds to American
            over_dec = float(r.get("actual_ou_over_odds") or 0)
            under_dec = float(r.get("actual_ou_under_odds") or 0)
            r["over_odds_am"] = decimal_to_american(over_dec) if over_dec else -110
            r["under_odds_am"] = decimal_to_american(under_dec) if under_dec else -110
            rows.append(r)

    print(f"Loaded {len(rows)} games with actual O/U lines\n")

    # Compute O/U for each game
    games = []
    for r in rows:
        he, ae = reweight_edges(r)
        # Look up team offense scores
        home_abbrev = TEAM_ABBREV.get(r["home_team"], "")
        away_abbrev = TEAM_ABBREV.get(r["away_team"], "")
        home_off = TEAM_OFFENSE_2025.get(home_abbrev, {"ops": 0.710, "rpg": 4.45})
        away_off = TEAM_OFFENSE_2025.get(away_abbrev, {"ops": 0.710, "rpg": 4.45})
        home_off_score = _offense_score(home_off["ops"], home_off["rpg"])
        away_off_score = _offense_score(away_off["ops"], away_off["rpg"])
        mt, ou, conv = calculate_ou(he, ae, r["park_factor"],
                                     r["home_bullpen_score"], r["away_bullpen_score"],
                                     home_off_score, away_off_score)
        games.append({
            "date": r["date"],
            "home": r["home_team"],
            "away": r["away_team"],
            "home_edge": he,
            "away_edge": ae,
            "model_total": mt,
            "ou_score": ou,
            "convergence": conv,
            "actual_total": r["actual_total"],
            "ou_line": r["ou_line"],
            "over_odds_am": r["over_odds_am"],
            "under_odds_am": r["under_odds_am"],
        })

    div = "=" * 70

    # --- Overall accuracy ---
    print(div)
    print("MODEL TOTAL vs ACTUAL TOTAL vs VEGAS LINE")
    print(div)
    avg_model = sum(g["model_total"] for g in games) / len(games)
    avg_actual = sum(g["actual_total"] for g in games) / len(games)
    avg_line = sum(g["ou_line"] for g in games) / len(games)
    print(f"  Avg model total:  {avg_model:.2f}")
    print(f"  Avg Vegas line:   {avg_line:.2f}")
    print(f"  Avg actual total: {avg_actual:.2f}")
    print(f"  Model bias:       {avg_model - avg_actual:+.2f} runs")
    print(f"  Vegas bias:       {avg_line - avg_actual:+.2f} runs")

    # --- Directional accuracy vs actual lines ---
    print(f"\n{div}")
    print("DIRECTIONAL ACCURACY vs ACTUAL VEGAS LINES")
    print(div)

    over_right = over_wrong = under_right = under_wrong = push = 0
    for g in games:
        if g["model_total"] > g["ou_line"]:
            if g["actual_total"] > g["ou_line"]:
                over_right += 1
            elif g["actual_total"] < g["ou_line"]:
                over_wrong += 1
            else:
                push += 1
        elif g["model_total"] < g["ou_line"]:
            if g["actual_total"] < g["ou_line"]:
                under_right += 1
            elif g["actual_total"] > g["ou_line"]:
                under_wrong += 1
            else:
                push += 1

    ov_t = over_right + over_wrong
    un_t = under_right + under_wrong
    print(f"  OVER:  {over_right}/{ov_t} ({over_right/ov_t*100:.1f}%)" if ov_t else "  OVER: 0 games")
    print(f"  UNDER: {under_right}/{un_t} ({under_right/un_t*100:.1f}%)" if un_t else "  UNDER: 0 games")
    print(f"  Pushes: {push}")
    all_t = ov_t + un_t
    all_r = over_right + under_right
    print(f"  Combined: {all_r}/{all_t} ({all_r/all_t*100:.1f}%)")

    # --- O/U SIGNAL THRESHOLD SWEEP with actual lines and actual odds ---
    print(f"\n{div}")
    print("O/U SIGNAL THRESHOLD SWEEP (actual DraftKings lines & odds)")
    print(div)
    print(f"  {'Thresh':>6} | {'OVER#':>5} {'OvWR':>5} {'Ov$':>7} {'OvROI':>6} | "
          f"{'UND#':>5} {'UnWR':>5} {'Un$':>7} {'UnROI':>6} | "
          f"{'Tot#':>5} {'TotWR':>5} {'Tot$':>7} {'TotROI':>6}")
    print("  " + "-" * 90)

    for thresh_dist in range(5, 26):
        over_bets = []
        under_bets = []

        for g in games:
            ou_strength = abs(g["ou_score"] - 50)
            if ou_strength < thresh_dist:
                continue

            if g["ou_score"] >= 50 and g["model_total"] > g["ou_line"]:
                # OVER signal — model agrees with score direction
                won = g["actual_total"] > g["ou_line"]
                is_push = g["actual_total"] == g["ou_line"]
                if not is_push:
                    profit = profit_from_odds(won, g["over_odds_am"])
                    over_bets.append({"won": won, "profit": profit, "risked": 100})

            elif g["ou_score"] < 50 and g["model_total"] < g["ou_line"]:
                # UNDER signal — model agrees with score direction
                won = g["actual_total"] < g["ou_line"]
                is_push = g["actual_total"] == g["ou_line"]
                if not is_push:
                    profit = profit_from_odds(won, g["under_odds_am"])
                    under_bets.append({"won": won, "profit": profit, "risked": 100})

        ov_n = len(over_bets)
        un_n = len(under_bets)
        ov_w = sum(1 for b in over_bets if b["won"])
        un_w = sum(1 for b in under_bets if b["won"])
        ov_p = sum(b["profit"] for b in over_bets)
        un_p = sum(b["profit"] for b in under_bets)
        tot_n = ov_n + un_n
        tot_w = ov_w + un_w
        tot_p = ov_p + un_p

        if tot_n >= 5:
            ov_wr = ov_w / ov_n * 100 if ov_n else 0
            un_wr = un_w / un_n * 100 if un_n else 0
            tot_wr = tot_w / tot_n * 100
            ov_roi = ov_p / (ov_n * 100) * 100 if ov_n else 0
            un_roi = un_p / (un_n * 100) * 100 if un_n else 0
            tot_roi = tot_p / (tot_n * 100) * 100

            print(f"  {thresh_dist:>6} | {ov_n:>5} {ov_wr:>4.1f}% {ov_p:>+7.0f} {ov_roi:>+5.1f}% | "
                  f"{un_n:>5} {un_wr:>4.1f}% {un_p:>+7.0f} {un_roi:>+5.1f}% | "
                  f"{tot_n:>5} {tot_wr:>4.1f}% {tot_p:>+7.0f} {tot_roi:>+5.1f}%")

    # --- Convergence analysis with real lines ---
    print(f"\n{div}")
    print("CONVERGENCE BOOST ANALYSIS (vs actual lines)")
    print(div)

    for conv_val, label in [(-15, "Both dominant (conv -15)"),
                             (-8, "Both solid (conv -8)"),
                             (0, "No convergence"),
                             (2, "Both below avg (conv +2)"),
                             (3, "Both weak (conv +3)")]:
        subset = [g for g in games if g["convergence"] == conv_val]
        if not subset:
            continue
        avg_at = sum(g["actual_total"] for g in subset) / len(subset)
        avg_mt = sum(g["model_total"] for g in subset) / len(subset)
        avg_ln = sum(g["ou_line"] for g in subset) / len(subset)

        over_hit = sum(1 for g in subset if g["actual_total"] > g["ou_line"])
        under_hit = sum(1 for g in subset if g["actual_total"] < g["ou_line"])
        push = sum(1 for g in subset if g["actual_total"] == g["ou_line"])

        print(f"  {label}: {len(subset)} games")
        print(f"    Avg model: {avg_mt:.1f}, Avg line: {avg_ln:.1f}, Avg actual: {avg_at:.1f}")
        print(f"    Over: {over_hit}/{len(subset)} ({over_hit/len(subset)*100:.1f}%), "
              f"Under: {under_hit}/{len(subset)} ({under_hit/len(subset)*100:.1f}%), "
              f"Push: {push}")

    # --- Monthly breakdown ---
    print(f"\n{div}")
    print("MONTHLY BREAKDOWN (O/U strength >= 15, actual lines)")
    print(div)

    monthly = defaultdict(lambda: {"over_w": 0, "over_n": 0, "under_w": 0, "under_n": 0,
                                    "over_profit": 0, "under_profit": 0})

    for g in games:
        ou_strength = abs(g["ou_score"] - 50)
        if ou_strength < 15:
            continue
        month = g["date"][:7]
        if g["ou_score"] >= 50 and g["model_total"] > g["ou_line"]:
            won = g["actual_total"] > g["ou_line"]
            is_push = g["actual_total"] == g["ou_line"]
            if not is_push:
                monthly[month]["over_n"] += 1
                if won:
                    monthly[month]["over_w"] += 1
                monthly[month]["over_profit"] += profit_from_odds(won, g["over_odds_am"])
        elif g["ou_score"] < 50 and g["model_total"] < g["ou_line"]:
            won = g["actual_total"] < g["ou_line"]
            is_push = g["actual_total"] == g["ou_line"]
            if not is_push:
                monthly[month]["under_n"] += 1
                if won:
                    monthly[month]["under_w"] += 1
                monthly[month]["under_profit"] += profit_from_odds(won, g["under_odds_am"])

    print(f"  {'Month':>7} | {'Ov W/N':>7} {'OvWR':>5} {'Ov$':>7} | "
          f"{'Un W/N':>7} {'UnWR':>5} {'Un$':>7} | {'Tot$':>7}")
    for m in sorted(monthly.keys()):
        d = monthly[m]
        ov = f"{d['over_w']}/{d['over_n']}" if d['over_n'] else "  -"
        un = f"{d['under_w']}/{d['under_n']}" if d['under_n'] else "  -"
        ov_wr = d["over_w"] / d["over_n"] * 100 if d["over_n"] else 0
        un_wr = d["under_w"] / d["under_n"] * 100 if d["under_n"] else 0
        tot_p = d["over_profit"] + d["under_profit"]
        print(f"  {m:>7} | {ov:>7} {ov_wr:>4.1f}% {d['over_profit']:>+7.0f} | "
              f"{un:>7} {un_wr:>4.1f}% {d['under_profit']:>+7.0f} | {tot_p:>+7.0f}")


if __name__ == "__main__":
    run()
