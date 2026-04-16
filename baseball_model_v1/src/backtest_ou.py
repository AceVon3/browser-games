"""
backtest_ou.py — Backtest the O/U formula against 2025 results.

Since we don't have historical O/U lines from OddsPortal, we use the
actual game total as the "line" and test directional accuracy. We also
simulate using the Vegas consensus line (avg total ~8.9) as a proxy.

Usage:
    py -3 -m src.backtest_ou
"""

import csv
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
BACKTEST_CSV = os.path.join(DATA_DIR, "backtest", "backtest_2025.csv")

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


def calculate_ou_new(home_edge, away_edge, park_factor, home_bp, away_bp):
    """New O/U formula matching score.py."""
    avg_edge = (home_edge + away_edge) / 2
    avg_bullpen = (home_bp + away_bp) / 2

    # Model total
    mt = 8.7
    mt -= ((avg_edge - 50) / 50) * 2.5
    mt += (park_factor - 1.0) * 3.0
    mt -= ((avg_bullpen - 50) / 40) * 0.5

    # O/U score
    ou = 50.0
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

    # Convergence
    low_e = min(home_edge, away_edge)
    high_e = max(home_edge, away_edge)
    conv = 0
    if low_e >= 60:
        conv = -15
    elif low_e >= 55 and high_e >= 55:
        conv = -8
    elif high_e <= 40:
        conv = 3   # dampened — weak pitchers get pulled early
    elif high_e <= 45 and low_e <= 40:
        conv = 2   # slight OVER
    ou += conv
    mt += conv / 15 * 0.5

    mt = round(max(4.0, min(15.0, mt)), 1)
    ou = max(0, min(100, round(ou, 1)))

    return mt, ou, conv


def ml_profit_110(won):
    """Standard -110 juice: risk $110 to win $100."""
    return 100 if won else -110


def run():
    # Load data
    rows = []
    with open(BACKTEST_CSV, newline="") as f:
        for r in csv.DictReader(f):
            for k in ("home_zone", "home_pitch", "home_walk", "home_hand",
                       "away_zone", "away_pitch", "away_walk", "away_hand",
                       "park_factor", "park_weather_adj", "home_bp_mod", "away_bp_mod",
                       "home_bullpen_score", "away_bullpen_score",
                       "home_score", "away_score"):
                r[k] = float(r.get(k) or 0)
            r["actual_total"] = r["home_score"] + r["away_score"]
            rows.append(r)

    print(f"Loaded {len(rows)} games\n")

    # Compute O/U for each game
    games = []
    for r in rows:
        he, ae = reweight_edges(r)
        mt, ou, conv = calculate_ou_new(he, ae, r["park_factor"],
                                         r["home_bullpen_score"], r["away_bullpen_score"])
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
            "home_bp": r["home_bullpen_score"],
            "away_bp": r["away_bullpen_score"],
        })

    div = "=" * 70

    # --- Overall accuracy: model total vs actual ---
    print(div)
    print("MODEL TOTAL vs ACTUAL TOTAL")
    print(div)
    avg_model = sum(g["model_total"] for g in games) / len(games)
    avg_actual = sum(g["actual_total"] for g in games) / len(games)
    print(f"  Avg model: {avg_model:.2f}")
    print(f"  Avg actual: {avg_actual:.2f}")
    print(f"  Bias: {avg_model - avg_actual:+.2f} runs")

    # --- O/U score distribution ---
    print(f"\n{div}")
    print("O/U SCORE DISTRIBUTION")
    print(div)
    for lo, hi, label in [(0, 30, "Strong UNDER"), (30, 40, "UNDER"),
                           (40, 50, "Mild UNDER"), (50, 60, "Mild OVER"),
                           (60, 70, "OVER"), (70, 100, "Strong OVER")]:
        subset = [g for g in games if lo <= g["ou_score"] < hi]
        if not subset:
            print(f"  {label} ({lo}-{hi}): 0 games")
            continue
        avg_at = sum(g["actual_total"] for g in subset) / len(subset)
        avg_mt = sum(g["model_total"] for g in subset) / len(subset)
        print(f"  {label} ({lo}-{hi}): {len(subset)} games, "
              f"avg model {avg_mt:.1f}, avg actual {avg_at:.1f}")

    # --- Directional test using league avg line (8.5) as proxy ---
    # This simulates: "if the line were always 8.5, how often is model right?"
    print(f"\n{div}")
    print("DIRECTIONAL ACCURACY vs FIXED LINES")
    print(div)

    for proxy_line in [7.5, 8.0, 8.5, 9.0, 9.5]:
        over_right = over_wrong = under_right = under_wrong = 0
        for g in games:
            if g["model_total"] > proxy_line:
                if g["actual_total"] > proxy_line:
                    over_right += 1
                else:
                    over_wrong += 1
            elif g["model_total"] < proxy_line:
                if g["actual_total"] < proxy_line:
                    under_right += 1
                else:
                    under_wrong += 1
            # skip pushes

        over_total = over_right + over_wrong
        under_total = under_right + under_wrong
        over_wr = over_right / over_total * 100 if over_total else 0
        under_wr = under_right / under_total * 100 if under_total else 0
        all_total = over_total + under_total
        all_right = over_right + under_right
        all_wr = all_right / all_total * 100 if all_total else 0

        print(f"  Line {proxy_line}: OVER {over_right}/{over_total} ({over_wr:.1f}%) | "
              f"UNDER {under_right}/{under_total} ({under_wr:.1f}%) | "
              f"Combined {all_right}/{all_total} ({all_wr:.1f}%)")

    # --- Signal simulation: O/U score threshold sweep ---
    print(f"\n{div}")
    print("O/U SIGNAL THRESHOLD SWEEP (using model_total vs actual_total)")
    print(div)
    print(f"  {'Thresh':>6} | {'OVER#':>5} {'OvWR':>5} {'Ov$':>7} | "
          f"{'UND#':>5} {'UnWR':>5} {'Un$':>7} | "
          f"{'Tot#':>5} {'TotWR':>5} {'Tot$':>7} {'ROI':>6}")
    print("  " + "-" * 75)

    # Use per-game median line as proxy (actual avg ~8.9)
    # But since we don't have real lines, use model_total vs actual_total
    # If model says OVER (ou_score > 50 + threshold) and actual > model... that's circular.
    # Better approach: for each game with a strong signal, check if actual went
    # in the predicted direction relative to a reasonable line.

    # Use 8.5 as the standard proxy line (slightly under league avg)
    proxy = 8.5

    for thresh_dist in range(5, 26):
        over_bets = []
        under_bets = []

        for g in games:
            ou_strength = abs(g["ou_score"] - 50)
            if ou_strength < thresh_dist:
                continue

            if g["ou_score"] >= 50:
                # OVER signal
                won = g["actual_total"] > proxy
                over_bets.append({"won": won, "profit": ml_profit_110(won)})
            else:
                # UNDER signal
                won = g["actual_total"] < proxy
                under_bets.append({"won": won, "profit": ml_profit_110(won)})

        ov_n = len(over_bets)
        un_n = len(under_bets)
        ov_w = sum(1 for b in over_bets if b["won"])
        un_w = sum(1 for b in under_bets if b["won"])
        ov_p = sum(b["profit"] for b in over_bets)
        un_p = sum(b["profit"] for b in under_bets)
        tot_n = ov_n + un_n
        tot_w = ov_w + un_w
        tot_p = ov_p + un_p
        tot_roi = tot_p / (tot_n * 110) * 100 if tot_n else 0

        if tot_n >= 10:
            ov_wr = ov_w / ov_n * 100 if ov_n else 0
            un_wr = un_w / un_n * 100 if un_n else 0
            tot_wr = tot_w / tot_n * 100

            print(f"  {thresh_dist:>6} | {ov_n:>5} {ov_wr:>4.1f}% {ov_p:>+6.0f} | "
                  f"{un_n:>5} {un_wr:>4.1f}% {un_p:>+6.0f} | "
                  f"{tot_n:>5} {tot_wr:>4.1f}% {tot_p:>+6.0f} {tot_roi:>+5.1f}%")

    # --- Convergence boost analysis ---
    print(f"\n{div}")
    print("CONVERGENCE BOOST ANALYSIS")
    print(div)

    for conv_val, label in [(-15, "Both dominant (conv -15)"),
                             (-8, "Both solid (conv -8)"),
                             (0, "No convergence"),
                             (5, "Both below avg (conv +5)"),
                             (10, "Both weak (conv +10)")]:
        subset = [g for g in games if g["convergence"] == conv_val]
        if not subset:
            continue
        avg_at = sum(g["actual_total"] for g in subset) / len(subset)
        avg_mt = sum(g["model_total"] for g in subset) / len(subset)

        # How often does UNDER/OVER actually hit vs 8.5 line?
        under_hit = sum(1 for g in subset if g["actual_total"] < 8.5)
        over_hit = sum(1 for g in subset if g["actual_total"] > 8.5)

        print(f"  {label}: {len(subset)} games")
        print(f"    Avg model: {avg_mt:.1f}, Avg actual: {avg_at:.1f}")
        print(f"    Under 8.5: {under_hit}/{len(subset)} ({under_hit/len(subset)*100:.1f}%), "
              f"Over 8.5: {over_hit}/{len(subset)} ({over_hit/len(subset)*100:.1f}%)")

    # --- Monthly breakdown for strong signals ---
    print(f"\n{div}")
    print("MONTHLY BREAKDOWN (O/U strength >= 15, proxy line 8.5)")
    print(div)

    from collections import defaultdict
    monthly = defaultdict(lambda: {"over_w": 0, "over_n": 0, "under_w": 0, "under_n": 0})

    for g in games:
        ou_strength = abs(g["ou_score"] - 50)
        if ou_strength < 15:
            continue
        month = g["date"][:7]
        if g["ou_score"] >= 50:
            monthly[month]["over_n"] += 1
            if g["actual_total"] > 8.5:
                monthly[month]["over_w"] += 1
        else:
            monthly[month]["under_n"] += 1
            if g["actual_total"] < 8.5:
                monthly[month]["under_w"] += 1

    print(f"  {'Month':>7} | {'Ov W/N':>7} {'OvWR':>5} | {'Un W/N':>7} {'UnWR':>5} | {'Total':>7}")
    for m in sorted(monthly.keys()):
        d = monthly[m]
        ov = f"{d['over_w']}/{d['over_n']}"
        un = f"{d['under_w']}/{d['under_n']}"
        ov_wr = d["over_w"] / d["over_n"] * 100 if d["over_n"] else 0
        un_wr = d["under_w"] / d["under_n"] * 100 if d["under_n"] else 0
        tot = d["over_n"] + d["under_n"]
        print(f"  {m:>7} | {ov:>7} {ov_wr:>4.1f}% | {un:>7} {un_wr:>4.1f}% | {tot:>7}")


if __name__ == "__main__":
    run()
