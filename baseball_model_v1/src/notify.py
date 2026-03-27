"""
notify.py — Format and print the daily output report to terminal.

Prints bet signals (expanded) and no-bet games (minimal).
No email, Slack, or SMS — terminal only.
"""

from datetime import datetime
from typing import Optional, List


def print_report(date_str: str, games: List[dict], pass_version: str = "morning") -> str:
    """Format and print the full daily report.

    Args:
        date_str: Date string (YYYY-MM-DD)
        games: List of game dicts with scoring and signal data
        pass_version: 'morning' or 'final'

    Returns:
        The formatted report string.
    """
    # Parse date for display
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        display_date = dt.strftime("%A %B %d")
    except ValueError:
        display_date = date_str

    total_games = len(games)

    # Separate bet signals from no-bet games
    ml_signals = []
    rl_alerts = []
    rl_plus_signals = []
    diff_signals = []
    ou_signals = []
    no_bet_games = []

    for g in games:
        has_signal = False
        if g.get("bet_signal") not in (None, "NO BET"):
            ml_signals.append(g)
            has_signal = True
        if g.get("rl_alert"):
            rl_alerts.append(g)
        if g.get("rl_plus_signal") not in (None, "NO BET"):
            rl_plus_signals.append(g)
            has_signal = True
        if g.get("diff_signal") not in (None, "NO BET"):
            diff_signals.append(g)
            has_signal = True
        if g.get("ou_signal") not in (None, "NO BET"):
            ou_signals.append(g)
            has_signal = True
        if not has_signal:
            no_bet_games.append(g)

    # Build report
    lines = []
    divider = "=" * 60
    thin_divider = "-" * 60

    # Header
    pass_label = "FINAL SIGNALS" if pass_version == "final" else "MORNING SIGNALS"
    lines.append(divider)
    lines.append(
        f"MLB MODEL -- {pass_label}  |  {display_date}  |  {total_games} games"
    )
    lines.append(
        f"Thresholds: ML >= {_get_threshold('ML_EDGE_THRESHOLD', 65)}  |  "
        f"RL alert >= {_get_threshold('RL_EDGE_THRESHOLD', 75)}  |  "
        f"O/U >= {_get_threshold('OU_EDGE_THRESHOLD', 65)}  |  "
        f"Value >= +{int(_get_threshold('VALUE_EDGE_MIN', 0.04) * 100)}%"
    )
    lines.append(
        f"Signals: {len(ml_signals)} ML  |  {len(diff_signals)} DIFF  |  "
        f"{len(rl_plus_signals)} RL+1.5  |  "
        f"{len(rl_alerts)} RL ALERT  |  "
        f"{len(ou_signals)} O/U  |  {len(no_bet_games)} no bet"
    )
    lines.append(divider)
    lines.append("")

    # Bet signal rows (expanded)
    for g in ml_signals:
        lines.extend(_format_ml_signal(g))
        lines.append("")

    for g in rl_plus_signals:
        if g not in ml_signals:  # avoid double printing games already shown as ML
            lines.extend(_format_rl_plus_signal(g))
            lines.append("")

    for g in diff_signals:
        lines.extend(_format_diff_signal(g))
        lines.append("")

    for g in ou_signals:
        if g not in ml_signals:  # avoid double printing
            lines.extend(_format_ou_signal(g))
            lines.append("")

    # No-bet rows (minimal)
    if no_bet_games:
        lines.append(thin_divider)
        lines.append(f"NO BET -- {len(no_bet_games)} games")
        lines.append("")
        for g in no_bet_games:
            lines.append(_format_no_bet(g))

    lines.append(divider)

    report = "\n".join(lines)
    print(report)
    return report


def _format_ml_signal(g: dict) -> List[str]:
    """Format an expanded ML bet signal row."""
    lines = []

    # Signal badge
    badges = []
    unconf = " (UNCONFIRMED)" if g.get("unconfirmed") else ""
    low_conf = " ** LOW CONFIDENCE **" if g.get("data_confidence") == "LOW CONFIDENCE" else ""

    badges.append(f"BET ML{unconf}")
    if g.get("rl_alert"):
        badges.append("RL_ALERT")

    badge_str = " + ".join(f"[{b}]" for b in badges)

    away = g.get("away_abbrev", g.get("away_team", "???"))
    home = g.get("home_abbrev", g.get("home_team", "???"))
    game_time = _format_time(g.get("game_time", ""))

    lines.append(f"{badge_str}{low_conf} {away} @ {home}  {game_time}")

    # Starters and venue
    away_sp = g.get("away_starter_name", "TBD")
    home_sp = g.get("home_starter_name", "TBD")
    venue = g.get("venue", "")
    lines.append(f"  {away_sp} vs. {home_sp}  |  {venue}")

    # Edge score and bullpen
    home_edge = g.get("home_edge_score", 0)
    away_edge = g.get("away_edge_score", 0)
    home_bp = g.get("home_bullpen_score", 0)
    away_bp = g.get("away_bullpen_score", 0)
    lines.append(
        f"  Edge score:     {max(home_edge, away_edge):.0f}  |  "
        f"Bullpen: {home} {home_bp:.0f}  {away} {away_bp:.0f}"
    )

    # Moneyline
    bet_side = g.get("bet_side", "")
    if bet_side == "HOME":
        ml = g.get("home_moneyline")
        ml_team = home
    else:
        ml = g.get("away_moneyline")
        ml_team = away
    ml_str = _format_line(ml)
    lines.append(f"  Moneyline:      {ml_team} {ml_str}")

    # Run line (if RL alert)
    if g.get("rl_alert"):
        if bet_side == "HOME":
            rl = g.get("home_run_line")
        else:
            rl = g.get("away_run_line")
        rl_str = _format_line(rl)
        lines.append(f"  Run line:       {ml_team} -1.5 ({rl_str})")

    # Probabilities
    line_prob = g.get("line_win_prob")
    model_prob = g.get("model_win_prob")
    value_edge = g.get("value_edge")

    if line_prob is not None:
        lines.append(f"  Line implied:   {line_prob * 100:.1f}%")
    if model_prob is not None:
        lines.append(f"  Model prob:     {model_prob * 100:.1f}%")
    if value_edge is not None:
        lines.append(f"  Value edge:     {value_edge * 100:+.1f}%")

    # Show +1.5 run line if it also has value
    rl_plus = g.get("rl_plus_signal")
    if rl_plus not in (None, "NO BET"):
        if bet_side == "HOME":
            rl_odds = g.get("home_run_line")
        else:
            rl_odds = g.get("away_run_line")
        rl_str = _format_line(rl_odds)
        rl_prob = g.get("rl_plus_prob")
        rl_ve = g.get("rl_plus_value_edge")
        lines.append(f"  +1.5 Run Line:  {ml_team} +1.5 ({rl_str})")
        if rl_prob is not None:
            lines.append(f"  +1.5 Model:     {rl_prob * 100:.1f}%")
        if rl_ve is not None:
            lines.append(f"  +1.5 Value:     {rl_ve * 100:+.1f}%")

    return lines


def _format_rl_plus_signal(g: dict) -> List[str]:
    """Format an expanded +1.5 run line signal row."""
    lines = []

    unconf = " (UNCONFIRMED)" if g.get("unconfirmed") else ""
    low_conf = " ** LOW CONFIDENCE **" if g.get("data_confidence") == "LOW CONFIDENCE" else ""

    away = g.get("away_abbrev", g.get("away_team", "???"))
    home = g.get("home_abbrev", g.get("home_team", "???"))
    game_time = _format_time(g.get("game_time", ""))

    bet_side = g.get("bet_side", "")
    if bet_side == "HOME":
        rl_team = home
        rl_odds = g.get("home_run_line")
    else:
        rl_team = away
        rl_odds = g.get("away_run_line")

    rl_str = _format_line(rl_odds)

    lines.append(f"[BET {rl_team} +1.5{unconf}]{low_conf} {away} @ {home}  {game_time}")

    away_sp = g.get("away_starter_name", "TBD")
    home_sp = g.get("home_starter_name", "TBD")
    venue = g.get("venue", "")
    lines.append(f"  {away_sp} vs. {home_sp}  |  {venue}")

    home_edge = g.get("home_edge_score", 0)
    away_edge = g.get("away_edge_score", 0)
    lines.append(f"  Edge score:     {max(home_edge, away_edge):.0f}")

    lines.append(f"  +1.5 Run Line:  {rl_team} +1.5 ({rl_str})")

    rl_prob = g.get("rl_plus_prob")
    rl_line_prob = g.get("rl_plus_line_prob")
    rl_ve = g.get("rl_plus_value_edge")

    if rl_line_prob is not None:
        lines.append(f"  Line implied:   {rl_line_prob * 100:.1f}%")
    if rl_prob is not None:
        lines.append(f"  +1.5 Model:     {rl_prob * 100:.1f}%")
    if rl_ve is not None:
        lines.append(f"  +1.5 Value:     {rl_ve * 100:+.1f}%")

    return lines


def _format_diff_signal(g: dict) -> List[str]:
    """Format an expanded differential bet signal row."""
    lines = []

    unconf = " (UNCONFIRMED)" if g.get("diff_unconfirmed") else ""
    low_conf = " ** LOW CONFIDENCE **" if g.get("data_confidence") == "LOW CONFIDENCE" else ""

    away = g.get("away_abbrev", g.get("away_team", "???"))
    home = g.get("home_abbrev", g.get("home_team", "???"))
    game_time = _format_time(g.get("game_time", ""))

    diff_side = g.get("diff_side", "")
    if diff_side == "HOME":
        bet_team = home
        ml = g.get("home_moneyline")
    else:
        bet_team = away
        ml = g.get("away_moneyline")

    lines.append(f"[BET ML DIFF{unconf}]{low_conf} {away} @ {home}  {game_time}")

    away_sp = g.get("away_starter_name", "TBD")
    home_sp = g.get("home_starter_name", "TBD")
    venue = g.get("venue", "")
    lines.append(f"  {away_sp} vs. {home_sp}  |  {venue}")

    home_edge = g.get("home_edge_score", 0)
    away_edge = g.get("away_edge_score", 0)
    diff_gap = g.get("diff_gap", 0)
    lines.append(
        f"  Edge scores:    {away} {away_edge:.0f} / {home} {home_edge:.0f}  |  "
        f"Gap: {diff_gap:.0f} pts favoring {bet_team}"
    )

    home_bp = g.get("home_bullpen_score", 0)
    away_bp = g.get("away_bullpen_score", 0)
    lines.append(
        f"  Bullpen:        {home} {home_bp:.0f}  {away} {away_bp:.0f}"
    )

    ml_str = _format_line(ml)
    lines.append(f"  Moneyline:      {bet_team} {ml_str}")

    diff_line_prob = g.get("diff_line_prob")
    diff_model_prob = g.get("diff_model_prob")
    diff_value_edge = g.get("diff_value_edge")

    if diff_line_prob is not None:
        lines.append(f"  Line implied:   {diff_line_prob * 100:.1f}%")
    if diff_model_prob is not None:
        lines.append(f"  Model prob:     {diff_model_prob * 100:.1f}%")
    if diff_value_edge is not None:
        lines.append(f"  Value edge:     {diff_value_edge * 100:+.1f}%")

    return lines


def _format_ou_signal(g: dict) -> List[str]:
    """Format an expanded O/U bet signal row."""
    lines = []

    direction = g.get("ou_signal", "OVER")
    unconf = " (UNCONFIRMED)" if g.get("unconfirmed") else ""
    low_conf = " ** LOW CONFIDENCE **" if g.get("data_confidence") == "LOW CONFIDENCE" else ""

    away = g.get("away_abbrev", g.get("away_team", "???"))
    home = g.get("home_abbrev", g.get("home_team", "???"))
    game_time = _format_time(g.get("game_time", ""))

    lines.append(f"[BET {direction}{unconf}]{low_conf} {away} @ {home}  {game_time}")

    away_sp = g.get("away_starter_name", "TBD")
    home_sp = g.get("home_starter_name", "TBD")
    venue = g.get("venue", "")
    lines.append(f"  {away_sp} vs. {home_sp}  |  {venue}")

    ou_line = g.get("ou_line")
    over_odds = _format_line(g.get("ou_over_odds"))
    under_odds = _format_line(g.get("ou_under_odds"))
    lines.append(f"  O/U line:       {ou_line}  ({over_odds} / {under_odds})")

    model_total = g.get("ou_model_total", 0)
    ou_score = g.get("ou_score", 50)
    ou_value = g.get("ou_value_edge")

    lines.append(f"  Model total:    {model_total:.1f}")
    lines.append(f"  O/U score:      {ou_score:.0f}")

    convergence = g.get("ou_convergence_boost", 0)
    if convergence != 0:
        label = "OVER" if convergence > 0 else "UNDER"
        lines.append(f"  Convergence:    {convergence:+.0f} pts ({label} — both pitchers {'weak' if convergence > 0 else 'dominant'})")

    if ou_value is not None:
        lines.append(f"  Value edge:     {ou_value * 100:+.1f}%")

    return lines


def _format_no_bet(g: dict) -> str:
    """Format a minimal no-bet row."""
    away = g.get("away_abbrev", g.get("away_team", "???"))
    home = g.get("home_abbrev", g.get("home_team", "???"))
    game_time = _format_time(g.get("game_time", ""))
    away_sp = g.get("away_starter_name", "TBD")
    home_sp = g.get("home_starter_name", "TBD")

    home_edge = g.get("home_edge_score", 0)
    away_edge = g.get("away_edge_score", 0)
    ou_score = g.get("ou_score", 50)
    value_edge = g.get("value_edge")
    value_str = f"Value: {value_edge * 100:+.1f}%" if value_edge is not None else "Value: N/A"

    return (
        f"{away} @ {home}  {game_time}  |  {away_sp} vs. {home_sp}  |"
        f"Edge: {away} {away_edge:.0f} / {home} {home_edge:.0f}  O/U: {ou_score:.0f}  {value_str}"
    )


def _format_time(game_time: str) -> str:
    """Format game time for display."""
    if not game_time:
        return ""
    try:
        dt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
        # %#I on Windows, %-I on Unix — use lstrip('0') as portable fallback
        return dt.strftime("%I:%M %p ET").lstrip("0")
    except (ValueError, AttributeError):
        return game_time


def _format_line(line: Optional[int]) -> str:
    """Format a moneyline for display."""
    if line is None:
        return "N/A"
    if line > 0:
        return f"+{line}"
    return str(line)


def _get_threshold(name: str, default: float) -> float:
    """Get threshold from env."""
    import os
    val = os.getenv(name)
    if val is not None:
        try:
            return float(val)
        except ValueError:
            pass
    return default
