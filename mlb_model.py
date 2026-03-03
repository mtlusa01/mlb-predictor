#!/usr/bin/env python3
"""
MLB Prediction Model — pitcher-centric with park factors.

Weights (approximate):
  Starting pitcher quality: 35%
  Team batting vs pitcher type: 20%
  Bullpen strength: 15%
  Home/away advantage: 10%
  Park factor: 8%
  Recent form/momentum: 7%
  Rest/travel: 5%

Usage:
  python mlb_model.py --today          # Predict today's games
  python mlb_model.py --date 2026-04-01  # Predict for a specific date
"""

import argparse
import json
import math
import os
import sqlite3
import sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "mlb_data.db")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "mlb_game_projections.json")

# ── Park Factors ──
PARK_FACTORS = {
    "COL": 1.32, "CIN": 1.08, "TEX": 1.06, "BOS": 1.05, "CHC": 1.04,
    "PHI": 1.03, "TOR": 1.03, "ATL": 1.02, "MIL": 1.01, "BAL": 1.01,
    "NYY": 1.00, "HOU": 1.00, "LAA": 1.00, "DET": 0.99, "MIN": 0.99,
    "CLE": 0.98, "ARI": 0.98, "WSH": 0.98, "STL": 0.97, "KC":  0.97,
    "CHW": 0.97, "PIT": 0.96, "SD":  0.95, "SF":  0.95, "TB":  0.95,
    "LAD": 0.94, "MIA": 0.93, "NYM": 0.93, "OAK": 0.92, "ATH": 0.98, "SEA": 0.91,
}

# League averages (2024-25 baseline, updated as season progresses)
LEAGUE_AVG = {
    "era": 4.20,
    "ops": 0.720,
    "runs_per_game": 4.50,
    "k_pct": 22.0,
    "bb_pct": 8.0,
    "whip": 1.30,
    "k_per_9": 8.5,
    "bb_per_9": 3.2,
    "hr_per_9": 1.2,
}

# Preseason OPS estimates (used when season < 30 games played)
# Based on 2025 projections — will be replaced by real stats as season progresses
PRESEASON_OPS = {
    "LAD": 0.780, "NYY": 0.770, "ATL": 0.760, "HOU": 0.755, "PHI": 0.750,
    "BAL": 0.745, "SD":  0.740, "SEA": 0.735, "TEX": 0.735, "BOS": 0.735,
    "NYM": 0.730, "SF":  0.730, "TOR": 0.725, "MIN": 0.725, "CLE": 0.720,
    "MIL": 0.720, "ARI": 0.720, "CHC": 0.715, "TB":  0.715, "CIN": 0.715,
    "STL": 0.710, "KC":  0.710, "DET": 0.705, "PIT": 0.705, "LAA": 0.700,
    "WSH": 0.700, "MIA": 0.695, "COL": 0.710, "OAK": 0.690, "ATH": 0.690, "CHW": 0.685,
}

PRESEASON_ERA = {
    "LAD": 3.40, "ATL": 3.50, "HOU": 3.55, "PHI": 3.60, "BAL": 3.65,
    "NYY": 3.70, "SD":  3.70, "SEA": 3.75, "CLE": 3.75, "MIN": 3.80,
    "MIL": 3.80, "SF":  3.85, "TB":  3.85, "TEX": 3.90, "BOS": 3.90,
    "NYM": 3.90, "TOR": 3.95, "ARI": 3.95, "CHC": 4.00, "DET": 4.00,
    "KC":  4.05, "CIN": 4.10, "STL": 4.10, "PIT": 4.15, "WSH": 4.20,
    "LAA": 4.25, "MIA": 4.30, "OAK": 4.40, "ATH": 4.40, "CHW": 4.50, "COL": 5.00,
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ────────────────────────────────────────────
#  Core Model Functions
# ────────────────────────────────────────────

def season_weight(games_played):
    """How much to weight current season stats vs preseason projections.
    Returns (current_weight, preseason_weight) that sum to 1.0.
    0 games -> 100% preseason, 30+ games -> 100% current."""
    gp = max(0, games_played or 0)
    current = min(gp / 30.0, 1.0)
    return current, 1.0 - current


def effective_ops(team):
    """Blend current season OPS with preseason projection."""
    abbr = team["team_abbr"]
    gp = team["games_played"] or 0
    current_ops = team["ops"] or LEAGUE_AVG["ops"]
    preseason_ops = PRESEASON_OPS.get(abbr, LEAGUE_AVG["ops"])
    cw, pw = season_weight(gp)
    return cw * current_ops + pw * preseason_ops


def effective_era(team):
    """Blend current season ERA with preseason projection."""
    abbr = team["team_abbr"]
    gp = team["games_played"] or 0
    current_era = team["era"] or LEAGUE_AVG["era"]
    preseason_era = PRESEASON_ERA.get(abbr, LEAGUE_AVG["era"])
    cw, pw = season_weight(gp)
    return cw * current_era + pw * preseason_era


def calculate_base_runs(team):
    """Estimate runs per game from team batting stats."""
    ops = effective_ops(team)

    # Base: OPS * 5.2 (empirical relationship)
    base = ops * 5.2

    # K% penalty (league avg ~22%)
    k_pct = team["k_pct"] or LEAGUE_AVG["k_pct"]
    k_penalty = (k_pct - LEAGUE_AVG["k_pct"]) * 0.02
    base -= k_penalty

    # BB% bonus (league avg ~8%)
    bb_pct = team["bb_pct"] or LEAGUE_AVG["bb_pct"]
    bb_bonus = (bb_pct - LEAGUE_AVG["bb_pct"]) * 0.03
    base += bb_bonus

    # ISO power bonus (isolated power above average adds runs)
    iso = team["isolated_power"] or 0.150
    iso_bonus = (iso - 0.150) * 2.0
    base += iso_bonus

    return max(2.5, min(7.5, base))


def stabilize_pitcher_era(era, games_played=0):
    """Stabilize pitcher ERA, especially for spring training / early season.
    Clamp to reasonable range and regress toward league average."""
    league_era = LEAGUE_AVG["era"]
    if era is None:
        return league_era

    # Clamp to reasonable range (no pitcher sustains <1.5 or >7.0 in regular season)
    clamped = max(1.50, min(7.00, era))

    # Regress toward league average — more regression in early season
    # Similar to season_weight: at 0 games, 70% league avg; at 30+ games, 0% regression
    gp = max(0, games_played or 0)
    regression = max(0, 1.0 - gp / 30.0) * 0.7
    return clamped * (1 - regression) + league_era * regression


def adjust_for_pitcher(base_runs, pitcher_era, pitcher_whip=None, games_played=0):
    """Adjust run expectancy based on opposing starting pitcher.

    A pitcher with ERA below league avg reduces runs, above increases them.
    Blend ERA and WHIP for more robust estimate.
    Starters pitch ~5.5 innings on average, so they affect ~61% of the game.
    """
    league_era = LEAGUE_AVG["era"]

    pitcher_era = stabilize_pitcher_era(pitcher_era, games_played)

    # Use WHIP as secondary signal
    if pitcher_whip is not None:
        # Convert WHIP to ERA-equivalent (rough: WHIP * 3.2)
        whip_era = max(1.50, min(7.00, pitcher_whip * 3.2))
        # Blend: 60% ERA, 40% WHIP-derived (more predictive in small samples)
        pitcher_quality = 0.6 * pitcher_era + 0.4 * whip_era
    else:
        pitcher_quality = pitcher_era

    # Starter affects ~61% of the game (5.5 of 9 innings)
    starter_impact = 0.61
    adjustment = (pitcher_quality / league_era)

    # Weighted adjustment: starter impact portion adjusted, rest stays at 1.0
    full_adj = starter_impact * adjustment + (1.0 - starter_impact) * 1.0

    return base_runs * full_adj


def adjust_for_bullpen(base_runs, team_stats):
    """Adjust for bullpen quality. Team ERA already includes bullpen,
    so this is a smaller secondary adjustment based on team pitching ERA
    vs league average, focusing on the bullpen portion (~39% of game)."""
    team_era = effective_era(team_stats)
    league_era = LEAGUE_AVG["era"]

    bullpen_impact = 0.39  # ~3.5 innings of 9
    bullpen_adj = (team_era / league_era)

    # Scale down — bullpen ERA is already partially captured
    full_adj = bullpen_impact * bullpen_adj + (1.0 - bullpen_impact) * 1.0

    return base_runs * full_adj


def adjust_for_park(runs, home_team_abbr):
    """Apply park factor to expected runs."""
    pf = PARK_FACTORS.get(home_team_abbr, 1.0)
    return runs * pf


def adjust_for_home(runs, is_home):
    """Home teams score ~3% more runs historically in MLB."""
    if is_home:
        return runs * 1.03
    return runs


def adjust_for_form(runs, team_stats):
    """Adjust for recent form — hot/cold streaks.
    Uses last 10 games record and run differential."""
    # Parse last_ten like "7-3" or "W7-L3"
    last_ten = team_stats.get("last_ten") or ""
    streak = team_stats.get("streak") or ""

    form_adj = 1.0

    # Parse last 10
    try:
        if "-" in str(last_ten):
            parts = str(last_ten).replace("W", "").replace("L", "").split("-")
            if len(parts) == 2:
                w = int(parts[0])
                l = int(parts[1])
                # 7-3 = +0.02, 3-7 = -0.02, 5-5 = 0
                form_adj += (w - l) * 0.004
    except (ValueError, TypeError):
        pass

    # Streak bonus/penalty (capped at ±3%)
    try:
        if streak:
            s = str(streak)
            if s.startswith("W"):
                wins = int(s[1:])
                form_adj += min(wins * 0.005, 0.03)
            elif s.startswith("L"):
                losses = int(s[1:])
                form_adj -= min(losses * 0.005, 0.03)
    except (ValueError, TypeError):
        pass

    return runs * max(0.92, min(1.08, form_adj))


def calculate_win_probability(home_runs, away_runs):
    """Calculate home win probability using Pythagorean expectation.
    Uses exponent of 1.83 (Bill James' baseball Pythagorean)."""
    exp = 1.83
    if home_runs <= 0 and away_runs <= 0:
        return 0.5
    if away_runs <= 0:
        return 0.95
    if home_runs <= 0:
        return 0.05

    home_wp = (home_runs ** exp) / (home_runs ** exp + away_runs ** exp)
    return max(0.05, min(0.95, home_wp))


def implied_probability(american_odds):
    """Convert American odds to implied probability."""
    if american_odds is None:
        return None
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    else:
        return abs(american_odds) / (abs(american_odds) + 100.0)


def calculate_spread_confidence(run_diff, vegas_spread):
    """Confidence for spread pick.
    Higher when model strongly disagrees with Vegas."""
    if vegas_spread is None:
        # No Vegas line — use raw run diff magnitude
        edge = abs(run_diff)
        return max(45, min(75, 50 + edge * 5))

    # Edge = how far model disagrees with Vegas
    # MLB run line is typically ±1.5
    edge = abs(run_diff - (-vegas_spread))
    conf = 50 + edge * 8
    return max(45, min(85, conf))


def calculate_total_confidence(proj_total, vegas_total):
    """Confidence for total (over/under) pick."""
    if vegas_total is None:
        return max(45, min(70, 50 + abs(proj_total - 8.5) * 3))

    edge = abs(proj_total - vegas_total)
    conf = 50 + edge * 6
    return max(45, min(85, conf))


def calculate_ml_confidence(home_wp, vegas_ml_home=None):
    """Confidence for moneyline pick."""
    # Base: magnitude of win probability away from 50%
    edge_from_50 = abs(home_wp - 0.5)
    conf = 50 + edge_from_50 * 50

    # If we have Vegas odds, add edge bonus
    if vegas_ml_home is not None:
        implied = implied_probability(vegas_ml_home)
        if implied:
            model_edge = abs(home_wp - implied)
            conf += model_edge * 20

    return max(45, min(85, conf))


# ────────────────────────────────────────────
#  Prediction Pipeline
# ────────────────────────────────────────────

def predict_game(game, away_stats, home_stats, vegas):
    """Generate full prediction for a single game."""

    # 1. Base run expectancy from batting
    away_base = calculate_base_runs(away_stats)
    home_base = calculate_base_runs(home_stats)

    # 2. Adjust for opposing starting pitcher (35% weight factor)
    home_pitcher_era = game["home_pitcher_era"]
    away_pitcher_era = game["away_pitcher_era"]
    # Use home team's games_played for pitcher ERA stabilization
    home_gp = home_stats.get("games_played") or 0
    away_gp = away_stats.get("games_played") or 0
    # Away team faces home pitcher, home team faces away pitcher
    away_runs = adjust_for_pitcher(away_base, home_pitcher_era, games_played=home_gp)
    home_runs = adjust_for_pitcher(home_base, away_pitcher_era, games_played=away_gp)

    # 3. Adjust for opposing bullpen (15% weight)
    away_runs = adjust_for_bullpen(away_runs, home_stats)
    home_runs = adjust_for_bullpen(home_runs, away_stats)

    # 4. Park factor (8% weight)
    home_abbr = game["home_team"]
    away_runs = adjust_for_park(away_runs, home_abbr)
    home_runs = adjust_for_park(home_runs, home_abbr)

    # 5. Home advantage (10% weight — ~54% historical MLB)
    home_runs = adjust_for_home(home_runs, is_home=True)

    # 6. Recent form adjustment (7% weight)
    away_runs = adjust_for_form(away_runs, away_stats)
    home_runs = adjust_for_form(home_runs, home_stats)

    # 7. Generate predictions
    proj_total = away_runs + home_runs
    run_diff = home_runs - away_runs  # Positive = home favored

    home_wp = calculate_win_probability(home_runs, away_runs)
    away_wp = 1.0 - home_wp

    # Vegas lines
    v_spread = vegas.get("spread_home") if vegas else None
    v_total = vegas.get("total_line") if vegas else None
    v_ml_home = vegas.get("ml_home") if vegas else None
    v_ml_away = vegas.get("ml_away") if vegas else None
    v_spread_home_odds = vegas.get("spread_home_odds") if vegas else None
    v_spread_away_odds = vegas.get("spread_away_odds") if vegas else None
    v_over_odds = vegas.get("over_odds") if vegas else None
    v_under_odds = vegas.get("under_odds") if vegas else None

    # ── Spread pick ──
    # MLB run line is typically ±1.5
    spread_line = v_spread if v_spread is not None else -1.5
    if run_diff > 0.5:
        spread_pick = home_abbr
        spread_display = f"{home_abbr} {spread_line}"
        spread_odds = v_spread_home_odds
    else:
        spread_pick = game["away_team"]
        away_spread = -spread_line if v_spread is not None else 1.5
        spread_display = f"{game['away_team']} +{abs(away_spread)}"
        spread_odds = v_spread_away_odds

    spread_conf = calculate_spread_confidence(run_diff, v_spread)

    # ── Total pick ──
    total_line = v_total if v_total is not None else 8.5
    if proj_total > total_line:
        total_pick = "OVER"
        total_odds = v_over_odds
    else:
        total_pick = "UNDER"
        total_odds = v_under_odds

    total_conf = calculate_total_confidence(proj_total, v_total)

    # ── ML pick ──
    if home_wp >= 0.5:
        ml_pick = home_abbr
        ml_odds = v_ml_home
    else:
        ml_pick = game["away_team"]
        ml_odds = v_ml_away

    ml_conf = calculate_ml_confidence(home_wp, v_ml_home)

    # ── Best bet ──
    bets = [
        ("spread", spread_conf),
        ("total", total_conf),
        ("ml", ml_conf),
    ]
    best = max(bets, key=lambda x: x[1])
    best_bet_type = best[0]
    best_bet_conf = best[1]

    return {
        "away_team": game["away_team"],
        "home_team": home_abbr,
        "away_pitcher": game["away_pitcher"] or "TBD",
        "home_pitcher": game["home_pitcher"] or "TBD",
        "commence_time": game["commence_time"],
        "game_id": game["game_id"],
        "venue": game["venue"],

        "proj_away_runs": round(away_runs, 1),
        "proj_home_runs": round(home_runs, 1),
        "proj_total": round(proj_total, 1),
        "proj_score": f"{round(away_runs, 1)}-{round(home_runs, 1)}",
        "home_win_prob": round(home_wp * 100, 1),
        "away_win_prob": round(away_wp * 100, 1),

        "spread_pick": spread_display,
        "spread_line": spread_line,
        "spread_odds": spread_odds,
        "spread_conf": round(spread_conf, 1),

        "total_pick": total_pick,
        "total_line": total_line,
        "total_odds": total_odds,
        "total_conf": round(total_conf, 1),

        "ml_pick": ml_pick,
        "ml_odds": ml_odds,
        "ml_conf": round(ml_conf, 1),

        "best_bet": best_bet_type,
        "best_bet_confidence": round(best_bet_conf, 1),

        "status": game["status"],
        "away_score": game["away_score"],
        "home_score": game["home_score"],
    }


def run_predictions(target_date=None):
    """Run predictions for all games."""
    conn = get_db()

    today = target_date or datetime.now().strftime("%Y-%m-%d")
    print(f"MLB Predictions for {today}")
    print("=" * 50)

    # Load games
    games = conn.execute(
        "SELECT * FROM todays_games WHERE game_date = ? ORDER BY commence_time",
        (today,)
    ).fetchall()

    if not games:
        # Try without date filter (might have been loaded without date)
        games = conn.execute("SELECT * FROM todays_games ORDER BY commence_time").fetchall()

    if not games:
        print("No games found. Run get_data.py first.")
        conn.close()
        return []

    print(f"Found {len(games)} games\n")

    # Load team stats
    team_rows = conn.execute("SELECT * FROM team_stats").fetchall()
    teams = {r["team_abbr"]: dict(r) for r in team_rows}

    # Load Vegas lines
    vegas_rows = conn.execute("SELECT * FROM vegas_lines").fetchall()
    vegas = {r["game_id"]: dict(r) for r in vegas_rows}

    predictions = []
    for g in games:
        g = dict(g)

        # Skip finals (already done)
        if g["status"] == "final":
            # Still include in output with scores
            predictions.append({
                "away_team": g["away_team"],
                "home_team": g["home_team"],
                "away_pitcher": g["away_pitcher"] or "TBD",
                "home_pitcher": g["home_pitcher"] or "TBD",
                "commence_time": g["commence_time"],
                "game_id": g["game_id"],
                "venue": g["venue"],
                "status": "final",
                "away_score": g["away_score"],
                "home_score": g["home_score"],
            })
            continue

        away_stats = teams.get(g["away_team"])
        home_stats = teams.get(g["home_team"])

        if not away_stats or not home_stats:
            print(f"  SKIP {g['away_team']} @ {g['home_team']} — missing team stats")
            continue

        v = vegas.get(g["game_id"], {})

        pred = predict_game(g, away_stats, home_stats, v)
        predictions.append(pred)

        # Print prediction
        sp = pred["spread_pick"]
        tp = f"{pred['total_pick']} {pred['total_line']}"
        mp = pred["ml_pick"]
        print(f"  {g['away_team']} @ {g['home_team']}")
        print(f"    Pitchers: {pred['away_pitcher']} vs {pred['home_pitcher']}")
        print(f"    Proj: {pred['proj_score']} (total {pred['proj_total']})")
        print(f"    Spread: {sp} ({pred['spread_conf']}%)")
        print(f"    Total:  {tp} ({pred['total_conf']}%)")
        print(f"    ML:     {mp} ({pred['ml_conf']}%) — WP: {pred['home_win_prob']}%")
        if pred["best_bet"]:
            print(f"    Best Bet: {pred['best_bet'].upper()} ({pred['best_bet_confidence']}%)")
        print()

    conn.close()
    return predictions


def export_json(predictions, target_date=None):
    """Export predictions to JSON."""
    today = target_date or datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()

    # Identify best bets (confidence >= 60%)
    best_bets = []
    for p in predictions:
        if p.get("best_bet_confidence") and p["best_bet_confidence"] >= 60:
            best_bets.append({
                "game": f"{p['away_team']} @ {p['home_team']}",
                "type": p["best_bet"],
                "confidence": p["best_bet_confidence"],
            })
    best_bets.sort(key=lambda x: x["confidence"], reverse=True)

    output = {
        "date": today,
        "sport": "MLB",
        "updated": now,
        "games": predictions,
        "best_bets": best_bets[:5],  # Top 5
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Exported {len(predictions)} games to {OUTPUT_PATH}")
    if best_bets:
        print(f"Best bets: {len(best_bets)} games with 60%+ confidence")
        for bb in best_bets[:5]:
            print(f"  {bb['game']} — {bb['type'].upper()} ({bb['confidence']}%)")


def main():
    parser = argparse.ArgumentParser(description="MLB Prediction Model")
    parser.add_argument("--today", action="store_true", help="Predict today's games")
    parser.add_argument("--date", help="Predict for a specific date (YYYY-MM-DD)")
    parser.add_argument("--json", help="Output JSON path", default=None)
    args = parser.parse_args()

    if not args.today and not args.date:
        parser.print_help()
        sys.exit(1)

    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    global OUTPUT_PATH
    if args.json:
        OUTPUT_PATH = args.json

    predictions = run_predictions(target_date)
    if predictions:
        export_json(predictions, target_date)


if __name__ == "__main__":
    main()
