#!/usr/bin/env python3
"""
MLB Grading — fetch final scores, grade predictions, update results.

Same pattern as grade_nhl.py:
1. Fetch completed scores from ESPN (or Odds API)
2. Compare to predictions in mlb_game_projections.json
3. Grade spread, total, ML picks
4. Update mlb_results.json with rolling stats
5. Copy files to mattev-sports and push
"""

import json
import os
import shutil
import subprocess
import sqlite3
import sys
from datetime import datetime, timedelta

import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "mlb_data.db")
PROJ_PATH = os.path.join(os.path.dirname(__file__), "mlb_game_projections.json")
RESULTS_PATH = os.path.join(os.path.dirname(__file__), "mlb_results.json")
MATTEV_DIR = os.path.join(os.path.dirname(__file__), "..", "mattev-sports")

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_scores(date_str=None):
    """Fetch final scores from ESPN scoreboard."""
    params = {}
    if date_str:
        params["dates"] = date_str.replace("-", "")

    r = requests.get(ESPN_SCOREBOARD, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    scores = {}
    for event in data.get("events", []):
        comp = event.get("competitions", [{}])[0]
        status = comp.get("status", {}).get("type", {})
        if not status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        home = away = None
        for c in competitors:
            if c.get("homeAway") == "home":
                home = c
            else:
                away = c

        if not home or not away:
            continue

        away_abbr = away.get("team", {}).get("abbreviation", "")
        home_abbr = home.get("team", {}).get("abbreviation", "")
        away_score = int(away.get("score", 0))
        home_score = int(home.get("score", 0))

        key = f"{away_abbr} @ {home_abbr}"
        scores[key] = {
            "away_team": away_abbr,
            "home_team": home_abbr,
            "away_score": away_score,
            "home_score": home_score,
            "completed": True,
        }

    return scores


def grade_spread(pick_str, away_score, home_score, spread_line):
    """Grade a spread pick. Returns 'W', 'L', or 'P'."""
    if not pick_str or spread_line is None:
        return None

    actual_margin = home_score - away_score  # Positive = home won by X

    # Parse the pick to determine if it's home or away
    # pick_str like "BOS -1.5" or "NYY +1.5"
    parts = pick_str.strip().split()
    if len(parts) < 2:
        return None

    team = parts[0]
    try:
        line = float(parts[1])
    except (ValueError, IndexError):
        line = spread_line

    # Determine if pick is home or away
    # We need to figure out if the picked team won against the spread
    # If pick is home team with line -1.5: home needs to win by > 1.5
    # If pick is away team with line +1.5: away needs to lose by < 1.5

    # actual_margin > 0 means home won
    # For home pick with line (e.g., -1.5): actual_margin + line > 0 means cover
    # For away pick with line (e.g., +1.5): -(actual_margin) + |line| > 0

    # Simpler: check if picked team covered
    covered = actual_margin + line  # from home perspective

    if covered > 0:
        # Home side covered
        return 'W'
    elif covered < 0:
        return 'L'
    else:
        return 'P'


def grade_total(pick_str, away_score, home_score, total_line):
    """Grade an over/under pick. Returns 'W', 'L', or 'P'."""
    if not pick_str or total_line is None:
        return None

    actual_total = away_score + home_score
    pick_upper = pick_str.strip().upper()

    if actual_total > total_line:
        return 'W' if 'OVER' in pick_upper else 'L'
    elif actual_total < total_line:
        return 'W' if 'UNDER' in pick_upper else 'L'
    else:
        return 'P'


def grade_ml(pick_team, away_team, home_team, away_score, home_score):
    """Grade a moneyline pick. Returns 'W', 'L', or 'P'."""
    if not pick_team:
        return None

    if away_score == home_score:
        return 'P'  # Shouldn't happen in MLB (no ties)

    winner = home_team if home_score > away_score else away_team
    return 'W' if pick_team == winner else 'L'


def grade_games():
    """Grade all predictions against actual scores."""
    if not os.path.exists(PROJ_PATH):
        print("No projections file found.")
        return False

    with open(PROJ_PATH, "r") as f:
        data = json.load(f)

    predictions = data.get("games", [])
    pred_date = data.get("date", "")

    if not predictions:
        print("No predictions to grade.")
        return False

    # Fetch scores for the prediction date
    print(f"Fetching scores for {pred_date}...")
    scores = fetch_scores(pred_date)

    if not scores:
        # Try yesterday if running after midnight
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"No scores for {pred_date}, trying {yesterday}...")
        scores = fetch_scores(yesterday)

    if not scores:
        print("No completed games found.")
        return False

    print(f"Found {len(scores)} completed games")

    graded = 0
    for p in predictions:
        key = f"{p['away_team']} @ {p['home_team']}"
        score = scores.get(key)
        if not score:
            continue

        away_score = score["away_score"]
        home_score = score["home_score"]

        # Update scores and status
        p["away_score"] = away_score
        p["home_score"] = home_score
        p["status"] = "final"

        # Grade each bet type
        if p.get("spread_pick"):
            p["spread_result"] = grade_spread(
                p["spread_pick"], away_score, home_score, p.get("spread_line")
            )

        if p.get("total_pick"):
            p["total_result"] = grade_total(
                p["total_pick"], away_score, home_score, p.get("total_line")
            )

        if p.get("ml_pick"):
            p["ml_result"] = grade_ml(
                p["ml_pick"], p["away_team"], p["home_team"], away_score, home_score
            )

        graded += 1
        result_str = f"S:{p.get('spread_result','?')} T:{p.get('total_result','?')} ML:{p.get('ml_result','?')}"
        print(f"  {key}: {away_score}-{home_score} — {result_str}")

    # Save updated projections
    data["games"] = predictions
    data["updated"] = datetime.now().isoformat()
    with open(PROJ_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nGraded {graded}/{len(predictions)} games")
    return graded > 0


def update_results():
    """Update rolling results tracking in mlb_results.json."""
    if not os.path.exists(PROJ_PATH):
        return

    with open(PROJ_PATH, "r") as f:
        data = json.load(f)

    pred_date = data.get("date", "")
    predictions = data.get("games", [])

    # Build picks list for today
    picks = []
    for p in predictions:
        if p.get("status") != "final":
            continue

        game = f"{p['away_team']} @ {p['home_team']}"
        result_str = f"{p.get('away_score', '?')}-{p.get('home_score', '?')}"

        for bet_type in ["spread", "total", "ml"]:
            result = p.get(f"{bet_type}_result")
            if result is None:
                continue

            pick_val = p.get(f"{bet_type}_pick", "")
            conf = p.get(f"{bet_type}_conf", 0)

            if result == 'W':
                hit = True
            elif result == 'L':
                hit = False
            else:
                hit = None  # Push

            picks.append({
                "date": pred_date,
                "type": bet_type,
                "game": game,
                "pick": str(pick_val),
                "result": result_str,
                "hit": hit,
                "confidence": conf,
                "best_bet": False,  # Will be set below
            })

    # Tag top 5 by confidence as best bets
    sorted_picks = sorted(picks, key=lambda x: x["confidence"], reverse=True)
    for i, pick in enumerate(sorted_picks[:5]):
        pick["best_bet"] = True

    # Tally categories
    def tally(pick_list):
        w = sum(1 for p in pick_list if p["hit"] is True)
        l = sum(1 for p in pick_list if p["hit"] is False)
        push = sum(1 for p in pick_list if p["hit"] is None)
        total = w + l
        pct = round(w / total * 100, 1) if total > 0 else 0
        # ROI: assume -110 odds
        profit = w * 90.91 - l * 100
        roi = round(profit / (total * 100) * 100, 1) if total > 0 else 0
        return {"wins": w, "losses": l, "pushes": push, "pct": f"{pct}%", "roi": f"{roi}%"}

    day_entry = {
        "date": pred_date,
        "spreads": tally([p for p in picks if p["type"] == "spread"]),
        "totals": tally([p for p in picks if p["type"] == "total"]),
        "moneylines": tally([p for p in picks if p["type"] == "ml"]),
        "best_bets": tally([p for p in picks if p["best_bet"]]),
        "picks": picks,
    }

    # Load existing results or create new
    if os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH, "r") as f:
            results = json.load(f)
    else:
        results = {"updated": "", "allTime": {}, "days": []}

    # Remove existing entry for this date (re-grade)
    results["days"] = [d for d in results["days"] if d["date"] != pred_date]
    results["days"].append(day_entry)
    results["days"].sort(key=lambda d: d["date"], reverse=True)

    # Recalculate all-time stats
    all_picks = []
    for day in results["days"]:
        all_picks.extend(day.get("picks", []))

    results["allTime"] = {
        "spreads": tally([p for p in all_picks if p["type"] == "spread"]),
        "totals": tally([p for p in all_picks if p["type"] == "total"]),
        "moneylines": tally([p for p in all_picks if p["type"] == "ml"]),
        "best_bets": tally([p for p in all_picks if p["best_bet"]]),
    }
    results["updated"] = datetime.now().isoformat()

    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults updated: {RESULTS_PATH}")
    at = results["allTime"]
    print(f"  All-time: Spreads {at['spreads']['pct']}, Totals {at['totals']['pct']}, "
          f"ML {at['moneylines']['pct']}, Best Bets {at['best_bets']['pct']}")


def copy_and_push():
    """Copy output files to mattev-sports and push."""
    if not os.path.isdir(MATTEV_DIR):
        print(f"mattev-sports not found at {MATTEV_DIR}")
        return

    files_to_copy = [
        (PROJ_PATH, "mlb_game_projections.json"),
        (RESULTS_PATH, "mlb_results.json"),
        (os.path.join(os.path.dirname(__file__), "mlb_recommended.json"), "mlb_recommended.json"),
    ]

    for src, fname in files_to_copy:
        if os.path.exists(src):
            dst = os.path.join(MATTEV_DIR, fname)
            shutil.copy2(src, dst)
            print(f"  Copied {fname} to mattev-sports")

    # Git commit and push
    try:
        subprocess.run(
            ["git", "add", "mlb_game_projections.json", "mlb_results.json", "mlb_recommended.json"],
            cwd=MATTEV_DIR, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "-c", "user.name=mlusa", "-c", "user.email=vesperkicks@gmail.com",
             "commit", "-m", f"MLB predictions update {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
            cwd=MATTEV_DIR, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "push"],
            cwd=MATTEV_DIR, check=True, capture_output=True,
        )
        print("  Pushed to mattev-sports")
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode() if e.stderr else ""
        if "nothing to commit" in stderr:
            print("  Nothing new to commit")
        else:
            print(f"  Git error: {stderr}")


def main():
    print("=" * 50)
    print("MLB Grading")
    print("=" * 50)

    graded = grade_games()
    if graded:
        update_results()
        copy_and_push()
    else:
        print("No games graded.")


if __name__ == "__main__":
    main()
