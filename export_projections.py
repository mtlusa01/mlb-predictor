#!/usr/bin/env python3
"""
MLB Export — produces mlb_game_projections.json for the mattev-sports dashboard.

Reads predictions from mlb_model.py output (or re-generates them from DB),
applies Vegas line preservation logic, and exports in website-compatible format.
"""

import json
import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "mlb_data.db")
MODEL_OUTPUT = os.path.join(os.path.dirname(__file__), "mlb_game_projections.json")
EXPORT_PATH = MODEL_OUTPUT  # Same file — mlb_model.py writes, this script enhances


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_vegas_lines(conn):
    """Load original Vegas lines from DB (persists even after games go live)."""
    rows = conn.execute("SELECT * FROM vegas_lines").fetchall()
    return {r["game_id"]: dict(r) for r in rows}


def load_team_names(conn):
    """Build abbreviation -> full name mapping."""
    rows = conn.execute("SELECT team_abbr, team_name FROM team_stats").fetchall()
    return {r["team_abbr"]: r["team_name"] for r in rows}


def enhance_predictions(predictions, vegas_map):
    """Ensure predictions have Vegas lines (from original fetch, not live)."""
    for p in predictions:
        gid = p.get("game_id", "")
        v = vegas_map.get(gid, {})

        # Preserve original Vegas lines even for live/final games
        if v:
            if p.get("spread_line") is None and v.get("spread_home") is not None:
                p["spread_line"] = v["spread_home"]
            if p.get("spread_odds") is None and v.get("spread_home_odds") is not None:
                p["spread_odds"] = v["spread_home_odds"]
            if p.get("total_line") is None and v.get("total_line") is not None:
                p["total_line"] = v["total_line"]
            if p.get("total_odds") is None and v.get("over_odds") is not None:
                p["total_odds"] = v["over_odds"]
            if p.get("ml_odds") is None:
                # Figure out which side we picked
                if p.get("ml_pick") == p.get("home_team") and v.get("ml_home") is not None:
                    p["ml_odds"] = v["ml_home"]
                elif v.get("ml_away") is not None:
                    p["ml_odds"] = v["ml_away"]

    return predictions


def run():
    print("MLB Export")
    print("=" * 50)

    # Check if model output exists
    if not os.path.exists(MODEL_OUTPUT):
        print(f"No model output found at {MODEL_OUTPUT}")
        print("Run: python mlb_model.py --today")
        return

    with open(MODEL_OUTPUT, "r") as f:
        data = json.load(f)

    predictions = data.get("games", [])
    if not predictions:
        print("No predictions to export.")
        return

    conn = get_db()

    # Enhance with preserved Vegas lines
    vegas_map = load_vegas_lines(conn)
    predictions = enhance_predictions(predictions, vegas_map)

    # Build team names map
    team_names = load_team_names(conn)

    # Update scores from DB for live/final games
    games_db = conn.execute("SELECT * FROM todays_games").fetchall()
    scores = {dict(r)["game_id"]: dict(r) for r in games_db}

    for p in predictions:
        gid = p.get("game_id", "")
        db_game = scores.get(gid, {})
        if db_game:
            # Update status and scores from DB
            if db_game.get("status"):
                p["status"] = db_game["status"]
            if db_game.get("away_score") is not None:
                p["away_score"] = db_game["away_score"]
            if db_game.get("home_score") is not None:
                p["home_score"] = db_game["home_score"]

    # Rebuild best bets
    best_bets = []
    for p in predictions:
        if p.get("best_bet_confidence") and p["best_bet_confidence"] >= 60:
            best_bets.append({
                "game": f"{p['away_team']} @ {p['home_team']}",
                "type": p["best_bet"],
                "confidence": p["best_bet_confidence"],
            })
    best_bets.sort(key=lambda x: x["confidence"], reverse=True)

    # Format output
    output = {
        "date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
        "sport": "MLB",
        "updated": datetime.now().isoformat(),
        "games": predictions,
        "best_bets": best_bets[:5],
        "team_names": team_names,
    }

    with open(EXPORT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    conn.close()

    print(f"Exported {len(predictions)} games to {EXPORT_PATH}")
    scheduled = sum(1 for p in predictions if p.get("status") == "scheduled")
    live = sum(1 for p in predictions if p.get("status") == "live")
    final = sum(1 for p in predictions if p.get("status") == "final")
    print(f"  Scheduled: {scheduled}, Live: {live}, Final: {final}")
    if best_bets:
        print(f"  Best bets ({len(best_bets)}):")
        for bb in best_bets[:5]:
            print(f"    {bb['game']} — {bb['type'].upper()} ({bb['confidence']}%)")


if __name__ == "__main__":
    run()
