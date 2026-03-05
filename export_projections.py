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

    # Lock recommendations (enrich with pick info from predictions)
    _write_recommendation_lock(best_bets, predictions, data.get("date", datetime.now().strftime("%Y-%m-%d")))

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


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _write_recommendation_lock(best_bets, predictions, game_date=None):
    """Write mlb_recommended.json — locked once per day."""
    today = game_date or datetime.now().strftime("%Y-%m-%d")
    lock_path = os.path.join(SCRIPT_DIR, "mlb_recommended.json")

    # Check if already locked for today
    try:
        with open(lock_path) as f:
            existing = json.load(f)
        if existing.get("date") == today and existing.get("recommendations"):
            print(f"  mlb_recommended.json LOCKED for {today} — skipping")
            return
    except Exception:
        pass

    # Build lookup for picks from predictions
    pick_lookup = {}
    for p in predictions:
        key = f"{p.get('away_team')} @ {p.get('home_team')}"
        pick_lookup[key] = p

    # Convert best_bets to unified recommendation format
    recs = []
    for i, bb in enumerate(best_bets[:5]):
        game_data = pick_lookup.get(bb["game"], {})
        bt = bb["type"]
        if bt == "spread":
            pick = game_data.get("spread_pick", "")
        elif bt == "total":
            tp = game_data.get("total_pick", "")
            tl = game_data.get("total_line", "")
            pick = f"{tp} {tl}".strip()
        elif bt == "ml":
            pick = game_data.get("ml_pick", "")
        else:
            pick = ""
        recs.append({
            "bet_type": bt.capitalize() if bt != "ml" else "ML",
            "game": bb["game"],
            "pick": pick,
            "confidence": bb["confidence"],
            "away_team": game_data.get("away_team", ""),
            "home_team": game_data.get("home_team", ""),
            "reasons": [f"{bb['confidence']}% model confidence"],
            "rec_rank": i + 1,
        })

    lock_data = {
        "date": today,
        "sport": "MLB",
        "type": "game",
        "locked_at": datetime.now().isoformat(timespec="seconds"),
        "recommendations": recs,
    }

    with open(lock_path, "w") as f:
        json.dump(lock_data, f, indent=2)
    print(f"  Locked {len(recs)} MLB game recommendations")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MLB Export Projections")
    parser.add_argument("--unlock-recommendations", action="store_true",
                        help="Delete locked recommendation file so it regenerates")
    args = parser.parse_args()

    if args.unlock_recommendations:
        lock_path = os.path.join(SCRIPT_DIR, "mlb_recommended.json")
        if os.path.exists(lock_path):
            os.remove(lock_path)
            print(f"  Deleted {lock_path}")
        print("  MLB recommendation lock cleared")

    run()
