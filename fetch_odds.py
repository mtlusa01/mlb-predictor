#!/usr/bin/env python3
"""
MLB Odds Fetcher

Fetches live odds from The Odds API for baseball_mlb.
Maps Odds API team names to ESPN abbreviations.
Preserves Vegas lines for games that have already started.
"""

import os
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_PATH = os.path.join(os.path.dirname(__file__), "mlb_data.db")
ODDS_SPORT_KEY = "baseball_mlb"
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")


# MLB team name mapping: Odds API names -> ESPN abbreviations
MLB_NAME_MAP = {
    # Full names
    "arizona diamondbacks": "ARI",
    "atlanta braves": "ATL",
    "baltimore orioles": "BAL",
    "boston red sox": "BOS",
    "chicago cubs": "CHC",
    "chicago white sox": "CHW",
    "cincinnati reds": "CIN",
    "cleveland guardians": "CLE",
    "colorado rockies": "COL",
    "detroit tigers": "DET",
    "houston astros": "HOU",
    "kansas city royals": "KC",
    "los angeles angels": "LAA",
    "los angeles dodgers": "LAD",
    "miami marlins": "MIA",
    "milwaukee brewers": "MIL",
    "minnesota twins": "MIN",
    "new york mets": "NYM",
    "new york yankees": "NYY",
    "oakland athletics": "ATH",
    "athletics": "ATH",
    "sacramento athletics": "ATH",
    "philadelphia phillies": "PHI",
    "pittsburgh pirates": "PIT",
    "san diego padres": "SD",
    "san francisco giants": "SF",
    "seattle mariners": "SEA",
    "st. louis cardinals": "STL",
    "st louis cardinals": "STL",
    "tampa bay rays": "TB",
    "texas rangers": "TEX",
    "toronto blue jays": "TOR",
    "washington nationals": "WSH",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def match_team(odds_name):
    """Match an Odds API team name to ESPN abbreviation."""
    if not odds_name:
        return None
    lower = odds_name.strip().lower()

    # Direct match
    if lower in MLB_NAME_MAP:
        return MLB_NAME_MAP[lower]

    # Check abbreviation directly
    upper = odds_name.strip().upper()
    all_abbrs = set(MLB_NAME_MAP.values())
    if upper in all_abbrs:
        return upper

    # Partial/substring match
    for key, abbr in MLB_NAME_MAP.items():
        if key in lower or lower in key:
            return abbr

    # Word overlap
    odds_words = set(lower.split())
    best_match = None
    best_overlap = 0
    for key, abbr in MLB_NAME_MAP.items():
        key_words = set(key.split())
        overlap = len(odds_words & key_words)
        if overlap > best_overlap:
            best_overlap = overlap
            best_match = abbr
    if best_overlap >= 1:
        return best_match

    print(f"  WARNING: Could not match '{odds_name}'")
    return None


def fetch_odds():
    """Fetch MLB odds from The Odds API."""
    if not ODDS_API_KEY:
        print("ERROR: No ODDS_API_KEY set")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/{ODDS_SPORT_KEY}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "regions": "us",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    # Show remaining API calls
    remaining = r.headers.get("x-requests-remaining", "?")
    print(f"  Odds API requests remaining: {remaining}")

    return r.json()


def parse_odds(events, conn):
    """Parse odds events and match to today's games."""
    # Load today's games for matching
    rows = conn.execute("SELECT game_id, away_team, home_team, commence_time, status FROM todays_games").fetchall()
    db_games = {(r["away_team"], r["home_team"]): dict(r) for r in rows}

    matched = []
    unmatched = []

    for ev in events:
        away_name = None
        home_name = None
        for team in ev.get("home_team", ""), ev.get("away_team", ""):
            pass
        # The Odds API has home_team and away_team at top level
        home_raw = ev.get("home_team", "")
        away_raw = ev.get("away_team", "")
        home_abbr = match_team(home_raw)
        away_abbr = match_team(away_raw)

        if not home_abbr or not away_abbr:
            unmatched.append(f"{away_raw} @ {home_raw}")
            continue

        key = (away_abbr, home_abbr)
        game = db_games.get(key)
        if not game:
            unmatched.append(f"{away_abbr} @ {home_abbr} (not in today's games)")
            continue

        # Skip live/started games — preserve existing lines
        if game["status"] in ("live", "final"):
            continue

        # Parse bookmaker odds — use consensus (first available)
        spread_home = spread_away = None
        spread_home_odds = spread_away_odds = None
        total_line = None
        over_odds = under_odds = None
        ml_home = ml_away = None

        for bk in ev.get("bookmakers", []):
            for market in bk.get("markets", []):
                mkey = market.get("key", "")
                outcomes = market.get("outcomes", [])

                if mkey == "spreads" and spread_home is None:
                    for o in outcomes:
                        name = match_team(o.get("name", ""))
                        if name == home_abbr:
                            spread_home = o.get("point")
                            spread_home_odds = o.get("price")
                        elif name == away_abbr:
                            spread_away = o.get("point")
                            spread_away_odds = o.get("price")

                elif mkey == "totals" and total_line is None:
                    for o in outcomes:
                        if o.get("name", "").lower() == "over":
                            total_line = o.get("point")
                            over_odds = o.get("price")
                        elif o.get("name", "").lower() == "under":
                            under_odds = o.get("price")

                elif mkey == "h2h" and ml_home is None:
                    for o in outcomes:
                        name = match_team(o.get("name", ""))
                        price = o.get("price")
                        # Sanity check — skip absurd lines
                        if price and abs(price) > 10000:
                            continue
                        if name == home_abbr:
                            ml_home = price
                        elif name == away_abbr:
                            ml_away = price

        matched.append({
            "game_id": game["game_id"],
            "away_team": away_abbr,
            "home_team": home_abbr,
            "spread_home": spread_home,
            "spread_away": spread_away,
            "spread_home_odds": spread_home_odds,
            "spread_away_odds": spread_away_odds,
            "total_line": total_line,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "ml_home": ml_home,
            "ml_away": ml_away,
        })

    return matched, unmatched


def save_vegas_lines(conn, lines):
    """Save odds to vegas_lines table. Only deletes lines we have fresh data for."""
    now = datetime.now(timezone.utc).isoformat()

    # Only delete lines for games we have fresh data
    game_ids = [l["game_id"] for l in lines]
    if game_ids:
        placeholders = ",".join(["?"] * len(game_ids))
        conn.execute(f"DELETE FROM vegas_lines WHERE game_id IN ({placeholders})", game_ids)

    for l in lines:
        conn.execute("""
            INSERT OR REPLACE INTO vegas_lines
            (game_id, away_team, home_team,
             spread_home, spread_away, spread_home_odds, spread_away_odds,
             total_line, over_odds, under_odds,
             ml_home, ml_away, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            l["game_id"], l["away_team"], l["home_team"],
            l["spread_home"], l["spread_away"],
            l["spread_home_odds"], l["spread_away_odds"],
            l["total_line"], l["over_odds"], l["under_odds"],
            l["ml_home"], l["ml_away"], now,
        ))
    conn.commit()


def run():
    print("=" * 50)
    print("MLB Odds Fetcher")
    print("=" * 50)

    conn = get_db()

    # Check we have games
    count = conn.execute("SELECT COUNT(*) FROM todays_games").fetchone()[0]
    if count == 0:
        print("No games in database. Run get_data.py first.")
        conn.close()
        return

    print(f"\n[1/3] Fetching odds from The Odds API...")
    events = fetch_odds()
    print(f"  Got {len(events)} events")

    print(f"\n[2/3] Matching odds to games...")
    matched, unmatched = parse_odds(events, conn)
    print(f"  Matched: {len(matched)}")
    if unmatched:
        print(f"  Unmatched: {len(unmatched)}")
        for u in unmatched[:5]:
            print(f"    - {u}")

    print(f"\n[3/3] Saving Vegas lines...")
    save_vegas_lines(conn, matched)

    for m in matched:
        spread_str = f"spread={m['spread_home']}" if m["spread_home"] is not None else "no spread"
        total_str = f"total={m['total_line']}" if m["total_line"] is not None else "no total"
        ml_str = f"ML home={m['ml_home']}" if m["ml_home"] is not None else "no ML"
        print(f"  {m['away_team']} @ {m['home_team']}: {spread_str}, {total_str}, {ml_str}")

    conn.close()
    print(f"\nOdds saved. {len(matched)} games with lines.")


if __name__ == "__main__":
    run()
