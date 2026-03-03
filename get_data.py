#!/usr/bin/env python3
"""MLB data collection — fetch team stats, pitcher info, and standings from ESPN APIs."""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "mlb_data.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
ESPN_STANDINGS = "https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ESPN team ID -> abbreviation mapping (built dynamically from teams endpoint)
TEAM_MAP = {}

# ── Park Factors ──
PARK_FACTORS = {
    "COL": 1.32, "CIN": 1.08, "TEX": 1.06, "BOS": 1.05, "CHC": 1.04,
    "PHI": 1.03, "TOR": 1.03, "ATL": 1.02, "MIL": 1.01, "BAL": 1.01,
    "NYY": 1.00, "HOU": 1.00, "LAA": 1.00, "DET": 0.99, "MIN": 0.99,
    "CLE": 0.98, "ARI": 0.98, "WSH": 0.98, "STL": 0.97, "KC":  0.97,
    "CHW": 0.97, "PIT": 0.96, "SD":  0.95, "SF":  0.95, "TB":  0.95,
    "LAD": 0.94, "MIA": 0.93, "NYM": 0.93, "OAK": 0.92, "ATH": 0.98, "SEA": 0.91,
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS todays_games (
            game_id TEXT PRIMARY KEY,
            game_date TEXT,
            commence_time TEXT,
            away_team TEXT,
            home_team TEXT,
            away_id TEXT,
            home_id TEXT,
            away_pitcher TEXT,
            home_pitcher TEXT,
            away_pitcher_era REAL,
            home_pitcher_era REAL,
            away_pitcher_whip REAL,
            home_pitcher_whip REAL,
            away_pitcher_record TEXT,
            home_pitcher_record TEXT,
            away_score INTEGER,
            home_score INTEGER,
            status TEXT,
            venue TEXT,
            indoor INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS team_stats (
            team_abbr TEXT PRIMARY KEY,
            team_id TEXT,
            team_name TEXT,
            -- Record
            wins INTEGER, losses INTEGER, games_played INTEGER,
            win_pct REAL,
            home_wins INTEGER, home_losses INTEGER,
            road_wins INTEGER, road_losses INTEGER,
            streak TEXT, last_ten TEXT,
            runs_scored INTEGER, runs_allowed INTEGER, run_diff INTEGER,
            avg_runs_for REAL, avg_runs_against REAL,
            -- Batting
            batting_avg REAL, obp REAL, slg REAL, ops REAL,
            home_runs INTEGER, runs_per_game REAL,
            strikeouts_bat INTEGER, walks_bat INTEGER,
            k_pct REAL, bb_pct REAL,
            isolated_power REAL,
            plate_appearances INTEGER, at_bats INTEGER,
            -- Pitching (team)
            era REAL, whip REAL,
            k_per_9 REAL, bb_per_9 REAL, hr_per_9 REAL,
            opp_avg REAL, opp_obp REAL, opp_slg REAL, opp_ops REAL,
            quality_starts INTEGER, saves INTEGER, holds INTEGER,
            innings_pitched REAL,
            pitching_strikeouts INTEGER, pitching_walks INTEGER,
            -- Park factor
            park_factor REAL DEFAULT 1.0,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS pitcher_stats (
            pitcher_id TEXT,
            team_abbr TEXT,
            name TEXT,
            era REAL,
            whip REAL,
            wins INTEGER,
            losses INTEGER,
            record TEXT,
            innings_pitched REAL,
            strikeouts INTEGER,
            walks INTEGER,
            k_per_9 REAL,
            bb_per_9 REAL,
            games_started INTEGER,
            quality_starts INTEGER,
            opp_avg REAL,
            updated_at TEXT,
            PRIMARY KEY (pitcher_id, team_abbr)
        );

        CREATE TABLE IF NOT EXISTS vegas_lines (
            game_id TEXT PRIMARY KEY,
            away_team TEXT,
            home_team TEXT,
            spread_home REAL,
            spread_away REAL,
            spread_home_odds INTEGER,
            spread_away_odds INTEGER,
            total_line REAL,
            over_odds INTEGER,
            under_odds INTEGER,
            ml_home INTEGER,
            ml_away INTEGER,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS recent_games (
            game_id TEXT PRIMARY KEY,
            game_date TEXT,
            away_team TEXT,
            home_team TEXT,
            away_score INTEGER,
            home_score INTEGER,
            away_pitcher TEXT,
            home_pitcher TEXT,
            venue TEXT
        );
    """)
    conn.commit()


# ────────────────────────────────────────────
#  ESPN API Fetchers
# ────────────────────────────────────────────

def fetch_teams():
    """Fetch all MLB teams and build ID -> abbreviation map."""
    url = f"{ESPN_BASE}/teams"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json()
    teams = {}
    for sport in data.get("sports", []):
        for league in sport.get("leagues", []):
            for t in league.get("teams", []):
                team = t.get("team", {})
                tid = str(team.get("id", ""))
                abbr = team.get("abbreviation", "")
                name = team.get("displayName", "")
                if tid and abbr:
                    teams[tid] = {"abbr": abbr, "name": name, "id": tid}
    return teams


def fetch_scoreboard(date_str=None):
    """Fetch today's MLB scoreboard. date_str format: YYYYMMDD."""
    url = f"{ESPN_BASE}/scoreboard"
    params = {}
    if date_str:
        params["dates"] = date_str
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_team_statistics(team_id):
    """Fetch batting + pitching stats for a team."""
    url = f"{ESPN_BASE}/teams/{team_id}/statistics"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_standings(season=None):
    """Fetch MLB standings."""
    url = ESPN_STANDINGS
    params = {}
    if season:
        params["season"] = season
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ────────────────────────────────────────────
#  Parsing helpers
# ────────────────────────────────────────────

def parse_stat(stats_list, stat_name, default=None):
    """Extract a stat value from ESPN's stats array by name."""
    for s in stats_list:
        if s.get("name") == stat_name or s.get("abbreviation") == stat_name:
            val = s.get("value")
            if val is not None:
                return float(val)
            # Try displayValue
            dv = s.get("displayValue", "")
            try:
                return float(dv)
            except (ValueError, TypeError):
                pass
    return default


def parse_games(scoreboard_data, game_date):
    """Parse games from ESPN scoreboard into structured dicts."""
    games = []
    for event in scoreboard_data.get("events", []):
        comp = event.get("competitions", [{}])[0]
        status_obj = comp.get("status", {}).get("type", {})
        state = status_obj.get("state", "pre")
        completed = status_obj.get("completed", False)

        # Map state to our status
        if completed or state == "post":
            status = "final"
        elif state == "in":
            status = "live"
        else:
            status = "scheduled"

        competitors = comp.get("competitors", [])
        home = away = None
        for c in competitors:
            if c.get("homeAway") == "home":
                home = c
            else:
                away = c
        if not home or not away:
            continue

        home_team = home.get("team", {})
        away_team = away.get("team", {})

        # Probable pitchers
        def get_probable(competitor):
            for p in competitor.get("probables", []):
                ath = p.get("athlete", {})
                if ath:
                    name = ath.get("displayName", ath.get("fullName", ""))
                    stats = p.get("statistics", [])
                    era = None
                    record = p.get("record", "")
                    for st in stats:
                        if st.get("abbreviation") == "ERA" or st.get("name") == "ERA":
                            try:
                                era = float(st.get("displayValue", 0))
                            except (ValueError, TypeError):
                                era = None
                    return {"name": name, "era": era, "id": str(ath.get("id", "")), "record": record}
            return {"name": "", "era": None, "id": "", "record": ""}

        hp = get_probable(home)
        ap = get_probable(away)

        venue_obj = comp.get("venue", {})
        venue_name = venue_obj.get("fullName", "")
        indoor = 1 if venue_obj.get("indoor", False) else 0

        game = {
            "game_id": str(event.get("id", "")),
            "game_date": game_date,
            "commence_time": event.get("date", ""),
            "away_team": away_team.get("abbreviation", ""),
            "home_team": home_team.get("abbreviation", ""),
            "away_id": str(away_team.get("id", "")),
            "home_id": str(home_team.get("id", "")),
            "away_pitcher": ap["name"],
            "home_pitcher": hp["name"],
            "away_pitcher_era": ap["era"],
            "home_pitcher_era": hp["era"],
            "away_pitcher_record": ap["record"],
            "home_pitcher_record": hp["record"],
            "away_score": int(away.get("score", 0)) if status != "scheduled" else None,
            "home_score": int(home.get("score", 0)) if status != "scheduled" else None,
            "status": status,
            "venue": venue_name,
            "indoor": indoor,
        }
        games.append(game)
    return games


def parse_team_stats(stats_data):
    """Parse ESPN team statistics response into batting/pitching dicts.
    Stores stats keyed by both name AND abbreviation for flexible lookup."""
    result = {"batting": {}, "pitching": {}}
    for cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
        cat_name = cat.get("name", "").lower()
        stats = cat.get("stats", [])
        bucket = {}
        for s in stats:
            val = s.get("value")
            if val is None:
                try:
                    val = float(s.get("displayValue", "").lstrip("."))
                except (ValueError, TypeError):
                    val = s.get("displayValue")
            name = s.get("name", "")
            abbr_key = s.get("abbreviation", "")
            if name:
                bucket[name] = val
            if abbr_key:
                bucket[abbr_key] = val
        if "batting" in cat_name or "hitting" in cat_name:
            result["batting"] = bucket
        elif "pitching" in cat_name:
            result["pitching"] = bucket
    return result


def parse_standings(standings_data):
    """Parse standings into dict keyed by team abbreviation."""
    teams = {}
    for child in standings_data.get("children", []):
        for div in child.get("children", []):
            for entry in div.get("standings", {}).get("entries", []):
                team_obj = entry.get("team", {})
                abbr = team_obj.get("abbreviation", "")
                if not abbr:
                    continue
                stats = {}
                for s in entry.get("stats", []):
                    name = s.get("name", s.get("abbreviation", ""))
                    if name:
                        stats[name] = s.get("value", s.get("displayValue"))
                teams[abbr] = stats
    return teams


# ────────────────────────────────────────────
#  Database writers
# ────────────────────────────────────────────

def save_games(conn, games):
    """Insert/update today's games."""
    for g in games:
        conn.execute("""
            INSERT OR REPLACE INTO todays_games
            (game_id, game_date, commence_time, away_team, home_team,
             away_id, home_id, away_pitcher, home_pitcher,
             away_pitcher_era, home_pitcher_era,
             away_pitcher_record, home_pitcher_record,
             away_score, home_score, status, venue, indoor)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            g["game_id"], g["game_date"], g["commence_time"],
            g["away_team"], g["home_team"], g["away_id"], g["home_id"],
            g["away_pitcher"], g["home_pitcher"],
            g["away_pitcher_era"], g["home_pitcher_era"],
            g["away_pitcher_record"], g["home_pitcher_record"],
            g["away_score"], g["home_score"],
            g["status"], g["venue"], g["indoor"],
        ))
    conn.commit()


def save_team_stats(conn, abbr, team_id, team_name, batting, pitching, standings_row):
    """Insert/update team stats combining ESPN stats + standings."""
    now = datetime.now().isoformat()

    # Parse batting stats
    def bval(key, default=None):
        v = batting.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (ValueError, TypeError):
            return default

    def pval(key, default=None):
        v = pitching.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (ValueError, TypeError):
            return default

    # Standings data
    st = standings_row or {}
    def sval(key, default=None):
        v = st.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (ValueError, TypeError):
            return default

    wins = int(sval("wins", 0))
    losses = int(sval("losses", 0))
    gp = int(sval("gamesPlayed", wins + losses))
    pa = bval("plateAppearances", 0)
    ab = bval("atBats", 0)

    # Calculate K% and BB% from counting stats
    k_bat = bval("strikeouts", 0)
    bb_bat = bval("walks", 0)
    k_pct = (k_bat / pa * 100) if pa and pa > 0 else 22.0
    bb_pct = (bb_bat / pa * 100) if pa and pa > 0 else 8.0

    # Runs per game
    runs_scored = int(sval("pointsFor", 0))
    rpg = runs_scored / gp if gp > 0 else 4.5

    conn.execute("""
        INSERT OR REPLACE INTO team_stats
        (team_abbr, team_id, team_name,
         wins, losses, games_played, win_pct,
         home_wins, home_losses, road_wins, road_losses,
         streak, last_ten,
         runs_scored, runs_allowed, run_diff,
         avg_runs_for, avg_runs_against,
         batting_avg, obp, slg, ops,
         home_runs, runs_per_game,
         strikeouts_bat, walks_bat, k_pct, bb_pct,
         isolated_power, plate_appearances, at_bats,
         era, whip, k_per_9, bb_per_9, hr_per_9,
         opp_avg, opp_obp, opp_slg, opp_ops,
         quality_starts, saves, holds, innings_pitched,
         pitching_strikeouts, pitching_walks,
         park_factor, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        abbr, team_id, team_name,
        wins, losses, gp,
        sval("winPercent", 0.5),
        int(sval("homeWins", 0)), int(sval("homeLosses", 0)),
        int(sval("roadWins", 0)), int(sval("roadLosses", 0)),
        str(st.get("streak", "")),
        str(st.get("lasttengames", "")),
        runs_scored,
        int(sval("pointsAgainst", 0)),
        int(sval("pointDifferential", 0)),
        sval("avgPointsFor", rpg),
        sval("avgPointsAgainst", 4.5),
        bval("avg", bval("AVG", 0.250)),
        bval("onBasePct", bval("OBP", 0.320)),
        bval("slugAvg", bval("SLG", 0.400)),
        bval("OPS", 0.720),
        int(bval("homeRuns", bval("HR", 0))),
        rpg,
        int(k_bat), int(bb_bat),
        round(k_pct, 1), round(bb_pct, 1),
        bval("isolatedPower", bval("ISOP", 0.150)),
        int(pa), int(ab),
        pval("ERA", 4.20),
        pval("WHIP", 1.30),
        pval("strikeoutsPerNineInnings", pval("K/9", 8.5)),
        pval("BB/9", pval("walksPerNineInnings", 3.2)),
        pval("HR/9", pval("homeRunsPerNineInnings", 1.2)),
        pval("opponentAvg", pval("OBA", 0.250)),
        pval("opponentOnBasePct", pval("OOBP", 0.320)),
        pval("opponentSlugAvg", pval("OSLUG", 0.400)),
        pval("opponentOPS", pval("OOPS", 0.720)),
        int(pval("qualityStarts", pval("QS", 0))),
        int(pval("saves", pval("SV", 0))),
        int(pval("holds", pval("HLD", 0))),
        pval("innings", pval("IP", 0)),
        int(pval("strikeouts", pval("K", 0))),
        int(pval("walks", pval("BB", 0))),
        PARK_FACTORS.get(abbr, 1.0),
        now,
    ))
    conn.commit()


def save_recent_games(conn, games):
    """Save recent games for historical tracking."""
    for g in games:
        if g["status"] != "final":
            continue
        conn.execute("""
            INSERT OR IGNORE INTO recent_games
            (game_id, game_date, away_team, home_team,
             away_score, home_score, away_pitcher, home_pitcher, venue)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            g["game_id"], g["game_date"],
            g["away_team"], g["home_team"],
            g["away_score"], g["home_score"],
            g["away_pitcher"], g["home_pitcher"],
            g["venue"],
        ))
    conn.commit()


# ────────────────────────────────────────────
#  Main pipeline
# ────────────────────────────────────────────

def run(target_date=None):
    """Main data collection pipeline."""
    conn = get_db()
    create_tables(conn)

    # 1. Build team map
    print("[1/5] Fetching team list...")
    global TEAM_MAP
    TEAM_MAP = fetch_teams()
    print(f"  Found {len(TEAM_MAP)} teams")

    # 2. Fetch today's games
    today = target_date or datetime.now().strftime("%Y-%m-%d")
    date_param = today.replace("-", "")
    print(f"\n[2/5] Fetching scoreboard for {today}...")
    sb = fetch_scoreboard(date_param)
    games = parse_games(sb, today)
    print(f"  Found {len(games)} games")

    if not games:
        print("  No games today — saving empty slate")
        conn.execute("DELETE FROM todays_games")
        conn.commit()
        conn.close()
        return

    # Clear old games for today
    conn.execute("DELETE FROM todays_games WHERE game_date = ?", (today,))
    save_games(conn, games)

    for g in games:
        pitcher_info = f"{g['away_pitcher'] or 'TBD'} vs {g['home_pitcher'] or 'TBD'}"
        print(f"  {g['away_team']} @ {g['home_team']} — {g['status']} — {pitcher_info}")

    # 3. Fetch standings
    print(f"\n[3/5] Fetching standings...")
    try:
        standings_data = fetch_standings()
        standings = parse_standings(standings_data)
        print(f"  Standings for {len(standings)} teams")
    except Exception as e:
        print(f"  Standings fetch failed: {e}")
        standings = {}

    # 4. Fetch team stats for all teams in today's games
    print(f"\n[4/5] Fetching team statistics...")
    teams_needed = set()
    for g in games:
        teams_needed.add((g["away_team"], g["away_id"]))
        teams_needed.add((g["home_team"], g["home_id"]))

    for abbr, tid in sorted(teams_needed):
        try:
            stats_raw = fetch_team_statistics(tid)
            stats = parse_team_stats(stats_raw)
            team_name = TEAM_MAP.get(tid, {}).get("name", abbr)
            st_row = standings.get(abbr, {})
            save_team_stats(conn, abbr, tid, team_name, stats["batting"], stats["pitching"], st_row)
            print(f"  {abbr}: batting + pitching stats saved")
            time.sleep(0.3)  # Rate limit
        except Exception as e:
            print(f"  {abbr}: ERROR fetching stats — {e}")

    # 5. Fetch recent games (last 7 days) for historical data
    print(f"\n[5/5] Fetching recent games (last 7 days)...")
    recent_count = 0
    for days_ago in range(1, 8):
        d = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
        try:
            sb_hist = fetch_scoreboard(d)
            hist_games = parse_games(sb_hist, d.replace("", ""))
            # Fix date format
            date_fmt = f"{d[:4]}-{d[4:6]}-{d[6:]}"
            for hg in hist_games:
                hg["game_date"] = date_fmt
            save_recent_games(conn, hist_games)
            finals = sum(1 for g in hist_games if g["status"] == "final")
            recent_count += finals
            time.sleep(0.3)
        except Exception as e:
            print(f"  Day {d}: {e}")
    print(f"  Saved {recent_count} completed games")

    conn.close()
    print(f"\nData collection complete. Database: {DB_PATH}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MLB data collection")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)", default=None)
    args = parser.parse_args()
    run(target_date=args.date)
