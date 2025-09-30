#!/usr/bin/env python3
"""Extract a reduced Ravens win probability dataset from ravens_2024.db.

Outputs:
  ravens_wp.csv  – Tidy CSV with per-play win probability for Ravens games.
  ravens_wp.json – JSON grouped by game for easy front-end consumption.

JSON shape:
{
  "games": [
     {
       "game_id": "2024_01_BAL_KC",
       "season_type": "REG",
       "week": 1,
       "home_team": "KC",
       "away_team": "BAL",
       "result": "L" | "W",
       "final_wp": 0 or 1,
       "points": {"ravens": 27, "opponent": 20},
       "plays": [
           {"t": minutes_elapsed(float), "wp": win_prob(float)}, ... (chronological)
       ]
     }, ...
  ]
}
"""
from __future__ import annotations

import sqlite3
import csv
import json
from pathlib import Path
import argparse

ROOT = Path(__file__).parent
DB_PATH = ROOT / 'ravens_2024.db'
OUT_CSV = ROOT / 'ravens_wp.csv'
OUT_JSON = ROOT / 'ravens_wp.json'
QUERY_FILE = ROOT / 'queries.sql'


def load_extraction_sql() -> str:
    text = QUERY_FILE.read_text(encoding='utf-8')
    # We only want the final SELECT from our added block; simplest is to split on SELECT lines.
    # Instead, just execute entire script wrapped, SQLite will run all statements and return last result.
    return text


def fetch_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    # Instead of brittle script parsing, replicate the extraction query explicitly.
    extraction_sql = """
    WITH base AS (
        SELECT
            game_id,
            home_team,
            away_team,
            season_type,
            week,
            qtr as quarter,
            quarter_seconds_remaining,
            game_seconds_remaining,
            wp,
            home_wp,
            away_wp,
            total_home_score AS home_score,
            total_away_score AS away_score,
            desc as play_desc
        FROM ravens_2024
        WHERE (home_team = 'BAL' OR away_team = 'BAL')
          AND wp IS NOT NULL
          AND game_seconds_remaining IS NOT NULL
    ), enriched AS (
        SELECT
            *,
            ROUND( (3600 - game_seconds_remaining) / 60.0, 3) AS minutes_elapsed,
            ROUND( game_seconds_remaining / 60.0, 3) AS minutes_remaining,
            CASE WHEN home_team = 'BAL' THEN home_wp ELSE away_wp END AS ravens_wp_raw,
            CASE WHEN home_team = 'BAL' THEN home_score ELSE away_score END AS ravens_score,
            CASE WHEN home_team = 'BAL' THEN away_score ELSE home_score END AS opponent_score
        FROM base
    )
    SELECT
        game_id,
        season_type,
        week,
        home_team,
        away_team,
        minutes_elapsed,
        minutes_remaining,
        ravens_wp_raw AS win_prob,
        ravens_score,
        opponent_score,
        play_desc
    FROM enriched
    ORDER BY game_id, minutes_elapsed;
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(extraction_sql).fetchall()
    return rows


def derive_game_results(rows: list[sqlite3.Row]):
    # Determine final WP endpoint (1 for Ravens win else 0) per game.
    # Use final scores from last row of each game.
    by_game: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        by_game.setdefault(r['game_id'], []).append(r)
    games_out = []
    for gid, plays in by_game.items():
        plays_sorted = sorted(plays, key=lambda r: float(r['minutes_elapsed']))
        last = plays_sorted[-1]
        ravens_score = last['ravens_score']
        opp_score = last['opponent_score']
        result = 'W' if ravens_score > opp_score else 'L'
        final_wp = 1 if result == 'W' else 0
        # Convert rows to dicts for mutation
        plays_dicts = [{k: r[k] for k in r.keys()} for r in plays_sorted]
        plays_sorted = plays_dicts
        # Snap final win probability
        plays_sorted[-1]['win_prob'] = float(final_wp)
        # Simple metrics: lead changes (crossings of 0.5) and amplitude (range)
        wps = [float(p['win_prob']) for p in plays_sorted]
        max_wp = max(wps) if wps else 0.0
        min_wp = min(wps) if wps else 0.0
        amplitude = round(max_wp - min_wp, 6)
        lead_changes = 0
        for i in range(1, len(wps)):
            prev_side = wps[i-1] >= 0.5
            curr_side = wps[i] >= 0.5
            if prev_side != curr_side:
                lead_changes += 1
        metrics = {
            'lead_changes': lead_changes,
            'amplitude': amplitude
        }
        games_out.append((gid, plays_sorted, result, final_wp, ravens_score, opp_score, metrics))
    return games_out


def write_csv(rows: list[sqlite3.Row]):
    fieldnames = [
        'game_id','season_type','week','home_team','away_team',
        'minutes_elapsed','minutes_remaining','win_prob',
        'ravens_score','opponent_score','play_desc'
    ]
    with OUT_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r[k] for k in fieldnames})


def write_json(games_out):
    payload = {"games": []}
    # Determine most exciting game: highest lead_changes, tie-break by amplitude
    best_gid = None
    best_tuple = (-1, -1.0)  # (lead_changes, amplitude)
    for gid, plays_sorted, result, final_wp, ravens_score, opp_score, metrics in games_out:
        if not plays_sorted:
            continue
        key = (metrics.get('lead_changes', 0), metrics.get('amplitude', 0.0))
        if key > best_tuple:
            best_tuple = key
            best_gid = gid
    for gid, plays_sorted, result, final_wp, ravens_score, opp_score, metrics in games_out:
        first = plays_sorted[0]
        payload['games'].append({
            'game_id': gid,
            'season_type': first['season_type'],
            'week': first['week'],
            'home_team': first['home_team'],
            'away_team': first['away_team'],
            'result': result,
            'final_wp': final_wp,
            'points': {'ravens': ravens_score, 'opponent': opp_score},
            'metrics': metrics,
            'highlight': gid == best_gid,
            'plays': [
                {'t': float(p['minutes_elapsed']), 'wp': float(p['win_prob'])}
                for p in plays_sorted
            ]
        })
    payload['most_exciting_game_id'] = best_gid
    with OUT_JSON.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', type=Path, default=DB_PATH)
    args = ap.parse_args()
    if not args.db.exists():
        raise SystemError(f"Database not found: {args.db}")
    with sqlite3.connect(args.db) as conn:
        rows = fetch_rows(conn)
    if not rows:
        raise RuntimeError("No rows returned by extraction SQL.")
    write_csv(rows)
    games_out = derive_game_results(rows)
    write_json(games_out)
    print(f"Extracted {len(rows)} rows across {len(games_out)} games -> {OUT_CSV.name}, {OUT_JSON.name}")


if __name__ == '__main__':  # pragma: no cover
    main()
