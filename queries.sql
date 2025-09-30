-- Simplified Ravens win probability extraction (same columns, no CTE layers)
-- minutes_elapsed: 0 at kickoff up to 60 at end of regulation (overtime not handled separately)
SELECT
  game_id,
  season_type,
  week,
  home_team,
  away_team,
  ROUND( (3600 - game_seconds_remaining) / 60.0, 3) AS minutes_elapsed,
  ROUND( game_seconds_remaining / 60.0, 3) AS minutes_remaining,
  CASE WHEN home_team = 'BAL' THEN home_wp ELSE away_wp END AS win_prob,
  CASE WHEN home_team = 'BAL' THEN total_home_score ELSE total_away_score END AS ravens_score,
  CASE WHEN home_team = 'BAL' THEN total_away_score ELSE total_home_score END AS opponent_score,
  desc AS play_desc
FROM ravens_2024
WHERE (home_team = 'BAL' OR away_team = 'BAL')
  AND wp IS NOT NULL
  AND game_seconds_remaining IS NOT NULL
ORDER BY game_id, minutes_elapsed;

