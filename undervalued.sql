WITH
future_games AS (
  SELECT game_id, game_date, home_team, away_team
  FROM Games
  WHERE game_date IN ('2025-12-04','2025-12-05')
),
player_pool AS (
  SELECT
    fg.game_date,
    fg.game_id,
    p.player_id,
    p.player_name,
    p.position,
    p.team_id,
    CASE WHEN p.team_id = fg.home_team THEN fg.away_team ELSE fg.home_team END AS opp_team_id,
    p.fantasy_price,
    p.fantasy_price_change
  FROM future_games fg
  JOIN Players p
    ON p.team_id IN (fg.home_team, fg.away_team)
),
hist_ext AS (
  SELECT
    g.game_date,
    b.player_id,
    pl.position,
    pl.team_id,
    CASE WHEN pl.team_id = g.home_team THEN g.away_team ELSE g.home_team END AS opp_team_id,
    b.minutes_played,
    b.eff,
    b.ft_taken,
    b.fouls_rv,
    b.fouls_cm
  FROM Boxscore b
  JOIN Games g ON g.game_id = b.game_id
  JOIN Players pl ON pl.player_id = b.player_id
),
hist_ranked AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (PARTITION BY h.player_id ORDER BY h.game_date DESC) AS rn
  FROM hist_ext h
),
recent_hist AS (
  SELECT * FROM hist_ranked WHERE rn <= 8
),
player_model AS (
  SELECT
    rh.player_id,
    COUNT(*) AS games_used,
    AVG(rh.minutes_played) AS avg_minutes,
    AVG(rh.eff * 1.0 / NULLIF(rh.minutes_played,0))  AS eff_per_min,
    AVG(rh.fouls_rv * 1.0 / NULLIF(rh.minutes_played,0)) AS fouls_rv_per_min,
    AVG(rh.fouls_cm * 1.0 / NULLIF(rh.minutes_played,0)) AS fouls_cm_per_min
  FROM recent_hist rh
  GROUP BY rh.player_id
),
league_pos AS (
  SELECT
    position,
    AVG(eff * 1.0 / NULLIF(minutes_played,0)) AS league_eff_per_min_pos
  FROM hist_ext
  GROUP BY position
),
opp_pos_allowed AS (
  SELECT
    opp_team_id,
    position,
    AVG(eff * 1.0 / NULLIF(minutes_played,0)) AS eff_per_min_allowed
  FROM hist_ext
  GROUP BY opp_team_id, position
),
opp_pos_adj AS (
  SELECT
    o.opp_team_id,
    o.position,
    (o.eff_per_min_allowed - lp.league_eff_per_min_pos) AS matchup_eff_adj
  FROM opp_pos_allowed o
  JOIN league_pos lp ON lp.position = o.position
),
base_preds AS (
  SELECT
    pp.game_date,
    pp.game_id,
    pp.player_id,
    pp.player_name,
    pp.position,
    t.team_name  AS team,
    ot.team_name AS opponent,
    pp.fantasy_price,
    pp.fantasy_price_change,
    pm.games_used,
    pm.avg_minutes,
    pm.eff_per_min,
    pm.fouls_rv_per_min,
    pm.fouls_cm_per_min,
    COALESCE(opa.matchup_eff_adj, 0.0) AS matchup_eff_adj,

    (pm.eff_per_min + 0.6 * COALESCE(opa.matchup_eff_adj, 0.0) + 0.15 * pm.fouls_rv_per_min) AS pred_eff_per_min,

    MAX(0.6, 1.0 - 0.08 * pm.fouls_cm_per_min * pm.avg_minutes) AS foul_min_factor

  FROM player_pool pp
  JOIN player_model pm ON pm.player_id = pp.player_id
  JOIN Teams t  ON t.team_id  = pp.team_id
  JOIN Teams ot ON ot.team_id = pp.opp_team_id
  LEFT JOIN opp_pos_adj opa
    ON opa.opp_team_id = pp.opp_team_id
   AND opa.position   = pp.position
  WHERE pm.games_used >= 3
)

SELECT
  game_date,
  game_id,
  player_name,
  team,
  opponent,
  position,
  fantasy_price,
  ROUND(avg_minutes, 1) AS avg_minutes,
  ROUND(pred_eff_per_min, 3) AS pred_eff_per_min,
  ROUND(avg_minutes * foul_min_factor, 1) AS pred_minutes,

  -- keep the raw predicted eff too
  (pred_eff_per_min * avg_minutes * foul_min_factor) AS pred_eff_raw,
  ROUND((pred_eff_per_min * avg_minutes * foul_min_factor), 2) AS pred_eff,

  -- FORCE REAL division + show 6 decimals so it doesn't print as 0
  printf('%.6f',
    (1.0 * (pred_eff_per_min * avg_minutes * foul_min_factor))
    / NULLIF(1.0 * CAST(fantasy_price AS REAL), 0.0)
  ) AS value_score

FROM base_preds
WHERE CAST(fantasy_price AS REAL) > 0
ORDER BY CAST(value_score AS REAL) DESC
LIMIT 50;
