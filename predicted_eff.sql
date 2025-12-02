WITH
/* =========================
   1) CONFIG: edit these
   ========================= */
params(eval_date) AS (
  VALUES
    ('2025-11-25'),
    ('2025-11-26'),
    ('2025-12-04'),
    ('2025-12-05')
),

/* games we are evaluating (backtest + future prediction) */
games_on_date AS (
  SELECT
    p.eval_date,
    g.game_id,
    g.game_date,
    g.home_team,
    g.away_team
  FROM params p
  JOIN Games  g
    ON g.game_date = p.eval_date
),

/* all players who will appear in those games (candidate pool) */
player_pool AS (
  SELECT
    god.eval_date,
    god.game_id,
    pl.player_id,
    pl.player_name,
    pl.position,
    pl.team_id,
    CASE WHEN pl.team_id = god.home_team THEN god.away_team ELSE god.home_team END AS opp_team_id,
    CASE WHEN pl.team_id = god.home_team THEN 1 ELSE 0 END AS is_home
  FROM games_on_date god
  JOIN Players pl
    ON pl.team_id IN (god.home_team, god.away_team)
),

/* ==========================================
   2) HISTORICAL DATA SNAPSHOT "as of" eval_date
   (this is what makes backtesting consistent)
   ========================================== */
hist_all AS (
  SELECT
    p.eval_date,
    g.game_id,
    g.game_date,
    b.player_id,
    pl.team_id,
    pl.position,
    /* opponent team for this player in that historical game */
    CASE WHEN pl.team_id = g.home_team THEN g.away_team ELSE g.home_team END AS opp_team_id,
    CASE WHEN pl.team_id = g.home_team THEN 1 ELSE 0 END AS is_home,

    b.minutes_played,
    b.eff,
    b.pts,
    b.twofg_made, b.twofg_taken,
    b.threefg_made, b.threefg_taken,
    b.ft_made, b.ft_taken,
    b.oreb, b.dreb,
    b.ast, b.stl,
    b.fv_blk, b.ag_blk,
    b.fouls_cm, b.fouls_rv
  FROM params p
  JOIN Games g
    ON g.game_date < p.eval_date          -- key: only data BEFORE eval_date
  JOIN Boxscore b
    ON b.game_id = g.game_id
  JOIN Players pl
    ON pl.player_id = b.player_id
),

/* For player-recent form we only need history for players in the pool */
hist_pool_players AS (
  SELECT ha.*
  FROM hist_all ha
  JOIN player_pool pp
    ON pp.eval_date = ha.eval_date
   AND pp.player_id = ha.player_id
),

/* last-N games per (eval_date, player) */
ranked_recent AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY eval_date, player_id
      ORDER BY game_date DESC
    ) AS rn
  FROM hist_pool_players
),

/* weighted recent averages (tune weights however you like) */
player_recent AS (
  SELECT
    eval_date,
    player_id,
    COUNT(*) AS games_used,

    /* minutes */
    SUM(minutes_played * w) / NULLIF(SUM(w), 0) AS avg_min,

    /* baseline performance */
    SUM((eff * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS eff_per_min,

    /* foul drawing + FT profile */
    SUM((ft_taken * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS fta_per_min,
    SUM(ft_made * w) / NULLIF(SUM(ft_taken * w), 0)                          AS ft_pct,
    SUM((fouls_rv * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS fouls_rv_per_min,

    /* foul trouble */
    SUM((fouls_cm * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS fouls_cm_per_min,

    /* extra signals (optional but useful) */
    SUM(((oreb + dreb) * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS reb_per_min,
    SUM((ast * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0)          AS ast_per_min,
    SUM((stl * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0)          AS stl_per_min,
    SUM(((fv_blk + ag_blk) * 1.0 / NULLIF(minutes_played,0)) * w) / NULLIF(SUM(w), 0) AS blk_per_min

  FROM (
    SELECT
      eval_date, player_id,
      minutes_played, eff, ft_taken, ft_made, fouls_rv, fouls_cm,
      oreb, dreb, ast, stl, fv_blk, ag_blk,
      CASE rn
        WHEN 1 THEN 1.00
        WHEN 2 THEN 0.90
        WHEN 3 THEN 0.80
        WHEN 4 THEN 0.70
        WHEN 5 THEN 0.60
        WHEN 6 THEN 0.50
        WHEN 7 THEN 0.40
        WHEN 8 THEN 0.30
        ELSE 0.20
      END AS w
    FROM ranked_recent
    WHERE rn <= 10
  )
  GROUP BY eval_date, player_id
),

/* ==========================================
   3) OPPONENT vs POSITION tendencies
   ========================================== */
league_pos AS (
  SELECT
    eval_date,
    position,
    AVG(eff * 1.0 / NULLIF(minutes_played,0))      AS league_eff_per_min,
    AVG(ft_taken * 1.0 / NULLIF(minutes_played,0)) AS league_fta_per_min,
    AVG(fouls_rv * 1.0 / NULLIF(minutes_played,0)) AS league_fouls_rv_per_min,
    AVG(fouls_cm * 1.0 / NULLIF(minutes_played,0)) AS league_fouls_cm_per_min
  FROM hist_all
  GROUP BY eval_date, position
),

/* "allowed" = what players (of a position) do when facing that defense team */
opp_pos_allowed AS (
  SELECT
    eval_date,
    opp_team_id AS def_team_id,
    position,
    AVG(eff * 1.0 / NULLIF(minutes_played,0))      AS eff_per_min_allowed,
    AVG(ft_taken * 1.0 / NULLIF(minutes_played,0)) AS fta_per_min_allowed,
    AVG(fouls_rv * 1.0 / NULLIF(minutes_played,0)) AS fouls_rv_per_min_allowed,
    AVG(fouls_cm * 1.0 / NULLIF(minutes_played,0)) AS fouls_cm_per_min_forced
  FROM hist_all
  GROUP BY eval_date, opp_team_id, position
),

opp_pos_adj AS (
  SELECT
    o.eval_date,
    o.def_team_id,
    o.position,
    (o.eff_per_min_allowed      - l.league_eff_per_min)      AS eff_adj,
    (o.fta_per_min_allowed      - l.league_fta_per_min)      AS fta_adj,
    (o.fouls_rv_per_min_allowed - l.league_fouls_rv_per_min) AS fouls_rv_adj,
    (o.fouls_cm_per_min_forced  - l.league_fouls_cm_per_min) AS fouls_cm_forced_adj
  FROM opp_pos_allowed o
  JOIN league_pos l
    ON l.eval_date = o.eval_date
   AND l.position = o.position
),

/* ==========================================
   4) HOME/AWAY split (small adjustment)
   ========================================== */
player_home_away AS (
  SELECT
    eval_date,
    player_id,
    AVG(CASE WHEN is_home=1 THEN eff*1.0/NULLIF(minutes_played,0) END) AS home_eff_per_min,
    AVG(CASE WHEN is_home=0 THEN eff*1.0/NULLIF(minutes_played,0) END) AS away_eff_per_min
  FROM hist_pool_players
  GROUP BY eval_date, player_id
),

/* ==========================================
   5) PREDICTION
   ========================================== */
predictions AS (
  SELECT
    pp.eval_date,
    pp.game_id,
    pp.player_id,
    pp.player_name,
    pp.position,
    pp.team_id,
    t.team_name  AS team,
    ot.team_name AS opponent,
    pp.is_home,

    pr.games_used,
    pr.avg_min AS base_proj_min,
    pr.eff_per_min AS base_eff_per_min,

    COALESCE(opa.eff_adj, 0.0)        AS matchup_eff_adj,
    COALESCE(opa.fta_adj, 0.0)        AS matchup_fta_adj,
    COALESCE(opa.fouls_rv_adj, 0.0)   AS matchup_fouls_rv_adj,
    COALESCE(opa.fouls_cm_forced_adj,0.0) AS matchup_fouls_cm_forced_adj,

    /* home/away tiny nudge */
    COALESCE(
      CASE
        WHEN pha.home_eff_per_min IS NOT NULL AND pha.away_eff_per_min IS NOT NULL THEN
          CASE WHEN pp.is_home=1
               THEN 0.35 * (pha.home_eff_per_min - pha.away_eff_per_min)
               ELSE 0.35 * (pha.away_eff_per_min - pha.home_eff_per_min)
          END
        ELSE 0.0
      END
    ,0.0) AS home_adj,

    /* expected extra FT contribution from matchup (very rough, but effective) */
    (
      COALESCE(pr.ft_pct, 0.75)
      * (COALESCE(opa.fta_adj,0.0))          -- opponent gives more/less FTAs to this position
      * 0.9                                  -- scaling into "eff-space"
    ) AS matchup_ft_eff_per_min_bonus,

    /* foul trouble minutes penalty (reduce minutes if high foul rate + opponent draws fouls) */
    MAX(
      0.65,
      1.0
      - 0.10 * (pr.fouls_cm_per_min * pr.avg_min)
      - 0.05 * (COALESCE(opa.fouls_cm_forced_adj,0.0) * pr.avg_min)
    ) AS foul_min_factor,

    /* predicted eff per minute */
    (
      pr.eff_per_min
      + 0.60 * COALESCE(opa.eff_adj, 0.0)
      + 0.12 * COALESCE(opa.fouls_rv_adj, 0.0)
      + (
          COALESCE(pr.ft_pct, 0.75)
          * COALESCE(opa.fta_adj,0.0)
          * 0.9
        )
      + COALESCE(
          CASE
            WHEN pha.home_eff_per_min IS NOT NULL AND pha.away_eff_per_min IS NOT NULL THEN
              CASE WHEN pp.is_home=1
                   THEN 0.35 * (pha.home_eff_per_min - pha.away_eff_per_min)
                   ELSE 0.35 * (pha.away_eff_per_min - pha.home_eff_per_min)
              END
            ELSE 0.0
          END
        ,0.0
      )
    ) AS pred_eff_per_min,

    /* predicted total eff */
    (
      (
        pr.eff_per_min
        + 0.60 * COALESCE(opa.eff_adj, 0.0)
        + 0.12 * COALESCE(opa.fouls_rv_adj, 0.0)
        + (
            COALESCE(pr.ft_pct, 0.75)
            * COALESCE(opa.fta_adj,0.0)
            * 0.9
          )
        + COALESCE(
            CASE
              WHEN pha.home_eff_per_min IS NOT NULL AND pha.away_eff_per_min IS NOT NULL THEN
                CASE WHEN pp.is_home=1
                     THEN 0.35 * (pha.home_eff_per_min - pha.away_eff_per_min)
                     ELSE 0.35 * (pha.away_eff_per_min - pha.home_eff_per_min)
                END
              ELSE 0.0
            END
          ,0.0
        )
      )
      * (pr.avg_min * MAX(
          0.65,
          1.0
          - 0.10 * (pr.fouls_cm_per_min * pr.avg_min)
          - 0.05 * (COALESCE(opa.fouls_cm_forced_adj,0.0) * pr.avg_min)
        )
      )
    ) AS pred_eff

  FROM player_pool pp
  JOIN Teams t  ON t.team_id  = pp.team_id
  JOIN Teams ot ON ot.team_id = pp.opp_team_id

  LEFT JOIN player_recent pr
    ON pr.eval_date  = pp.eval_date
   AND pr.player_id  = pp.player_id

  LEFT JOIN opp_pos_adj opa
    ON opa.eval_date   = pp.eval_date
   AND opa.def_team_id = pp.opp_team_id
   AND opa.position    = pp.position

  LEFT JOIN player_home_away pha
    ON pha.eval_date = pp.eval_date
   AND pha.player_id = pp.player_id

  /* minimum history requirement (tune this) */
  WHERE pr.games_used >= 3
),

/* actuals exist only for backtest dates that already have boxscores */
actuals AS (
  SELECT
    p.eval_date,
    g.game_id,
    b.player_id,
    b.eff AS actual_eff
  FROM params p
  JOIN Games g
    ON g.game_date = p.eval_date
  JOIN Boxscore b
    ON b.game_id = g.game_id
),

scored AS (
  SELECT
    prd.*,
    act.actual_eff,
    (act.actual_eff - prd.pred_eff) AS error_eff,
    ROW_NUMBER() OVER (
      PARTITION BY prd.eval_date, prd.game_id
      ORDER BY prd.pred_eff DESC
    ) AS pred_rank,
    ROW_NUMBER() OVER (
      PARTITION BY prd.eval_date, prd.game_id
      ORDER BY act.actual_eff DESC
    ) AS actual_rank
  FROM predictions prd
  LEFT JOIN actuals act
    ON act.eval_date = prd.eval_date
   AND act.game_id   = prd.game_id
   AND act.player_id = prd.player_id
)

/* =========================
   OUTPUT:
   - for 12-04 and 12-05 you’ll get predictions (actual_eff will be NULL)
   - for 11-25 and 11-26 you’ll ALSO get actual_eff + error
   ========================= */
SELECT
  eval_date,
  game_id,
  pred_rank,
  player_name,
  team,
  opponent,
  position,
  ROUND(base_proj_min, 1) AS base_proj_min,
  ROUND(pred_eff, 2)      AS pred_eff,
  ROUND(pred_eff_per_min, 3) AS pred_eff_per_min,
  ROUND(matchup_eff_adj, 3)  AS matchup_eff_adj,
  ROUND(matchup_fta_adj, 3)  AS matchup_fta_adj,
  ROUND(matchup_fouls_rv_adj, 3) AS matchup_fouls_rv_adj,
  ROUND(foul_min_factor, 3)  AS foul_min_factor,
  actual_eff,
  ROUND(error_eff, 2) AS error_eff
FROM scored
WHERE eval_date IN ('2025-12-04','2025-12-05')   -- change to ('2025-11-25','2025-11-26') for backtest view
ORDER BY eval_date, game_id, pred_rank
LIMIT 200;