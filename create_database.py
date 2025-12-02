import sqlite3 as s

con = s.connect('database.db')
cur = con.cursor()

cur.execute("PRAGMA foreign_keys = ON;")

# Teams table
cur.execute("""
CREATE TABLE IF NOT EXISTS Teams (
    team_id      INTEGER PRIMARY KEY,
    team_name    TEXT NOT NULL UNIQUE,
    abbreviation TEXT NOT NULL UNIQUE
);
""")

# Players table
cur.execute("""
CREATE TABLE IF NOT EXISTS Players (
    player_id               INTEGER PRIMARY KEY,
    player_code             TEXT NOT NULL UNIQUE,
    player_name             TEXT NOT NULL,
    team_id                 INTEGER NOT NULL,
    position                TEXT NOT NULL,
    fantasy_price           REAL NOT NULL,
    fantasy_price_change    REAL NOT NULL,
    FOREIGN KEY (team_id)   REFERENCES Teams(team_id)
);
""")

# Games table
cur.execute("""
CREATE TABLE IF NOT EXISTS Games (
    game_id    INTEGER PRIMARY KEY,
    game_date  TEXT NOT NULL,             -- or DATE; SQLite stores as TEXT internally
    home_team  INTEGER NOT NULL,
    away_team  INTEGER NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    FOREIGN KEY (home_team) REFERENCES Teams(team_id),
    FOREIGN KEY (away_team) REFERENCES Teams(team_id)
);
""")

# Boxscore table
cur.execute("""
CREATE TABLE IF NOT EXISTS Boxscore (
    game_id        INTEGER NOT NULL,
    player_id      INTEGER NOT NULL,
    minutes_played REAL NOT NULL,
    pts            INTEGER NOT NULL,
    twofg_made       INTEGER NOT NULL,
    twofg_taken      INTEGER NOT NULL,
    threefg_made       INTEGER NOT NULL,
    threefg_taken      INTEGER NOT NULL,
    ft_made        INTEGER NOT NULL,
    ft_taken       INTEGER NOT NULL,
    oreb           INTEGER NOT NULL,
    dreb           INTEGER NOT NULL,
    ast            INTEGER NOT NULL,
    stl            INTEGER NOT NULL,
    fv_blk         INTEGER NOT NULL,
    ag_blk         INTEGER NOT NULL,
    fouls_cm       INTEGER NOT NULL,
    fouls_rv       INTEGER NOT NULL,
    eff            INTEGER NOT NULL,
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES Games(game_id),
    FOREIGN KEY (player_id) REFERENCES Players(player_id)
);
""")

con.commit()
con.close()
