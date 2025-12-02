from unittest import case

import requests as r
import sqlite3
import json
from collections import defaultdict
from rapidfuzz import fuzz
from unidecode import unidecode
import re

DB_PATH = "database.db"

resp = r.get('https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E2025/clubs')
clubs = []
FANTASY_QUERY = """
query playersSearchRecordsFromClient($locale: String, $leagueId: String!, $fantasyRound: Int, $position: String, $teamId: String, $search: String, $teamGamesCurrentRound: Boolean, $pointCalcSystem: String) {
  playersSearchRecordsFromClient(
    leagueId: $leagueId
    position: $position
    teamId: $teamId
    search: $search
    fantasyRound: $fantasyRound
  ) {
    records {
      ...partialPlayerWithStats
      __typename
    }
    __typename
  }
}

fragment partialPlayerWithStats on Player {
  id
  basketnewsApiPlayerId
  firstName
  middleName
  lastName
  health
  photo
  fantasyRound(fantasyRound: $fantasyRound)
  team(leagueId: $leagueId, fantasyRound: $fantasyRound) {
    id
    teamId
    status
    number
    positions
    team {
      id
      logo
      jersey
      jerseyWhite
      abbreviation
      translation(locale: $locale) {
        name
        shortName
        __typename
      }
      games(fantasyRound: $fantasyRound, currentRound: $teamGamesCurrentRound) {
        ...partialGame
        __typename
      }
      __typename
    }
    __typename
  }
  fantasyPrice(leagueId: $leagueId, fantasyRound: $fantasyRound)
  fantasyPriceChange(leagueId: $leagueId, fantasyRound: $fantasyRound)
  previousFantasyPriceChange(leagueId: $leagueId, fantasyRound: $fantasyRound)
  stats(leagueId: $leagueId, fantasyRound: $fantasyRound) {
    ...partialSumPlayerStats
    __typename
  }
  fantasy_pts(
    leagueId: $leagueId
    pointCalcSystem: $pointCalcSystem
    fantasyRound: $fantasyRound
  )
  previous_fantasy_pts(
    leagueId: $leagueId
    pointCalcSystem: $pointCalcSystem
    fantasyRound: $fantasyRound
  )
  average_fantasy_pts: fantasy_pts(
    leagueId: $leagueId
    pointCalcSystem: $pointCalcSystem
  )
  popularity(leagueId: $leagueId, fantasyRound: $fantasyRound) {
    ownedRatio
    __typename
  }
  __typename
}

fragment partialGame on Game {
  id
  basketnewsApiGameId
  originalGameAt
  team1 {
    points
    team {
      id
      logo
      abbreviation
      translation(locale: $locale) {
        name
        shortName
        __typename
      }
      __typename
    }
    __typename
  }
  team2 {
    points
    team {
      id
      logo
      abbreviation
      translation(locale: $locale) {
        name
        shortName
        __typename
      }
      __typename
    }
    __typename
  }
  live
  completed
  __typename
}

fragment partialSumPlayerStats on SumPlayerStats {
  leagueId
  s_time
  s_pts
  s_orb
  s_drb
  s_ast
  s_2pm
  s_2pa
  s_3pm
  s_3pa
  s_ftm
  s_fta
  s_stl
  s_tov
  s_blk
  s_rbs
  s_pf
  s_rf
  s_eff
  s_pm
  s_gp
  s_gw
  __typename
}
"""


def load_players_by_team(cur: sqlite3.Cursor) -> dict[str, list[dict[str, str]]]:
    cur.execute("""
        SELECT p.player_id, p.player_name, t.abbreviation
        FROM Players p
        JOIN Teams t ON p.team_id = t.team_id
    """)
    players_by_team: dict[str, list[dict[str, str]]] = {}

    for player_id, player_name, team_abbr in cur.fetchall():
        norm_name = normalize_name(player_name)
        last_norm = extract_last(player_name)
        players_by_team.setdefault(team_abbr, []).append(
            {
                "player_id": player_id,
                "name": player_name,
                "norm_name": norm_name,
                "last_norm": last_norm,
            }
        )

    return players_by_team


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unidecode(name)        # removes accents (Ąžuolas -> Azuolas)
    name = name.upper()
    name = re.sub(r"[^A-Z\s]", "", name)   # keep only letters + spaces
    name = re.sub(r"\s+", " ", name).strip()
    return name


def extract_last(name: str) -> str:
    n = normalize_name(name)
    parts = n.split()
    return parts[-1] if parts else ""


def debug_candidates(fantasy_full_norm, fantasy_last_norm, candidates):
    rows = []
    for cand in candidates:
        cand_full = cand["norm_name"]
        cand_last = cand["last_norm"]

        score_last = fuzz.ratio(fantasy_last_norm, cand_last)
        score_full = fuzz.token_sort_ratio(fantasy_full_norm, cand_full)

        combined = 0.7 * score_last + 0.3 * score_full

        rows.append({
            "db_name": cand["name"],
            "score_last": score_last,
            "score_full": score_full,
            "combined": combined,
        })

    rows.sort(key=lambda x: x["combined"], reverse=True)
    return rows


def find_best_match(full_name_raw, team_abbr, players_by_team, threshold=65.5, debug=True):
    fantasy_full_norm = normalize_name(full_name_raw)
    fantasy_last_norm = extract_last(full_name_raw)

    candidates = players_by_team.get(team_abbr, [])
    if not candidates:
        return None, -1

    best = None
    best_score = -1

    for cand in candidates:
        cand_full = cand["norm_name"]
        cand_last = cand["last_norm"]

        score_last = fuzz.ratio(fantasy_last_norm, cand_last)
        score_full = fuzz.token_sort_ratio(fantasy_full_norm, cand_full)
        combined = 0.7 * score_last + 0.3 * score_full

        if combined > best_score:
            best_score = combined
            best = cand

    # If low score & debug enabled → show full candidate list
    if debug and best_score < threshold:
        print("\n⚠️  LOW SCORE MATCH ATTEMPT")
        print(f"Fantasy name: {full_name_raw}")
        print(f"Team: {team_abbr}")
        print(f"Best score: {best_score:.1f}")
        print("\nCandidates:")
        ranked = debug_candidates(fantasy_full_norm, fantasy_last_norm, candidates)
        for r in ranked:
            print(f"  - {r['db_name']:25s} | combined={r['combined']:.1f} | last={r['score_last']:.1f} | full={r['score_full']:.1f}")

    if best_score >= threshold:
        return best, best_score

    return None, best_score


def update_fantasy_prices(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    players_by_team = load_players_by_team(cur)
    data = get_fantasy_data()['data']['playersSearchRecordsFromClient']['records']
    unmatched = []

    for player in data:
        first_name = player['firstName'] or ""
        middle_name = player['middleName'] or ""
        last_name = player['lastName'] or ""
        team_abbr = player['team']['team']['abbreviation']
        price = player['fantasyPrice']
        price_change = player['fantasyPriceChange']

        full_name = unidecode(" ".join([first_name, last_name]).strip().upper())

        # enable debug = True to show candidate lists
        match, score = find_best_match(full_name, team_abbr, players_by_team, threshold=85, debug=True)

        if match is None:
            unmatched.append((full_name, team_abbr, score))
            continue

        print(f"MATCHED: {full_name} -> {match['name']} (score={score:.1f})")

        cur.execute("""
            UPDATE Players SET fantasy_price = ?, fantasy_price_change = ? WHERE player_id = ?
        """, (price, price_change, match["player_id"]))

    conn.commit()

    if unmatched:
        print("\n=== UNMATCHED PLAYERS ===")
        for name, team, score in unmatched:
            print(f"- {name} ({team}) score={score:.1f}")



def get_fantasy_data() -> dict:
    url = 'https://fantasy.basketnews.com/backend/graphql'
    variables = {
        "locale": "lt",
        "leagueId": "68c43c76151aef80fbfdb894",
        "fantasyRound": None,
        "position": None,
        "teamId": None,
        "search": None,
        "teamGamesCurrentRound": None,
        "pointCalcSystem": None
    }
    headers = {
        "Content-Type": "application/json",
        # Add auth header if required:
        # "Authorization": "Bearer <TOKEN>"
    }
    response = r.post(url, json={"query": FANTASY_QUERY, "variables": variables}, headers=headers)
    return response.json()


def convert_abbr(abbr: str) -> str:
    match abbr:
        case 'FBB':
            return 'FEN'
        case 'KBA':
            return 'BKN'
        case 'VBC':
            return 'VAL'
        case 'ZAL':
            return 'ŽAL'
        case _:
            return abbr


def get_teams() -> list[tuple[str, str]]:
    for club in resp.json()['data']:
        abbreviation = convert_abbr(club['tvCode'])

        name = club['name']
        clubs.append((name, abbreviation))

    return clubs


def update_teams(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    clubs_api = get_teams()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for club in clubs_api:
        name = club[0]
        abbreviation = club[1]

        print("Syncing:", abbreviation, name)

        cur.execute(
            """
            INSERT INTO Teams (team_name, abbreviation)
            VALUES (?, ?)
            ON CONFLICT(abbreviation) DO UPDATE
                SET team_name = excluded.team_name;
            """,
            (name, abbreviation),
        )

    conn.commit()


def update_players(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    url = 'https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E2025/people?personType=J&Limit=1000&Offset=0&active=true&search=&sortBy=name'
    resp = r.get(url)
    for player in resp.json()['data']:
        person_data = player['person']
        code = person_data['code']
        name = person_data['name'].split(', ')
        first_name, last_name = name[1], name[0]
        team_name = player['club']['name']
        team_abbr = convert_abbr(player['club']['tvCode'])
        position = player['positionName']
        url_detailed = f'https://www.euroleaguebasketball.net/_next/data/7KEJm6i-JCDbt9MDHCi3O/en/euroleague/players/{first_name.lower()}-{last_name.lower()}/{code}.json'
        resp_detailed = r.get(url_detailed)

        cur.execute("SELECT team_id FROM Teams WHERE abbreviation = ?", (team_abbr,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Team not found in Teams table: {team_abbr} ({team_name})")
        team_id = row[0]

        cur.execute(
            """
            INSERT INTO Players (player_code, player_name, team_id, position, fantasy_price, fantasy_price_change)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_code) DO UPDATE SET team_id  = excluded.team_id,
                                                   position = excluded.position
            """,
            (code, f'{first_name} {last_name}', team_id, position, 0.0, 0.0),
        )

        cur.execute("SELECT player_id FROM Players WHERE player_code = ?", (code,))
        player_id = cur.fetchone()[0]

        try:
            game_stats = resp_detailed.json()['pageProps']['data']['stats']['currentSeason']['gameStats'][0]['table']['sections']
            stat_dict = defaultdict(dict)
            for i in range(0, 6):
                for j in range(0, len(game_stats[i]['stats']) - 2):
                    match i:
                        case 0:
                            # Minutes / Points / 2FG / 3FG / Free throws
                            stat_dict[j]['min'] = game_stats[i]['stats'][j]['statSets'][0]['value']
                            stat_dict[j]['pts'] = game_stats[i]['stats'][j]['statSets'][1]['value']
                            stat_dict[j]['2fg_made'], stat_dict[j]['2fg_taken'] = game_stats[i]['stats'][j]['statSets'][2]['value'].split('/')[0], game_stats[i]['stats'][j]['statSets'][2]['value'].split('/')[1]
                            stat_dict[j]['3fg_made'], stat_dict[j]['3fg_taken'] = game_stats[i]['stats'][j]['statSets'][3]['value'].split('/')[0], game_stats[i]['stats'][j]['statSets'][3]['value'].split('/')[1]
                            stat_dict[j]['ft_made'], stat_dict[j]['ft_taken'] = game_stats[i]['stats'][j]['statSets'][4]['value'].split('/')[0], game_stats[i]['stats'][j]['statSets'][4]['value'].split('/')[1]
                        case 1:
                            # Offensive rebounds / Defensive rebounds / Total rebounds
                            stat_dict[j]['oreb'] = game_stats[i]['stats'][j]['statSets'][0]['value']
                            stat_dict[j]['dreb'] = game_stats[i]['stats'][j]['statSets'][1]['value']
                            stat_dict[j]['treb'] = game_stats[i]['stats'][j]['statSets'][2]['value']
                        case 2:
                            # ???
                            stat_dict[j]['as'] = game_stats[i]['stats'][j]['statSets'][0]['value']
                            stat_dict[j]['st'] = game_stats[i]['stats'][j]['statSets'][1]['value']
                            stat_dict[j]['to'] = game_stats[i]['stats'][j]['statSets'][2]['value']
                        case 3:
                            # Blocks
                            stat_dict[j]['fv'] = game_stats[i]['stats'][j]['statSets'][0]['value']
                            stat_dict[j]['ag'] = game_stats[i]['stats'][j]['statSets'][1]['value']
                        case 4:
                            # Fouls
                            stat_dict[j]['cm'] = game_stats[i]['stats'][j]['statSets'][0]['value']
                            stat_dict[j]['rv'] = game_stats[i]['stats'][j]['statSets'][1]['value']
                        case 5:
                            # Efficiency
                            stat_dict[j]['eff'] = game_stats[i]['stats'][j]['statSets'][0]['value']

            opp_names = resp_detailed.json()['pageProps']['data']['stats']['currentSeason']['gameStats'][0]['table']['headSection']['stats']
            for i in range(0, len(opp_names)):
                if i == len(opp_names) - 1 or i == len(opp_names) - 2:
                    stat_dict[i]['opp'] = opp_names[i]['statSets'][0]['value']
                else:
                    stat_dict[i]['opp'] = convert_abbr(opp_names[i]['statSets'][1]['value'])
                    stat_dict[i]['type'] = 'home' if opp_names[i]['statSets'][1]['statType'] == 'vsType' else 'away'

            update_boxscore(stat_dict, player_id, team_id)
            print("Updated stats for player:", first_name, last_name)
            conn.commit()

        except TypeError:
            print("No stats found for player:", first_name, last_name)


def update_boxscore(stats: defaultdict, pid: int, tid: int) -> None:
    for idx, stats in stats.items():
        # skip rows that don't have stats (just in case)
        if 'pts' not in stats:
            continue

        # 1) find opponent team_id from abbreviation
        opp_abbr = stats['opp']
        cur.execute("SELECT team_id FROM Teams WHERE abbreviation = ?", (opp_abbr,))
        row = cur.fetchone()
        if row is None:
            print("Opponent not found in Teams:", opp_abbr)
            continue
        opp_team_id = row[0]

        # 2) decide who is home/away from 'type'
        if stats.get('type') == 'home':
            home_team_id = tid  # player's team is home
            away_team_id = opp_team_id
        else:
            home_team_id = opp_team_id  # player's team is away
            away_team_id = tid

        # 3) find the game_id from Games table
        cur.execute(
            """
            SELECT game_id
            FROM Games
            WHERE home_team = ?
              AND away_team = ?
            """,
            (home_team_id, away_team_id),
        )
        row = cur.fetchone()
        if row is None:
            print("Game not found in Games for teams:", home_team_id, away_team_id)
            continue
        game_id = row[0]

        # 4) prepare boxscore values
        minutes = parse_minutes(stats.get('min', "0"))
        pts = int(stats.get('pts', 0) or 0)
        twofg_made = int(stats.get('2fg_made', 0) or 0)
        twofg_taken = int(stats.get('2fg_taken', 0) or 0)
        threefg_made = int(stats.get('3fg_made', 0) or 0)
        threefg_taken = int(stats.get('3fg_taken', 0) or 0)
        ft_made = int(stats.get('ft_made', 0) or 0)
        ft_taken = int(stats.get('ft_taken', 0) or 0)
        oreb = int(stats.get('oreb', 0) or 0)
        dreb = int(stats.get('dreb', 0) or 0)
        ast = int(stats.get('as', 0) or 0)
        stl = int(stats.get('st', 0) or 0)
        fv_blk = int(stats.get('fv', 0) or 0)
        ag_blk = int(stats.get('ag', 0) or 0)
        fouls_cm = int(stats.get('cm', 0) or 0)
        fouls_rv = int(stats.get('rv', 0) or 0)
        eff = int(stats.get('eff', 0) or 0)

        # 5) insert / update Boxscore
        cur.execute(
            """
            INSERT INTO Boxscore
                (
                    game_id,
                    player_id,
                    minutes_played,
                    pts,
                    twofg_made,
                    twofg_taken,
                    threefg_made,
                    threefg_taken,
                    ft_made,
                    ft_taken,
                    oreb,
                    dreb,
                    ast,
                    stl,
                    fv_blk,
                    ag_blk,
                    fouls_cm,
                    fouls_rv,
                    eff
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id, player_id)
                DO UPDATE SET minutes_played = excluded.minutes_played,
                              pts            = excluded.pts,
                              twofg_made     = excluded.twofg_made,
                              twofg_taken    = excluded.twofg_taken,
                              threefg_made   = excluded.threefg_made,
                              threefg_taken  = excluded.threefg_taken,
                              ft_made        = excluded.ft_made,
                              ft_taken       = excluded.ft_taken,
                              oreb           = excluded.oreb,
                              dreb           = excluded.dreb,
                              ast            = excluded.ast,
                              stl            = excluded.stl,
                              fv_blk         = excluded.fv_blk,
                              ag_blk         = excluded.ag_blk,
                              fouls_cm       = excluded.fouls_cm,
                              fouls_rv       = excluded.fouls_rv,
                              eff            = excluded.eff;
            """,
            (game_id, pid, minutes, pts, twofg_made, twofg_taken, threefg_made, threefg_taken,
             ft_made, ft_taken, oreb, dreb, ast, stl, fv_blk, ag_blk, fouls_cm, fouls_rv, eff),
        )


def parse_minutes(min_str: str) -> float:
    # Handles "MM:SS" or just "MM"
    if not min_str:
        return 0.0
    parts = min_str.split(":")
    if len(parts) == 1:
        return float(parts[0])
    minutes = int(parts[0])
    seconds = int(parts[1])
    return minutes + seconds / 60.0


def update_games(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    for i in range(1, 39):
        url = f'https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E2025/games?teamCode=&phaseTypeCode=RS&roundNumber={i}'
        resp = r.get(url)
        for game in resp.json()['data']:
            home_team = game['home']['name']
            away_team = game['away']['name']
            home_score = game['home']['score']
            away_score = game['away']['score']
            date = game['date'].split('T')[0]

            cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (home_team,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Home team not found in Teams table: {home_team}")
            home_team_id = row[0]

            cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (away_team,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Away team not found in Teams table: {away_team}")
            away_team_id = row[0]

            cur.execute(
                """
                SELECT game_id, home_score, away_score
                FROM Games
                WHERE game_date = ?
                  AND home_team = ?
                  AND away_team = ?
                """,
                (date, home_team_id, away_team_id),
            )
            existing = cur.fetchone()

            if existing is None:
                cur.execute(
                    """
                    INSERT INTO Games (game_date, home_team, away_team, home_score, away_score)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (date, home_team_id, away_team_id, home_score, away_score),
                )
                print(f"Inserted game: {date} {home_team} {home_score} - {away_score} {away_team}")
            else:
                game_id, old_home_score, old_away_score = existing
                if old_home_score != home_score or old_away_score != away_score:
                    cur.execute(
                        """
                        UPDATE Games
                        SET home_score = ?,
                            away_score = ?
                        WHERE game_id = ?
                        """,
                        (home_score, away_score, game_id),
                    )
                    print(
                        f"Updated score for game {game_id}: {old_home_score}-{old_away_score} -> {home_score}-{away_score}")
            conn.commit()

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    update_teams(conn, cur)
    update_games(conn, cur)
    update_players(conn, cur)
    update_fantasy_prices(conn, cur)
    print("\nALL STATISTICS UPDATED.")
    conn.close()