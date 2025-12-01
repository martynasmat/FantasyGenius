import requests as r
import sqlite3
import json

DB_PATH = "database.db"

resp = r.get('https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E2025/clubs')
clubs = []


def get_teams() -> list[tuple[str, str]]:
    for club in resp.json()['data']:
        match club['tvCode']:
            case 'FBB':
                abbreviation = 'FEN'
            case 'KBA':
                abbreviation = 'BKN'
            case 'VBC':
                abbreviation = 'VAL'
            case 'ZAL':
                abbreviation = 'ŽAL'
            case _:
                abbreviation = club['tvCode']

        name = club['name']
        clubs.append((name, abbreviation))

    return clubs


def update_teams(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    clubs_api = get_teams()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for club in clubs_api:
        abbreviation = club[0]
        name = club[1]

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
    conn.close()


def update_players(conn: sqlite3.Connection, cur: sqlite3.Cursor):
    url = 'https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E2025/people?personType=J&Limit=24&Offset=0&active=true&search=&sortBy=name'
    resp = r.get(url)
    print(json.dumps(resp.json(), indent=4))
    for player in resp.json()['data']:
        player = player['person']
        first_name = player['passportName']
        last_name = player['passportSurname']
        

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    update_teams(conn, cur)
    update_players(conn, cur)
    print("Teams updated.")