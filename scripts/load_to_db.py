"""
Leser alle CSV-ene i data/raw/ og laster dem inn i data/football.db.
Kjøres som: python scripts/load_to_db.py
"""

from pathlib import Path
import sqlite3
import pandas as pd

# Mappestier
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "data" / "football.db"

# Mapping fra CSV-kolonnenavn (football-data.co.uk) til våre interne navn.
# Kolonner som ikke står her ignoreres.
COLUMN_MAP = {
    "Date": "date",
    "HomeTeam": "home_team",
    "AwayTeam": "away_team",
    "FTHG": "home_goals",          # Full Time Home Goals
    "FTAG": "away_goals",          # Full Time Away Goals
    "FTR": "result",               # Full Time Result: H/D/A
    "HTHG": "ht_home_goals",       # Half Time Home Goals
    "HTAG": "ht_away_goals",
    "HS": "home_shots",
    "AS": "away_shots",
    "HST": "home_shots_target",
    "AST": "away_shots_target",
    "HC": "home_corners",
    "AC": "away_corners",
    "HY": "home_yellow",
    "AY": "away_yellow",
    "HR": "home_red",
    "AR": "away_red",
    "B365H": "b365_home_odds",
    "B365D": "b365_draw_odds",
    "B365A": "b365_away_odds",
    "AvgH": "avg_home_odds",       # Gjennomsnitt over bookmakere (nyere sesonger)
    "AvgD": "avg_draw_odds",
    "AvgA": "avg_away_odds",
    "BbAvH": "avg_home_odds",      # Eldre navn for samme – BetBrain Avg
    "BbAvD": "avg_draw_odds",
    "BbAvA": "avg_away_odds",
}

# Endelig kolonneliste i databasen, i samme rekkefølge som CREATE TABLE
DB_COLUMNS = [
    "league", "season", "date",
    "home_team", "away_team",
    "home_goals", "away_goals", "result",
    "ht_home_goals", "ht_away_goals",
    "home_shots", "away_shots",
    "home_shots_target", "away_shots_target",
    "home_corners", "away_corners",
    "home_yellow", "away_yellow",
    "home_red", "away_red",
    "b365_home_odds", "b365_draw_odds", "b365_away_odds",
    "avg_home_odds", "avg_draw_odds", "avg_away_odds",
    "source_file",
]


def parse_filename(filename: str) -> tuple[str, str]:
    """
    'premier_league_2425.csv' -> ('premier_league', '2024-25')
    """
    stem = Path(filename).stem  # 'premier_league_2425'
    parts = stem.rsplit("_", 1)  # ['premier_league', '2425']
    league = parts[0]
    season_code = parts[1]  # '2425'
    start = int(season_code[:2])
    end = int(season_code[2:])
    # 94 -> 1994, 99 -> 1999, 00 -> 2000, 25 -> 2025
    start_year = 1900 + start if start >= 90 else 2000 + start
    end_year = 1900 + end if end >= 90 else 2000 + end
    season = f"{start_year}-{str(end_year)[-2:]}"
    return league, season


def load_csv(path: Path) -> pd.DataFrame:
    """Les én CSV og returner en DataFrame med våre standardkolonner."""
    league, season = parse_filename(path.name)

    # Les CSV. Bruker 'latin-1' fordi gamle filer har spesialtegn (lagsnavn)
    # som ikke alltid er gyldig UTF-8.
    df = pd.read_csv(path, encoding="latin-1", on_bad_lines="skip")

    # Behold bare kolonnene vi kjenner igjen, og rename til våre navn
    keep = {csv_col: our_col for csv_col, our_col in COLUMN_MAP.items() if csv_col in df.columns}
    df = df[list(keep.keys())].rename(columns=keep)

    # Slipp rader uten dato eller resultat – det er typisk tomme rader på slutten
    df = df.dropna(subset=["date", "home_team", "away_team", "result"])

    # Parse dato. Football-data bruker enten DD/MM/YY eller DD/MM/YYYY – pandas
    # håndterer begge med dayfirst=True. Rader som ikke parser droppes.
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["date"])

    # Legg til metadata
    df["league"] = league
    df["season"] = season
    df["source_file"] = path.name

    # Sørg for at alle DB_COLUMNS finnes (manglende = NULL)
    for col in DB_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[DB_COLUMNS]


def create_database(conn: sqlite3.Connection) -> None:
    """Opprett matches-tabellen hvis den ikke finnes."""
    conn.executescript("""
        DROP TABLE IF EXISTS matches;

        CREATE TABLE matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league TEXT NOT NULL,
            season TEXT NOT NULL,
            date TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_goals INTEGER,
            away_goals INTEGER,
            result TEXT,
            ht_home_goals INTEGER,
            ht_away_goals INTEGER,
            home_shots INTEGER,
            away_shots INTEGER,
            home_shots_target INTEGER,
            away_shots_target INTEGER,
            home_corners INTEGER,
            away_corners INTEGER,
            home_yellow INTEGER,
            away_yellow INTEGER,
            home_red INTEGER,
            away_red INTEGER,
            b365_home_odds REAL,
            b365_draw_odds REAL,
            b365_away_odds REAL,
            avg_home_odds REAL,
            avg_draw_odds REAL,
            avg_away_odds REAL,
            source_file TEXT
        );

        CREATE INDEX idx_matches_date ON matches(date);
        CREATE INDEX idx_matches_league_season ON matches(league, season);
        CREATE INDEX idx_matches_teams ON matches(home_team, away_team);
    """)


def main():
    if not RAW_DIR.exists():
        raise SystemExit(f"Fant ikke {RAW_DIR}. Kjør først: python scripts/download_data.py")

    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"Ingen CSV-filer i {RAW_DIR}. Kjør først: python scripts/download_data.py")

    print(f"Fant {len(csv_files)} CSV-filer å laste inn i {DB_PATH}")

    # Slett gammel db hvis den finnes – vi bygger fra bunn hver gang
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    create_database(conn)

    total_rows = 0
    failed_files = []

    for path in csv_files:
        try:
            df = load_csv(path)
            df.to_sql("matches", conn, if_exists="append", index=False)
            print(f"  [ok]   {path.name}: {len(df)} kamper")
            total_rows += len(df)
        except Exception as e:
            print(f"  [feil] {path.name}: {e}")
            failed_files.append(path.name)

    conn.commit()
    conn.close()

    print(f"\n=== Ferdig: {total_rows} kamper i databasen ===")
    if failed_files:
        print(f"Feilet på {len(failed_files)} filer: {failed_files}")


if __name__ == "__main__":
    main()