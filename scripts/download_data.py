"""
Laster ned historiske kampdata fra football-data.co.uk og lagrer
dem som CSV-filer i data/raw/.
Kjøres som: python scripts/download_data.py
"""

from pathlib import Path
import requests
import time

# Hvilke ligaer vi henter, og hva de heter i lesbar form
LEAGUES = {
    "E0": "premier_league",
    "D1": "bundesliga",
    "SP1": "la_liga",
    "I1": "serie_a",
    "F1": "ligue_1",
}

# Sesongkoder. 9495 = 1994/95, 9900 = 1999/00, 0001 = 2000/01, ..., 2526 = 2025/26.
# Vi starter på 1994/95 fordi det er da Premier League ble 20 lag.
def _season_code(start_year: int) -> str:
    """Sesongkoden football-data.co.uk bruker: to siste siffer av start + to siste av slutt."""
    return f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"

SEASONS = [_season_code(y) for y in range(1994, 2026)]

BASE_URL = "https://www.football-data.co.uk/mmz4281"

# Hvor vi lagrer
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download_one(season: str, league_code: str, league_name: str) -> bool:
    """Last ned én CSV. Returnerer True hvis det lyktes, False hvis ikke."""
    url = f"{BASE_URL}/{season}/{league_code}.csv"
    out_path = DATA_DIR / f"{league_name}_{season}.csv"

    if out_path.exists():
        print(f"  [skip] {out_path.name} finnes allerede")
        return True

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            # Sesongen finnes ikke for denne ligaen (typisk veldig gamle år)
            print(f"  [404]  {out_path.name} – finnes ikke på serveren")
            return False
        response.raise_for_status()

        # Skriv til disk (binær mode, så vi ikke roter med encoding)
        out_path.write_bytes(response.content)
        size_kb = len(response.content) / 1024
        print(f"  [ok]   {out_path.name} ({size_kb:.0f} KB)")
        return True
    except requests.RequestException as e:
        print(f"  [feil] {out_path.name}: {e}")
        return False


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Lagrer data i: {DATA_DIR}")
    print(f"Henter {len(LEAGUES)} ligaer × {len(SEASONS)} sesonger = {len(LEAGUES) * len(SEASONS)} filer\n")

    successes = 0
    failures = 0

    for league_code, league_name in LEAGUES.items():
        print(f"\n=== {league_name.upper()} ===")
        for season in SEASONS:
            ok = download_one(season, league_code, league_name)
            if ok:
                successes += 1
            else:
                failures += 1
            time.sleep(0.3)  # vær snill med serveren

    print(f"\n=== Ferdig: {successes} ok, {failures} feil/manglende ===")


if __name__ == "__main__":
    main()