"""Sanity-sjekk av databasen."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "football.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("Totalt antall kamper:")
print(" ", cur.execute("SELECT COUNT(*) FROM matches").fetchone()[0])

print("\nKamper per liga:")
for row in cur.execute("""
    SELECT league, COUNT(*) FROM matches GROUP BY league ORDER BY league
"""):
    print(f"  {row[0]:<20} {row[1]}")

print("\nÅrsspenn (eldste og nyeste kamp):")
for row in cur.execute("SELECT MIN(date), MAX(date) FROM matches"):
    print(f"  {row[0]}  ->  {row[1]}")

print("\nFordeling av resultater:")
for row in cur.execute("""
    SELECT result, COUNT(*),
           ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM matches WHERE result IS NOT NULL), 1)
    FROM matches WHERE result IS NOT NULL GROUP BY result
"""):
    print(f"  {row[0]}: {row[1]} ({row[2]}%)")

print("\nTopp 5 lag etter antall kamper i databasen:")
for row in cur.execute("""
    SELECT team, SUM(c) FROM (
        SELECT home_team AS team, COUNT(*) AS c FROM matches GROUP BY home_team
        UNION ALL
        SELECT away_team AS team, COUNT(*) AS c FROM matches GROUP BY away_team
    ) GROUP BY team ORDER BY SUM(c) DESC LIMIT 5
"""):
    print(f"  {row[0]:<20} {row[1]}")

conn.close()