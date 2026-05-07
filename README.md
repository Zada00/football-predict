# Football Predict

Prediksjonsmodell for utfall av kamper i de fem store europeiske ligaene
(Premier League, Bundesliga, La Liga, Serie A, Ligue 1), evaluert mot
bookmaker-odds.

## Mål
Bygge en modell hvis sannsynligheter for hjemme/uavgjort/borte er bedre
kalibrert enn markedets på et hold-out sett av kamper.

## Status
Fase 1: Datainnhenting (under arbeid)

## Setup
python -m venv .venv
..venv\Scripts\Activate.ps1
pip install -r requirements.txt

## Datakilde
[football-data.co.uk](https://www.football-data.co.uk/) – historiske kampresultater og bookmaker-odds.