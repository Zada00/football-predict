"""
Funksjoner for å bygge features fra rådata om kamper.
Den gylne regelen: en feature for kamp K kan KUN bruke informasjon
fra kamper før K's dato. Ingen fremtidsdata.
"""

from __future__ import annotations
import pandas as pd


def add_team_form_features(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """
    Legg til form-features per lag basert på de siste N kampene.

    For hvert lag, for hver kamp, regner vi:
      - poeng siste N kamper (3 for seier, 1 for uavgjort, 0 for tap)
      - mål scoret siste N kamper
      - mål sluppet inn siste N kamper

    Form regnes uavhengig av om laget spilte hjemme eller borte i de
    forrige kampene – det er det totale formgrunnlaget.

    Forutsetninger:
      - df er sortert kronologisk (vi sorterer for sikkerhets skyld)
      - df inneholder kolonnene: date, league, home_team, away_team,
        home_goals, away_goals, result

    Returnerer en kopi av df med åtte nye kolonner:
      home_form_points, home_form_gf, home_form_ga,
      away_form_points, away_form_gf, away_form_ga,
      home_form_n_games, away_form_n_games

    De to siste angir hvor mange kamper formgjennomsnittet er basert på
    (kan være < N tidlig i sesongen).
    """
    df = df.sort_values("date").reset_index(drop=True).copy()

    # Lag en "lang" tabell der hver rad er én lag-i-én-kamp.
    # Da kan vi enkelt rulle bakover i tid per lag.
    home_rows = pd.DataFrame({
        "match_id": df.index,
        "date": df["date"],
        "league": df["league"],
        "team": df["home_team"],
        "gf": df["home_goals"],
        "ga": df["away_goals"],
        "points": df["result"].map({"H": 3, "D": 1, "A": 0}),
    })
    away_rows = pd.DataFrame({
        "match_id": df.index,
        "date": df["date"],
        "league": df["league"],
        "team": df["away_team"],
        "gf": df["away_goals"],
        "ga": df["home_goals"],
        "points": df["result"].map({"H": 0, "D": 1, "A": 3}),
    })
    long = pd.concat([home_rows, away_rows], ignore_index=True)
    long = long.sort_values(["team", "date"]).reset_index(drop=True)

    # For hver lag-rad, regn rullende sum av forrige N kamper – uten
    # å inkludere DENNE kampen. shift(1) hopper over nåværende kamp.
    grouped = long.groupby("team", sort=False)
    long["form_points"] = grouped["points"].shift(1).rolling(n, min_periods=1).sum().reset_index(drop=True)
    long["form_gf"]     = grouped["gf"].shift(1).rolling(n, min_periods=1).sum().reset_index(drop=True)
    long["form_ga"]     = grouped["ga"].shift(1).rolling(n, min_periods=1).sum().reset_index(drop=True)
    long["form_n"]      = grouped["points"].shift(1).rolling(n, min_periods=1).count().reset_index(drop=True)

    # Merk: ovenstående er litt finurlig fordi rolling etter groupby
    # kan oppføre seg uventet. La oss heller bruke transform-mønsteret,
    # som er trygt:
    def _rolling_sum_excl_current(s: pd.Series) -> pd.Series:
        return s.shift(1).rolling(n, min_periods=1).sum()

    def _rolling_count_excl_current(s: pd.Series) -> pd.Series:
        return s.shift(1).rolling(n, min_periods=1).count()

    long["form_points"] = long.groupby("team", sort=False)["points"].transform(_rolling_sum_excl_current)
    long["form_gf"]     = long.groupby("team", sort=False)["gf"].transform(_rolling_sum_excl_current)
    long["form_ga"]     = long.groupby("team", sort=False)["ga"].transform(_rolling_sum_excl_current)
    long["form_n"]      = long.groupby("team", sort=False)["points"].transform(_rolling_count_excl_current)

    # Plukk hjemme- og bortelagets form ut av long og koble tilbake til df
    home_form = long.loc[long["team"].isin(df["home_team"].unique())].copy()  # alle lag uansett, men trygt

    # Bedre: vi bruker match_id for å koble tilbake. For hver match_id finnes 
    # to rader i long – en for hjemmelaget, en for bortelaget.
    long_home = long.merge(
        df[["home_team"]].reset_index().rename(columns={"index": "match_id", "home_team": "team"}),
        on=["match_id", "team"],
        how="inner",
    )
    long_away = long.merge(
        df[["away_team"]].reset_index().rename(columns={"index": "match_id", "away_team": "team"}),
        on=["match_id", "team"],
        how="inner",
    )

    df["home_form_points"] = long_home.set_index("match_id")["form_points"]
    df["home_form_gf"]     = long_home.set_index("match_id")["form_gf"]
    df["home_form_ga"]     = long_home.set_index("match_id")["form_ga"]
    df["home_form_n"]      = long_home.set_index("match_id")["form_n"]

    df["away_form_points"] = long_away.set_index("match_id")["form_points"]
    df["away_form_gf"]     = long_away.set_index("match_id")["form_gf"]
    df["away_form_ga"]     = long_away.set_index("match_id")["form_ga"]
    df["away_form_n"]      = long_away.set_index("match_id")["form_n"]

    return df


def add_rest_days_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Legger til to kolonner: home_rest_days og away_rest_days.

    For hvert lag, hvor mange dager siden forrige kamp i databasen?
    Hvis det er lagets første kamp i datasettet, settes verdien til NaN.

    Vi bruker hele datasettet (alle ligaer/sesonger), fordi et lag kan
    spille i forskjellige konkurranser. Med kun ligadata her er det i
    praksis bare lag-historikk innen ligaen, men logikken er den samme.
    """
    df = df.sort_values("date").reset_index(drop=True).copy()

    # Bygg samme long-format som før
    home_rows = pd.DataFrame({
        "match_id": df.index,
        "date": df["date"],
        "team": df["home_team"],
    })
    away_rows = pd.DataFrame({
        "match_id": df.index,
        "date": df["date"],
        "team": df["away_team"],
    })
    long = pd.concat([home_rows, away_rows], ignore_index=True)
    long = long.sort_values(["team", "date"]).reset_index(drop=True)

    # For hvert lag, regn ut differansen til forrige kamp
    long["rest_days"] = long.groupby("team", sort=False)["date"].diff().dt.days

    # Koble tilbake til df via match_id
    home_rest = long.merge(
        df[["home_team"]].reset_index().rename(columns={"index": "match_id", "home_team": "team"}),
        on=["match_id", "team"],
        how="inner",
    ).set_index("match_id")["rest_days"]

    away_rest = long.merge(
        df[["away_team"]].reset_index().rename(columns={"index": "match_id", "away_team": "team"}),
        on=["match_id", "team"],
        how="inner",
    ).set_index("match_id")["rest_days"]

    df["home_rest_days"] = home_rest
    df["away_rest_days"] = away_rest

    return df


def add_elo_features(
    df: pd.DataFrame,
    initial_rating: float = 1500.0,
    new_team_rating: float = 1300.0,
    home_advantage: float = 100.0,
    k_factor: float = 20.0,
) -> pd.DataFrame:
    """
    Beregn Elo-rating for hvert lag og legg til to features:
      - home_elo_pre  (hjemmelaget sin rating RETT FØR kampen)
      - away_elo_pre  (bortelaget sin rating RETT FØR kampen)

    Elo holdes separat per liga. Lag som dukker opp første gang får
    `new_team_rating` som startverdi (lav, fordi nyopprykkede lag
    historisk gjør det dårlig).

    Vi går gjennom kamper kronologisk og oppdaterer ratingen etter
    hver kamp. Featurene som lagres er ALLTID rating før kampen,
    aldri etter – så det er ingen lekkasje.
    """
    df = df.sort_values("date").reset_index(drop=True).copy()

    # Ratings holdes som dict per (league, team)
    ratings: dict[tuple[str, str], float] = {}

    home_elo_pre = []
    away_elo_pre = []

    # Kjapt oppslag: hvilke lag har vi sett før per liga? Brukes til 
    # å bestemme startrating. Det første laget får `initial_rating`
    # for at det skal være en "etablert baseline"; etterfølgende nye 
    # lag i samme liga får `new_team_rating`.
    seen_in_league: dict[str, set[str]] = {}

    def get_rating(league: str, team: str) -> float:
        key = (league, team)
        if key in ratings:
            return ratings[key]
        # Nytt lag i denne ligaen
        if league not in seen_in_league:
            seen_in_league[league] = set()
        if not seen_in_league[league]:
            # Aller første lag i ligaen i datasettet – bruk normal start
            r = initial_rating
        else:
            r = new_team_rating
        ratings[key] = r
        seen_in_league[league].add(team)
        return r

    # Iterer over kamper i kronologisk rekkefølge
    for _, row in df.iterrows():
        league = row["league"]
        home = row["home_team"]
        away = row["away_team"]

        r_home = get_rating(league, home)
        r_away = get_rating(league, away)

        # Lagre PRE-rating som feature
        home_elo_pre.append(r_home)
        away_elo_pre.append(r_away)

        # Beregn forventet resultat med hjemmefordel
        exp_home = 1.0 / (1.0 + 10 ** ((r_away - r_home - home_advantage) / 400.0))
        exp_away = 1.0 - exp_home

        # Faktisk score
        result = row["result"]
        if result == "H":
            s_home, s_away = 1.0, 0.0
        elif result == "A":
            s_home, s_away = 0.0, 1.0
        elif result == "D":
            s_home, s_away = 0.5, 0.5
        else:
            # Manglende resultat (f.eks. fremtidig kamp) – ikke oppdater
            continue

        # Oppdater
        ratings[(league, home)] = r_home + k_factor * (s_home - exp_home)
        ratings[(league, away)] = r_away + k_factor * (s_away - exp_away)

    df["home_elo_pre"] = home_elo_pre
    df["away_elo_pre"] = away_elo_pre
    df["elo_diff"] = df["home_elo_pre"] - df["away_elo_pre"]

    return df


def build_basic_features(df: pd.DataFrame, n_form: int = 5) -> pd.DataFrame:
    """Hovedinngang: legg på alle grunn-features."""
    df = add_team_form_features(df, n=n_form)
    df = add_rest_days_features(df)
    df = add_elo_features(df)
    return df