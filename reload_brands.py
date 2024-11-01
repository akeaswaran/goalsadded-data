import pandas as pd
import numpy as np
import json
import time
import datetime

current_date_time = datetime.datetime.now()
year = current_date_time.date().strftime("%Y")
year = int(year)

leagues = ["mls", "nwsl", "uslc", "usls", "mlsnp", "usl1"]
league_teams = []

for l in leagues:
    print(f"load {l} team data for year {year}")
    url = f"https://app.americansocceranalysis.com/api/v1/{l}/teams/goals-added?season_name={year}"
    tmp = pd.read_json(url)

    print(f"found {len(tmp)} teams for year {year}")
    team_id_str = ",".join(tmp["team_id"].tolist())
    league_url = f"https://app.americansocceranalysis.com/api/v1/{l}/teams?team_id={team_id_str}"

    league = pd.read_json(league_url)
    league["competition"] = l
    league_teams.append(league)

league_teams = pd.concat(league_teams, axis = 0)

print(f"found {len(league_teams)} total teams for year {year}")
league_teams \
    .drop(["team_short_name"], axis = 1) \
    .rename({
        "team_id" : "asaId",
        "team_name": "name",
        "team_abbreviation": "abbreviation"
    }, axis = 1) \
    .to_json("./data/brands.json", orient = "records")