import pandas as pd
import numpy as np
import json
import math
import time
import datetime
import os

def query_player_data(competition, player_ids):
    player_data = []
    chunks = np.array_split(player_ids, 50)
    for i in range(0, len(chunks)):
        time.sleep(1.25)
        chunk = chunks[i]
        id_list = ','.join(chunk)
        print(f"Grabbing player data using ids chunk {i} of {len(chunks)}...")

        tmp = pd.read_json(f"https://app.americansocceranalysis.com/api/v1/{competition}/players/xgoals?player_id={id_list}&split_by_teams=true&split_by_seasons=true")
        if len(tmp) > 0:
            tmp["competition"] = competition
            player_data.append(tmp[["player_id", "competition", "season_name", "team_id"]])

        tmp = pd.read_json(f"https://app.americansocceranalysis.com/api/v1/{competition}/goalkeepers/xgoals?player_id={id_list}&split_by_teams=true&split_by_seasons=true")
        if len(tmp) > 0:
            tmp["competition"] = competition
            player_data.append(tmp[["player_id", "competition", "season_name", "team_id"]])
    
    return pd.concat(player_data, axis=0)

current_date_time = datetime.datetime.now()
year = current_date_time.date().strftime("%Y")
year = int(year)
next_year = int(year) + 1

competitions = [
    {
        "competition": "mls",
        "start_year": 2013,
        "end_year": next_year
    },
    {
        "competition": "nwsl",
        "start_year": 2013,
        "end_year": next_year
    },
    {
        "competition": "usls",
        "start_year": 2024,
        "end_year": next_year
    },
    {
        "competition": "uslc",
        "start_year": 2017,
        "end_year": next_year
    },
    {
        "competition": "usl1",
        "start_year": 2019,
        "end_year": next_year
    },
    {
        "competition": "mlsnp",
        "start_year": 2022,
        "end_year": next_year
    }
]

big_dfs = []
for c in competitions:
    dfs = []
    competition = c["competition"]
    print(f"Retrieving player season data for {competition} from file system...")
    tmp = pd.read_csv(f"./data/{competition}/player-g+-ranks.csv")
    tmp["competition"] = competition
    dfs.append(tmp[["player_id", "competition", "season_name", "team_id"]])

    # handle players left out of ranks because of minutes restrictions or other nonsense
    player_lookup = pd.read_csv(f"./data/{competition}/player_lookup.csv")
    leftovers = player_lookup[~(player_lookup.player_id.isin(tmp.player_id))]
    if len(leftovers) > 0:
        print(f"Found {len(leftovers)} leftover players in the lookup table for {competition}")

        remaining = query_player_data(competition=competition, player_ids=leftovers.player_id.tolist())
        dfs.append(remaining)
    
    b_df = pd.concat(dfs, axis = 0)
    b_df = pd.merge(b_df, player_lookup, on = "player_id")
    big_dfs.append(b_df)

print(f"assembling player season table based on data from ASA...")
player_df = pd.concat(big_dfs, axis=0)
player_df["season_name"] = player_df["season_name"].astype(int)
player_df = player_df[(player_df.team_id != "All")].drop_duplicates(["player_id", "competition", "season_name", "team_id"])
player_df[["competition", "season_name", "team_id", "player_id", "player_name"]].to_csv("./data/player_lookup.csv",index=False)
    