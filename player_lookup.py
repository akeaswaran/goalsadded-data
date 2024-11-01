import pandas as pd
import numpy as np
import json
import math
import time
import datetime
import os

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

dfs = []
for c in competitions:
    tmp = pd.read_csv(f"./data/{c['competition']}/player-g+-ranks.csv")
    tmp["competition"] = c["competition"]
    dfs.append(tmp)

player_df = pd.concat(dfs, axis=0)
player_df["season_name"] = player_df["season_name"].astype(int)
player_df = player_df[(player_df.team_id != "All")].drop_duplicates(["player_id", "competition", "season_name", "team_id"])
player_df[["competition", "season_name", "team_id", "player_id", "player_name"]].to_csv("./data/player_lookup.csv",index=False)
    