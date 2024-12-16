import pandas as pd
import numpy as np
import json
import math
import time
import datetime
import os

print(f"assembling player season table based on data from ASA...")
team_df = pd.read_csv(f"./data/player_lookup.csv")
team_df["season_name"] = team_df["season_name"].astype(int)
team_df = team_df[(team_df.team_id != "All") & (team_df.player_name != "")].drop_duplicates(["competition", "season_name", "team_id"])
team_df[["competition", "season_name", "team_id"]].to_csv("./data/team_lookup.csv",index=False)
    