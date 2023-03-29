import pandas as pd
import numpy as np
import json
import math

print(f"Grabbing G+ data from ASA...")
def retrieve_data(split_by_game = False, split_by_seasons = True):
    gplus_data = pd.DataFrame()
    for yr in range(2013, 2024):
        print(f"Grabbing data for field players in {yr} with params: split_by_game = {split_by_game}, split_by_seasons = {split_by_seasons}")
        url = f"https://app.americansocceranalysis.com/api/v1/mls/players/goals-added?stage_name=Regular%20Season&season_name={yr}&split_by_teams=true&split_by_seasons={split_by_seasons}&split_by_games={split_by_game}"
        tmp = pd.read_json(url)
        tmp['season'] = yr
        
        print(f"Grabbing data for GKs in {yr} with params: split_by_game = {split_by_game}, split_by_seasons = {split_by_seasons}")
        gk_url = f"https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/goals-added?stage_name=Regular%20Season&season_name={yr}&split_by_teams=true&split_by_seasons={split_by_seasons}&split_by_games={split_by_game}"
        tmp_gk = pd.read_json(gk_url)
        tmp_gk["general_position"] = "GK"
        tmp_gk['season'] = yr
        
        gplus_data = gplus_data.append(tmp, ignore_index=True)
        gplus_data = gplus_data.append(tmp_gk, ignore_index=True)
        
    json_gk_expl_txt = json.loads(gplus_data.explode('data').to_json(orient="records"))
    return pd.json_normalize(json_gk_expl_txt)

def action_percentiles(base, position, action_type, year):
    slice_gplus = base[(base.general_position == position) & (base["data.action_type"] == action_type) & (base.season == year)]
    if (len(slice_gplus) == 0):
        # print(f"no data for Combo of {position} / {action_type}")
        return pd.DataFrame()

    print(f"Compiling data for combo of {position} / {action_type} / {year}") 
    data = slice_gplus["data.goals_added_raw"]
    adj_data = slice_gplus["data.goals_added_raw_p96"]
    return pd.DataFrame({ "position" : position, "action_type" : action_type, "season" : year, "pct" : base_range, "p96" : adj_data.quantile(base_range), "pSzn" : data.quantile(base_range)})

def total_percentiles(base, position, year):
    slice_gplus = base[(base.general_position == position) & (base.season == year)]
    grouped_slice = slice_gplus.groupby(['season_name','player_id']).agg({
        'data.goals_added_raw': ['sum'], 
        'minutes_played' : ['mean']
    }).reset_index()
    grouped_slice.columns = grouped_slice.columns.droplevel(level=1)
    grouped_slice['total'] = grouped_slice['data.goals_added_raw']
    grouped_slice['p96'] = grouped_slice['data.goals_added_raw'] * 96 / grouped_slice["minutes_played"]

    if (len(grouped_slice) == 0):
        # print(f"no data for Combo of {position} / {action_type}")
        return pd.DataFrame()

    print(f"Compiling data for combo of {position} / {year}") 
    data = grouped_slice["total"]
    adj_data = grouped_slice["p96"]
    return pd.DataFrame({ "position" : position, "season" : year, "pct" : base_range, "p96" : adj_data.quantile(base_range), "pSzn" : data.quantile(base_range)})

def rank_players(base, year, team: str = None, position: str = None, action_type: str = None):
    print(f"Ranking players for combo of {year} - {position} - {action_type}")

    slice_gplus = base[(base.season == year)]
    max_minutes = slice_gplus["minutes_played"].max()
    print(f"Found max minutes in season: {max_minutes}")
    max_games = math.floor(max_minutes / 96)
    print(f"Found max games played in season: {max_games}")
    
    rank_threshold_gm = max_games * 0.25
    rank_threshold_min = rank_threshold_gm * 96
    print(f"leaderboard threshold by games is {rank_threshold_gm}, by minutes is {rank_threshold_min}")

    slice_gplus = slice_gplus[slice_gplus.minutes_played >= rank_threshold_min]

    if (team != None):
        slice_gplus = slice_gplus[(slice_gplus.team_id == team)]

    if (position != None):
        slice_gplus = slice_gplus[(slice_gplus.general_position == position)]

    if (action_type != None):
        slice_gplus = slice_gplus[(slice_gplus["data.action_type"] == action_type)]

    grouped_slice = slice_gplus.groupby(['season_name','player_id']).agg({
        'data.goals_added_raw': ['sum'], 
        'minutes_played' : ['mean']
    }).reset_index()
    grouped_slice.columns = grouped_slice.columns.droplevel(level=1)
    grouped_slice['total'] = grouped_slice['data.goals_added_raw']
    grouped_slice['total_rank'] = grouped_slice['total'].rank(ascending = False)
    grouped_slice['p96'] = grouped_slice['data.goals_added_raw'] * 96 / grouped_slice["minutes_played"]
    grouped_slice['p96_rank'] = grouped_slice['p96'].rank(ascending = False)

    grouped_slice['team_id'] = team if (team != None) else 'All'
    grouped_slice['position'] = position if (position != None) else 'All'
    grouped_slice['action_type'] = action_type if (action_type != None) else 'All'
    if (len(slice_gplus) == 0):
        # print(f"no data for Combo of {position} / {action_type}")
        return [
            pd.DataFrame(),
            pd.DataFrame()
        ]

    return [
        grouped_slice.sort_values(by=['total_rank'], ascending=True).head(10),
        grouped_slice.sort_values(by=['p96_rank'], ascending=True).head(10)
    ]
    
print(f"Retriving fresh G+ data from ASA...") 
gplus_expl_flat = retrieve_data(False, True)
gplus_expl_flat["data.goals_added_raw_p96"] =  gplus_expl_flat["data.goals_added_raw"] * 96 / gplus_expl_flat["minutes_played"]

print(f"Found {len(gplus_expl_flat)} total rows from ASA, parsing...") 
base_range = np.linspace(0.01, 1.00, 100)
years = gplus_expl_flat["season"].unique().tolist()
print(f"Found {len(years)} unique seasons in data set: {years}")
action_types = gplus_expl_flat["data.action_type"].unique().tolist()
print(f"Found {len(action_types)} unique action types in data set: {action_types}")
positions = gplus_expl_flat["general_position"].unique().tolist()
print(f"Found {len(positions)} unique positions in data set: {positions}")
teams = gplus_expl_flat["team_id"].unique().tolist()
print(f"Found {len(teams)} unique teams in data set: {teams}")

print(f"Generating seasonal composites...") 
percentile_composite = pd.DataFrame()
for t in action_types:
    for p in positions:
        for y in years:
            df = action_percentiles(gplus_expl_flat, p, t, y)
            percentile_composite = percentile_composite.append(df, ignore_index=True)

print(f"Saving {len(percentile_composite)} seasonal action percentiles to disk...")
percentile_composite.to_csv('./data/season-g+-pct.csv', index=False)
print(f"Generated {len(percentile_composite)} seasonal action percentiles for {len(action_types)} action types, saved to disk.") 

player_composite = pd.DataFrame()
for p in positions:
    for y in years:
        df = total_percentiles(gplus_expl_flat, p, y)
        player_composite = player_composite.append(df, ignore_index=True)
print(f"Saving {len(player_composite)} seasonal player percentiles to disk...")
player_composite.to_csv('./data/player-g+-pct.csv', index=False)
print(f"Generated {len(player_composite)} seasonal player percentiles, saved to disk.") 

print(f"Grabbing players for look-up table...") 
player_list = gplus_expl_flat[gplus_expl_flat.player_id.notna() == True].player_id.unique().tolist()
print(f"Found {len(player_list)} players in G+ dataset, parsing...")

player_data = pd.DataFrame()
chunks = np.array_split(player_list, 50)
for i in range(0, len(chunks)):
    chunk = chunks[i]
    id_list = ','.join(chunk)
    print(f"Grabbing player data using ids chunk {i} of {len(chunks)}...")
    url = f"https://app.americansocceranalysis.com/api/v1/mls/players?player_id={id_list}"
    tmp = pd.read_json(url)
    player_data = player_data.append(tmp, ignore_index=True)

print(f"Found {len(player_data)} ASA player records, dropping dupes...")
player_data.drop_duplicates('player_id',inplace=True)
player_data.sort_values(by='player_name', inplace=True)
print(f"Found {len(player_data)} unique ASA player records, slimming and saving to disk...")
slim_set = player_data[['player_id', 'player_name']]
slim_set.to_csv('./data/player_lookup.csv',index=False)
print(f"Saved lookup table of {len(player_data)} player records to disk.")

player_ranks_total = pd.DataFrame()
player_ranks_p96 = pd.DataFrame()

no_gk_rank_df = gplus_expl_flat[gplus_expl_flat.general_position != 'GK']

for year in years:
    # rank within league WITHOUT GK
    [df_total, df_p96] = rank_players(no_gk_rank_df, year, None, None, None)
    player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
    player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)

    # rank within position WITH GK
    for pos in positions:
        [df_total, df_p96] = rank_players(gplus_expl_flat, year, None, pos, None)
        player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
        player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)

    # rank in action type
    for action_type in action_types:
        [df_total, df_p96] = rank_players(gplus_expl_flat, year, None, None, action_type)
        player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
        player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)
        # rank in action_type at position
        for pos in positions:
            [df_total, df_p96] = rank_players(gplus_expl_flat, year, None, pos, action_type)
            player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
            player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)

    # for each team
    for team in teams:
        # rank within action type WITH GK
        for action_type in action_types:
            [df_total, df_p96] = rank_players(gplus_expl_flat, year, team, None, action_type)
            player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
            player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)

            # rank in action_type at position
            for pos in positions:
                [df_total, df_p96] = rank_players(gplus_expl_flat, year, team, pos, action_type)
                player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
                player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)

        # rank within position WITH GK
        for pos in positions:
            [df_total, df_p96] = rank_players(gplus_expl_flat, year, team, pos, None)
            player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
            player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)
        # rank within team WITHOUT GK
        [df_total, df_p96] = rank_players(no_gk_rank_df, year, team, None, None)
        player_ranks_total = player_ranks_total.append(df_total, ignore_index=True)
        player_ranks_p96 = player_ranks_p96.append(df_p96, ignore_index=True)


player_ranks_p96['rank_type'] = 'p96'
player_ranks_total['rank_type'] = 'total'
rank_composite = player_ranks_p96.append(player_ranks_total)

print(rank_composite.dtypes)
print(slim_set.dtypes)

slim_set.player_id = slim_set.player_id.astype(str)
rank_composite.player_id = rank_composite.player_id.astype(str)

named_composite = rank_composite.merge(slim_set, on=["player_id"])

print(f"Saving {len(named_composite)} player ranks to disk...")
named_composite.to_csv('./data/player-g+-ranks.csv', index=False)
print(f"Generated {len(named_composite)} player ranks, saved to disk.") 


print(f"Starting to process {len(gplus_expl_flat)} records for team roster breakdowns...")
grouped_gplus = gplus_expl_flat.groupby(['season_name','team_id','player_id','general_position']).agg({
    'data.goals_added_raw': ['sum'], 
    'minutes_played' : ['mean']
}).reset_index()
grouped_gplus.columns = grouped_gplus.columns.droplevel(level=1)
grouped_gplus['total'] = grouped_gplus['data.goals_added_raw']
grouped_gplus['p96'] = grouped_gplus['data.goals_added_raw'] * 96 / grouped_gplus["minutes_played"]
grouped_gplus = grouped_gplus[['season_name', 'team_id', 'player_id','general_position', 'minutes_played','total', 'p96']]
print(f"Found {len(grouped_gplus)} unique season/team/player/position groups...")

print(f"Organizing players by season/team/position...")
team_breakdown_gplus = grouped_gplus.groupby(['season_name','team_id','general_position']).apply(
        lambda x: pd.Series([
            np.mean(x['total']),
            np.mean(x['p96']),
            np.average(x['p96'], weights=(x['minutes_played'] / sum(x['minutes_played'])))
        ], index=['total_avg', 'p96_avg', 'p96_weighted_avg'])
    ).reset_index()
print(f"Calculated G+ averages for {len(team_breakdown_gplus)} season/team/position")
print(f"Calculating G+ ranks for {len(team_breakdown_gplus)} season/team/positions...")
team_breakdown_gplus['total_avg_rank'] = team_breakdown_gplus.groupby(['season_name','general_position'])['total_avg'].rank(ascending=False)
team_breakdown_gplus['p96_avg_rank'] = team_breakdown_gplus.groupby(['season_name','general_position'])['p96_avg'].rank(ascending=False)
team_breakdown_gplus['p96_weighted_avg_rank'] = team_breakdown_gplus.groupby(['season_name','general_position'])['p96_weighted_avg'].rank(ascending=False)
print(f"Calculated G+ ranks for {len(team_breakdown_gplus)} season/team/positions.")
print(f"Writing team roster breakdown records to disk...")
team_breakdown_gplus.to_csv('./data/team_position_breakdown.csv', index=False)
print(f"Wrote {len(team_breakdown_gplus)} team roster breakdown records to disk.")