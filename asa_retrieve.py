import pandas as pd
import numpy as np
import json

print(f"Grabbing G+ data from ASA...")
def retrieve_data(split_by_game = False, split_by_seasons = True):
    gplus_data = pd.DataFrame()
    for yr in range(2013, 2023):
        print(f"Grabbing data for field players in {yr} with params: split_by_game = {split_by_game}, split_by_seasons = {split_by_seasons}")
        url = f"https://app.americansocceranalysis.com/api/v1/mls/players/goals-added?season_name={yr}&split_by_teams=true&split_by_seasons={split_by_seasons}&split_by_games={split_by_game}"
        tmp = pd.read_json(url)
        tmp['season'] = yr
        
        print(f"Grabbing data for GKs in {yr} with params: split_by_game = {split_by_game}, split_by_seasons = {split_by_seasons}")
        gk_url = f"https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/goals-added?season_name={yr}&split_by_teams=true&split_by_seasons={split_by_seasons}&split_by_games={split_by_game}"
        tmp_gk = pd.read_json(gk_url)
        tmp_gk["general_position"] = "GK"
        tmp_gk['season'] = yr
        
        gplus_data = gplus_data.append(tmp, ignore_index=True)
        gplus_data = gplus_data.append(tmp_gk, ignore_index=True)
        
    json_gk_expl_txt = json.loads(gplus_data.explode('data').to_json(orient="records"))
    return pd.json_normalize(json_gk_expl_txt)

def percentiles(base, position, action_type, year):
    slice_gplus = base[(base.general_position == position) & (base["data.action_type"] == action_type) & (base.season == year)]
    if (len(slice_gplus) == 0):
        # print(f"no data for Combo of {position} / {action_type}")
        return pd.DataFrame()

    print(f"Compiling data for combo of {position} / {action_type} / {year}") 
    data = slice_gplus["data.goals_added_raw"]
    adj_data = data * 96 / slice_gplus["minutes_played"]
    return pd.DataFrame({ "position" : position, "action_type" : action_type, "season" : year, "pct" : base_range, "p96" : adj_data.quantile(base_range), "pSzn" : data.quantile(base_range)})

print(f"Retriving fresh G+ data from ASA...") 
gplus_expl_flat = retrieve_data(False, True)

print(f"Found {len(gplus_expl_flat)} total rows from ASA, parsing...") 
base_range = np.linspace(0.01, 1.00, 100)
years = gplus_expl_flat["season"].unique().tolist()
print(f"Found {len(years)} unique seasons in data set: {years}")
action_types = gplus_expl_flat["data.action_type"].unique().tolist()
print(f"Found {len(action_types)} unique action types in data set: {action_types}")
positions = gplus_expl_flat["general_position"].unique().tolist()
print(f"Found {len(positions)} unique positions in data set: {positions}")

print(f"Generating seasonal composites...") 
percentile_composite = pd.DataFrame()
for t in action_types:
    for p in positions:
        for y in years:
            df = percentiles(gplus_expl_flat, p, t, y)
            percentile_composite = percentile_composite.append(df, ignore_index=True)

print(f"Saving {len(percentile_composite)} seasonal percentiles to disk...")
percentile_composite.to_csv('./data/season-g+-pct.csv', index=False)
print(f"Generated {len(percentile_composite)} seasonal percentiles and saved to disk.") 

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