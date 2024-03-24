import pandas as pd
import numpy as np
import json
import time
import datetime

current_date_time = datetime.datetime.now()
year = current_date_time.date().strftime("%Y")
next_year = int(year) + 1

seasons = range(year, next_year)
zones = range(1, 31)
game_states = range(-2, 3)

gplus_data = pd.DataFrame()

print(f"Grabbing G+ zonal data from ASA...")
for yr in seasons:
    print(f"Grabbing ASA data for {yr}")
    for z in zones:
        for g in game_states:
            time.sleep(3)
            url = f"https://app.americansocceranalysis.com/api/v1/mls/teams/goals-added?zone={z}&season_name={yr}&stage_name=Regular%20Season&gamestate_trunc={g}"
            print(f"{yr} - accessing ASA zone: {z} with gamestate {g}")
            tmp = pd.read_json(url)
            tmp['zone'] = z
            tmp['season_name'] = yr
            tmp['game_state'] = g
            gplus_data = pd.concat([gplus_data, tmp], axis=0, ignore_index=True)

print(f"Found {len(gplus_data)} records of team zone data, exploding to get G+ factors")
json_expl_txt = json.loads(gplus_data.explode('data').to_json(orient="records"))
gplus_json_expl_flat = pd.json_normalize(json_expl_txt)
print(f"Found {len(gplus_json_expl_flat)} records of exploded team zone data, stripping ignored action types")
stripped_expl_flat = gplus_json_expl_flat[~(gplus_json_expl_flat["data.action_type"].isin(["Claiming", "Interrupting"]))]
print(f"Found {len(stripped_expl_flat)} records for valid action types in exploded team zone data, writing to data directory")
stripped_expl_flat.to_csv('./data/team-g+-zones.csv', index=False)
print(f"Wrote {len(stripped_expl_flat)} records of exploded team zone data to data directory")

print(f"slimming columns...")
gplus_expl_flat = stripped_expl_flat[["season_name", "team_id", "minutes", "zone", "game_state", "data.action_type", "data.goals_added_for", "data.goals_added_against"]]
gplus_expl_flat.columns = ["season_name",'team_id', 'minutes', 'zone', "game_state", 'action_type', 'for_total', 'against_total']

print(f"calculating p96 rates for records...")
gplus_expl_flat['for_p96'] = gplus_expl_flat["for_total"] * 96 / gplus_expl_flat["minutes"]
gplus_expl_flat['against_p96'] = gplus_expl_flat["against_total"] * 96 / gplus_expl_flat["minutes"]

print(f"Grouping records by season, team, and zone to calculate pctles...")
grouped_gplus = gplus_expl_flat.groupby(['season_name','team_id','zone', 'game_state']).agg({
    'minutes' : ['mean'],
    'for_total': ['sum'], 
    'against_total': ['sum'], 
    'for_p96': ['sum'], 
    'against_p96': ['sum']
}).reset_index()
grouped_gplus.columns = grouped_gplus.columns.droplevel(level=1)
print(f"Found {len(grouped_gplus)} aggregated group records, calculating net vars")

def find_transpose(season, team, zone, game_state, field):
    print(f"calculating transpose with params: season - {season}, team - {team}, zone - {zone}, game_state - {game_state} and field - {field}")
    result = grouped_gplus[(grouped_gplus.season_name == season) & (grouped_gplus.game_state == game_state) & (grouped_gplus.team_id == team) & (grouped_gplus.zone == zone)][field].tolist()
    if len(result) == 0:
        return 0
    else:
        return result[0]

grouped_gplus['defensive_zone'] = 31 - grouped_gplus.zone
grouped_gplus['def_for_total'] = grouped_gplus.apply(lambda x: find_transpose(x.season_name, x.team_id, x.defensive_zone, x.game_state, 'for_total'), axis=1)
grouped_gplus['def_against_total'] = grouped_gplus.apply(lambda x: find_transpose(x.season_name, x.team_id, x.defensive_zone, x.game_state, 'against_total'), axis=1)
grouped_gplus['def_for_p96'] = grouped_gplus.apply(lambda x: find_transpose(x.season_name, x.team_id, x.defensive_zone, x.game_state, 'for_p96'), axis=1)
grouped_gplus['def_against_p96'] = grouped_gplus.apply(lambda x: find_transpose(x.season_name, x.team_id, x.defensive_zone, x.game_state, 'against_p96'), axis=1)

grouped_gplus['net_p96'] = grouped_gplus['for_p96'] - grouped_gplus['against_p96']
grouped_gplus['net_total'] = grouped_gplus['for_total'] - grouped_gplus['against_total']
grouped_gplus['transposed_net_p96'] = grouped_gplus['for_p96'] - grouped_gplus['def_for_p96']
grouped_gplus['transposed_net_total'] = grouped_gplus['for_total'] - grouped_gplus['def_against_total']

print(f"Calculating percentiles...")
base_range = np.linspace(0.01, 1.00, 100)
years = grouped_gplus["season_name"].unique().tolist()
zones = grouped_gplus["zone"].unique().tolist()
states = grouped_gplus["game_state"].unique().tolist()
percentile_composite = pd.DataFrame()

def percentiles(zone, year, game_state):
    slice_gplus = grouped_gplus[(grouped_gplus.zone == zone) & (grouped_gplus.season_name == year) & (grouped_gplus.game_state == game_state)]
    if (len(slice_gplus) == 0):
        #print(f"no data for Combo of {position} / {action_type}")
        return pd.DataFrame()

    data = slice_gplus["for_total"]
    adj_data = slice_gplus["for_p96"]
    
    ag_data = slice_gplus["against_total"]
    ag_adj_data = slice_gplus["against_p96"]
    
    net_data = slice_gplus["net_total"]
    net_adj_data = slice_gplus["net_p96"]
        
    trans_net_data = slice_gplus["transposed_net_total"]
    trans_net_adj_data = slice_gplus["transposed_net_p96"]
    
    return pd.DataFrame({ 
        "season" : year,
        "zone" : zone, 
        "game_state" : game_state,
        "pct" : base_range,
        "for_p96" : adj_data.quantile(base_range), "for_pSzn" : data.quantile(base_range),
        "against_p96" : ag_adj_data.quantile(base_range), "against_pSzn" : ag_data.quantile(base_range),
        "net_p96" : net_adj_data.quantile(base_range), "net_pSzn" : net_data.quantile(base_range),
        "trans_net_p96" : trans_net_adj_data.quantile(base_range), "trans_net_pSzn" : trans_net_data.quantile(base_range)
    })

for z in zones:
    for y in years:
        for g in states:
            print(f"Calculating percentile for zone {z} with gamestate {g} in year {y}...")
            df = percentiles(z, y, g)
            print(f"adding {len(df)} records to composite")
            percentile_composite = pd.concat([percentile_composite, df], ignore_index=True)

print(f"Generated {len(percentile_composite)} composite zone records, writing to data directory")
percentile_composite.to_csv('./data/percentile-g+-zones.csv', index=False)
print(f"Wrote {len(percentile_composite)} composite zone records to data directory, pull done")
