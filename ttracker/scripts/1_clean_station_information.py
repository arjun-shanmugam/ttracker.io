import pandas as pd
import json
import gtfs_kit
from pathlib import Path
import math

GREEN_LINE_STATION_COORDINATES = "../../data/raw/spider_green_line.json"
GREEN_LINE_STATION_NAMES = "../../data/raw/station-network_green_line.json"
NON_GREEN_LINE_STATION_COORDINATES = "../../data/raw/spider.json"
NON_GREEN_LINE_STATION_NAMES = "../../data/raw/station-network.json"
MBTA_GTFS = "../../data/raw/MBTA_GTFS.zip"
OUTPUT_STATION_DATA = "../../data/clean/stations.csv"

# Get station coordinates on canvas.
# read raw green line coordinates.
green_line_station_coordinates = pd.read_json(GREEN_LINE_STATION_COORDINATES).T
green_line_station_coordinates.columns = ['x', 'y']
# read raw non-green line coordinates.
non_green_line_station_coordinates = pd.read_json(NON_GREEN_LINE_STATION_COORDINATES).T
non_green_line_station_coordinates.columns = ['x', 'y']
# adjust non-green line coordinates so that they are not offset relative to other stations
for dimension in ['x', 'y']:
    # difference between coordinates of park street in the two datasets will give offset
    offset = (green_line_station_coordinates.loc['place-pktrm', dimension] -
              non_green_line_station_coordinates.loc['place-pktrm', dimension])
    non_green_line_station_coordinates.loc[:, dimension] = non_green_line_station_coordinates[dimension] + offset
# append non-green line station coordinates to green line station coordinates
station_coordinates = pd.concat([green_line_station_coordinates, non_green_line_station_coordinates], axis=0).reset_index()
# drop stations which appear in both green line and non-green line, then reset index
station_coordinates = station_coordinates.drop_duplicates(subset='index').set_index('index')
# rename index
station_coordinates.index.name = "station_id"
# flip map vertically
station_coordinates['y'] = station_coordinates['y'].max() - station_coordinates['y']
# space out stations
station_coordinates = station_coordinates * 10

# Merge with station names.
station_names_dfs = []
for json_file in [NON_GREEN_LINE_STATION_NAMES, GREEN_LINE_STATION_NAMES]:
    with open(json_file) as station_json:
        station_data = json.load(station_json)
        station_names_data = station_data['nodes']
        station_names_dfs.append(pd.DataFrame.from_records(station_names_data))
station_names = (pd.concat(station_names_dfs, axis=0)  # append non-green line station names to green line station names
                 .drop_duplicates(subset='id')  # drop stations which appear in both green line and non-green line, then reset index
                 .set_index('id'))
station_df = pd.concat([station_coordinates, station_names], axis=1)

# Add GLX stations and Assembly
# get GLX E station coordinates; they are on the same line as lechmere and sciences park
xs = []
ys = []
names = ["East Somerville", "Gilman Square", "Magoun Square", "Ball Square", "Medford/Tufts"]
ids = ["place-esomr", "place-gilmn", "place-mgngl", "place-balsq", "place-mdftf"]
p1 = (station_df.loc['place-lech', 'x'], station_df.loc['place-lech', 'y'])
p2 = (station_df.loc['place-spmnl', 'x'], station_df.loc['place-spmnl', 'y'])
def glx_function(x):
    y = (p2[1] - p1[1])/(p2[0] - p1[0])*(x - p1[0]) + p1[1]
    return y
for i in range(1, 6):
    curr_x = p1[0] - (p2[0] - p1[0]) * i
    curr_y = glx_function(curr_x)
    xs.append(curr_x)
    ys.append(curr_y)
# plot union square station directly west of lechmere
distance_between_lechmere_sciences_park = math.dist(p2, p1)
xs.append(station_df.loc['place-lech', 'x'] - distance_between_lechmere_sciences_park )
ys.append(station_df.loc['place-lech', 'y'])
names.append("Union Square")
ids.append("place-unsqu")
# Add assembly in current Oak Grove location
names.append("Assembly")
ids.append("place-astao")
xs.append(station_df.loc['place-welln', 'x'])
ys.append(station_df.loc['place-welln', 'y'])
# Move Oak Grove, Malden Center, wellington, up one unit
station_df.loc[['place-ogmnl', 'place-welln', 'place-mlmnl'], 'y'] = station_df.loc[['place-ogmnl', 'place-welln', 'place-mlmnl'], 'y'] +\
                                     (station_df.loc['place-ogmnl', 'y'] - station_df.loc['place-mlmnl', 'y'])
glx_and_assembly_df = pd.DataFrame(index=ids)
glx_and_assembly_df['x'] = xs
glx_and_assembly_df['y'] = ys
glx_and_assembly_df['name'] = names
station_df = pd.concat([station_df, glx_and_assembly_df], axis=0)

# update stations around BU
delta_y = station_df.loc['place-brico', 'y'] - station_df.loc['place-plsgr', 'y']
delta_x = station_df.loc['place-brico', 'x'] - station_df.loc['place-plsgr', 'x']
station_df = station_df.drop(labels=['place-babck', 'place-plsgr'])
station_df = station_df.rename(index={'place-buwst': 'place-amory', 'place-stplb': 'place-babck'})
station_df = station_df.replace({"Boston University West Station": "Amory Street Station",
                                 "Saint Paul Street": "Babcock Street Station"})
green_line_b_to_shift = ['place-brico', 'place-harvd', 'place-grigg', 'place-alsgr', 'place-wrnst', 'place-wascm',
                         'place-sthld', 'place-chswk', 'place-chill', 'place-sougr', 'place-lake']
station_df.loc[green_line_b_to_shift, ['x', 'y']] = (station_df.loc[green_line_b_to_shift, ['x', 'y']] -
                                                     [delta_x, delta_y])

# Merge with station latitudes and longitudes.
# pull data from MBTA GTFS
feed = gtfs_kit.read_feed(Path(MBTA_GTFS), dist_units='km')
columns_to_keep = ['stop_name', 'parent_station', 'location_type', 'stop_lat', 'stop_lon']
station_latitudes_and_longitudes = (feed.get_stops()[columns_to_keep]  # keep select columns from stop data
                                    .groupby('stop_name')
                                    .fillna(method='bfill')  # fill parent station for the parent stations, which are missing parent station
                                    .loc[feed.get_stops()['location_type'] == 1, :]  # then restrict to parent stations
                                    .drop(columns='location_type')  # no longer need location type columnn
                                    .rename(columns={'parent_station': 'station_id'})  # rename parent station column to match station_df
                                    .set_index('station_id'))  # set index to prepare for merge


station_df = pd.concat([station_df, station_latitudes_and_longitudes], axis=1)

# drop stations which could not be merged with canvas coordinates
station_df = station_df.dropna(subset=['x'])

# Add line endpoint data
endpoints = ["place-alfcl", "place-asmnl", "place-brntn",
             "place-mdftf", "place-unsqu", "place-lake", "place-clmnl", "place-river", "place-hsmnl",
             "place-forhl", "place-ogmnl",
             "place-bomnl", "place-wondl"]
station_df.loc[:, 'endpoint'] = 0
station_df.loc[endpoints, 'endpoint'] = 1

# set station colors
station_df.loc[:, 'map_color'] = 'black'
station_df.loc[endpoints[0:3], 'map_color'] = 'red'
station_df.loc[endpoints[3:9], 'map_color'] = 'green'
station_df.loc[endpoints[9:11], 'map_color'] = 'orange'
station_df.loc[endpoints[11:13], 'map_color'] = 'blue'

# save cleaned data
station_df.index.name = "station_id"
station_df.to_csv(OUTPUT_STATION_DATA)
