import json
from pathlib import Path

import gtfs_kit
import pandas as pd

GREEN_LINE_ROUTE_INFORMATION = "../../static/data/raw/station-network_green_line.json"
NON_GREEN_LINE_ROUTE_INFORMATION = "../../static/data/raw/station-network.json"
CLEANED_STATION_DATA = "../../static/data/clean/stations.csv"
MBTA_GTFS = "../../static/data/raw/MBTA_GTFS.zip"
STOP_CODES_STATION_ID_CROSSWALK = "../../static/data/clean/stop_codes_to_station_id_crosswalk.csv"
OUTPUT_DATA = "../../static/data/clean/links.csv"

links_dfs = []
for ROUTE_INFORMATION in [GREEN_LINE_ROUTE_INFORMATION, NON_GREEN_LINE_ROUTE_INFORMATION]:
    with open(ROUTE_INFORMATION) as route_information:
        data = json.load(route_information)
        nodes = pd.DataFrame().from_records(data['nodes'])
        links = pd.DataFrame().from_records(data['links'])
    origin_stations = nodes.loc[links['source']]['id'].reset_index(drop=True)
    destination_stations = nodes.loc[links['target']]['id'].reset_index(drop=True)
    links_dfs.append(pd.concat([origin_stations, destination_stations, links['line']], axis=1, ignore_index=True))

links_df = pd.concat(links_dfs, axis=0)

links_df.columns = ['source_station_id', 'target_station_id', 'line']

# manually add glx stations
lechmere_union_square = {'source_station_id': 'place-lech', 'target_station_id': 'place-unsqu',
                         'line': 'green-d'}
lechmere_east_somerville = {'source_station_id': 'place-lech', 'target_station_id': 'place-esomr',
                            'line': 'green-e'}
east_somerville_gilman_square = {'source_station_id': 'place-esomr', 'target_station_id': 'place-gilmn',
                                 'line': 'green-e'}
gilman_square_magoun_square = {'source_station_id': 'place-gilmn', 'target_station_id': 'place-mgngl',
                               'line': 'green-e'}
magoun_square_ball_square = {'source_station_id': 'place-mgngl', 'target_station_id': 'place-balsq',
                             'line': 'green-e'}
ball_square_medford_tufts = {'source_station_id': 'place-balsq', 'target_station_id': "place-mdftf",
                             'line': 'green-e'}
link_records = [lechmere_union_square, lechmere_east_somerville, east_somerville_gilman_square,
                gilman_square_magoun_square, magoun_square_ball_square,
                ball_square_medford_tufts]
links_df = pd.concat([links_df, pd.DataFrame().from_records(link_records)], axis=0)

# manually remove link between oak grove and malden center
# add links between sullivan square and assembly and assembly and wellington
sullivan_sq_to_assembly = {'source_station_id': 'place-sull', 'target_station_id': 'place-astao',
                           'line': 'orange'}
assembly_to_wellington = {'source_station_id': 'place-astao', 'target_station_id': 'place-welln',
                          'line': 'orange'}
sullivan_sq_to_wellington_mask = ((links_df['source_station_id'] == "place-sull") &
                                  (links_df['target_station_id'] == "place-welln"))
links_df = links_df.loc[~sullivan_sq_to_wellington_mask, :]
links_df = pd.concat([links_df, pd.DataFrame.from_records([assembly_to_wellington, sullivan_sq_to_assembly])],
                     axis=0)

# update links to reflect bu west, st paul, pleasant, babcock consolidation
links_to_remove = [("place-buwst", "place-bucen"),
                   ("place-stplb", "place-buwst"),
                   ("place-plsgr", "place-stplb"),
                   ("place-babck", "place-plsgr")]
for link_to_remove in links_to_remove:
    link_to_remove_mask = ((links_df['source_station_id'] == link_to_remove[1]) &
                           (links_df['target_station_id'] == link_to_remove[0]))
    links_df = links_df.loc[~link_to_remove_mask, :]
amory_to_bu_central = {'source_station_id': 'place-amory', 'target_station_id': 'place-bucen',
                       'line': 'green-b'}
babcock_to_amory = {'source_station_id': 'place-babck', 'target_station_id': 'place-amory',
                    'line': 'green-b'}
links_df = pd.concat([links_df, pd.DataFrame.from_records([amory_to_bu_central, babcock_to_amory])],
                     axis=0)

# add the reverse of each link
records = []
for link in links_df.itertuples(name=None):
    current_record = {'source_station_id': link[2], 'target_station_id': link[1],
                      'line': link[3]}
    records.append(current_record)
links_df = pd.concat([links_df, pd.DataFrame.from_records(records)], axis=0)

# add x, y coordinates and lat, long coordinates to links
station_df = pd.read_csv(CLEANED_STATION_DATA, index_col='station_id')
links_df.loc[:, ['x_source', 'y_source']] = station_df.loc[links_df['source_station_id'], ['x', 'y']].values
links_df.loc[:, ['x_target', 'y_target']] = station_df.loc[links_df['target_station_id'], ['x', 'y']].values
links_df.loc[:, ['lon_source', 'lat_source']] = station_df.loc[
    links_df['source_station_id'], ['stop_lon', 'stop_lat']].values
links_df.loc[:, ['lon_target', 'lat_target']] = station_df.loc[
    links_df['source_station_id'], ['stop_lon', 'stop_lat']].values

# rename line column for consistency across project
links_df = links_df.rename(columns={'line': 'route_id'})

# some green line stations currently have route id's equal to "green"
# duplicate when the same link is serviced by multiple green line branches
links_serviced = {'b': [('place-gover', 'place-pktrm'), ('place-pktrm', 'place-boyls'),
                        ('place-boyls', 'place-armnl'), ('place-armnl', 'place-coecl'),
                        ('place-coecl', 'place-hymnl'), ('place-hymnl', 'place-kencl')],
                  'c': [('place-gover', 'place-pktrm'), ('place-pktrm', 'place-boyls'),
                        ('place-boyls', 'place-armnl'), ('place-armnl', 'place-coecl'),
                        ('place-coecl', 'place-hymnl'), ('place-hymnl', 'place-kencl')],
                  'd': [('place-lech', 'place-spmnl'), ('place-spmnl', 'place-north'),
                        ('place-north', 'place-haecl'), ('place-haecl', 'place-gover'),
                        ('place-gover', 'place-pktrm'), ('place-pktrm', 'place-boyls'),
                        ('place-boyls', 'place-armnl'), ('place-armnl', 'place-coecl'),
                        ('place-coecl', 'place-hymnl'), ('place-hymnl', 'place-kencl')],
                  'e': [('place-lech', 'place-spmnl'), ('place-spmnl', 'place-north'),
                        ('place-north', 'place-haecl'), ('place-haecl', 'place-gover'),
                        ('place-gover', 'place-pktrm'), ('place-pktrm', 'place-boyls'),
                        ('place-boyls', 'place-armnl'), ('place-armnl', 'place-coecl')]}
links_to_add = []
for branch in ['b', 'c', 'd', 'e']:
    links = links_serviced[branch]
    for link in links:
        station_mask = (((links_df['source_station_id'] == link[0]) & (links_df['target_station_id'] == link[1])) |
                        ((links_df['source_station_id'] == link[1]) & (links_df['target_station_id'] == link[0])))
        green_mask = links_df['route_id'] == "green"
        links_to_add.append(links_df.loc[(station_mask) & (green_mask), :].replace({"green": f"green-{branch}"}))

links_df = pd.concat([links_df] + links_to_add, axis=0)
links_df = links_df.loc[links_df['route_id'] != "green", :]

# model red line with two separate branches
links_serviced = {"a": [('place-asmnl', 'place-smmnl'), ('place-smmnl', 'place-fldcr'), ('place-fldcr', 'place-shmnl'),
                        ('place-shmnl', 'place-jfk'), ('place-jfk', 'place-andrw'), ('place-andrw', 'place-brdwy'),
                        ('place-brdwy', 'place-sstat'), ('place-sstat', 'place-dwnxg'), ('place-dwnxg', 'place-pktrm'),
                        ('place-pktrm', 'place-chmnl'), ('place-chmnl', 'place-knncl'), ('place-knncl', 'place-cntsq'),
                        ('place-cntsq', 'place-harsq'), ('place-harsq', 'place-portr'), ('place-portr', 'place-davis'),
                        ('place-davis', 'place-alfcl')],
                  "b": [('place-brntn', 'place-qamnl'), ('place-qamnl', 'place-qnctr'), ('place-qnctr', 'place-wlsta'),
                        ('place-wlsta', 'place-nqncy'), ('place-nqncy', 'place-jfk'), ('place-jfk', 'place-andrw'),
                        ('place-andrw', 'place-brdwy'), ('place-brdwy', 'place-sstat'), ('place-sstat', 'place-dwnxg'),
                        ('place-dwnxg', 'place-pktrm'), ('place-pktrm', 'place-chmnl'), ('place-chmnl', 'place-knncl'),
                        ('place-knncl', 'place-cntsq'), ('place-cntsq', 'place-harsq'), ('place-harsq', 'place-portr'),
                        ('place-portr', 'place-davis'),
                        ('place-davis', 'place-alfcl')]}
links_to_add = []
for branch in ["a", "b"]:
    links = links_serviced[branch]
    for link in links:
        station_mask = (((links_df['source_station_id'] == link[0]) & (links_df['target_station_id'] == link[1])) |
                        ((links_df['source_station_id'] == link[1]) & (links_df['target_station_id'] == link[0])))
        red_mask = links_df['route_id'] == "red"
        links_to_add.append(links_df.loc[(station_mask) & (red_mask), :].replace({"red": f"red-{branch}"}))

links_df = pd.concat([links_df] + links_to_add, axis=0)
links_df = links_df.loc[links_df['route_id'] != "red", :]

# assign direction to each link
directions = []
stop_orders = {}
for route_id in ['red-a', 'red-b', 'blue', 'orange', 'green-b', 'green-c', 'green-d', 'green-e']:
    stop_orders[route_id] = pd.read_csv(f"../../static/data/raw/{route_id}_stop_order.csv")['station_id']
for link in links_df.to_dict('records'):
    route_id = link['route_id']
    stop_order = stop_orders[route_id]
    source_station_index = stop_order.loc[stop_order == link['source_station_id']].index
    target_station_index = stop_order[stop_order == link['target_station_id']].index
    if len(source_station_index) == 0 or len(target_station_index) == 0:
        continue

    if source_station_index[0] < target_station_index[0]:
        directions.append(0)
    else:
        directions.append(1)
links_df['direction'] = directions

links_df = links_df.loc[~links_df.duplicated(), :]

# add self-links for endpoints
endpoints = ['place-gover', 'place-forhl', 'place-wondl', 'place-bomnl', 'place-brntn', 'place-asmnl', 'place-alfcl',
             'place-alfcl', 'place-clmnl', 'place-lake', 'place-ogmnl', 'place-gover', 'place-spmnl', 'place-river',
             'place-hsmnl']
columns = ['x', 'y', 'stop_lon', 'stop_lat']
coordinates = pd.concat([station_df.loc[endpoints, columns],
                         station_df.loc[endpoints, columns]], axis=1).reset_index(drop=True)
coordinates.columns = ['x_source', 'y_source', 'lon_source', 'lat_source',  'x_target', 'y_target', 'lon_target', 'lat_target']
source_station_ids = pd.Series(endpoints, name='source_station_id')
target_station_ids = pd.Series(endpoints, name='target_station_id')
route_ids = pd.Series(['green-c', 'orange', 'blue', 'blue', 'red-b', 'red-a', 'red-b', 'red-a', 'green-c', 'green-b',
                       'orange', 'green-b', 'green-c', 'green-d', 'green-e'], name="route_id")
directions = pd.Series([0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1], name='direction')
self_links = pd.concat([source_station_ids, target_station_ids, route_ids, coordinates, directions], axis=1)

links_df = pd.concat([links_df, self_links], axis=0)

links_df.to_csv(OUTPUT_DATA)
