from typing import List
from pandas import read_csv, Series, DataFrame, json_normalize

from requests import get
from protobuf_to_dict import protobuf_to_dict
from google.transit import gtfs_realtime_pb2


def _clean_stop_code(raw_stop_code: str):
    stop_code = raw_stop_code
    if "Braintree" in raw_stop_code:
        stop_code = 38671
    elif "Oak Grove" in raw_stop_code:
        stop_code = 70036
    elif "Union Square" in raw_stop_code:
        stop_code = 70503
    elif "Alewife" in raw_stop_code:
        stop_code = 141
    elif "Forest Hills" in raw_stop_code:
        stop_code = 10642

    return int(stop_code)


class GTFSRealtime:
    _gtfs_rt_vehicle_positions: str
    _gtfs_rt_trip_updates: str
    _stop_code_to_station_id_crosswalk: Series

    def __init__(self,
                 gtfs_rt_vehicle_positions_url: str,
                 path_to_stop_code_to_station_id_crosswalk: str):
        self._gtfs_rt_vehicle_positions = gtfs_rt_vehicle_positions_url
        self._gtfs_rt_trip_updates = "https://cdn.mbta.com/realtime/TripUpdates.pb"  # TODO: add as param
        self._vehicle_positions_feed = gtfs_realtime_pb2.FeedMessage()
        self._trip_updates_feed = gtfs_realtime_pb2.FeedMessage()
        self._stop_code_to_station_id_crosswalk = read_csv(path_to_stop_code_to_station_id_crosswalk,
                                                              index_col='stop_code')['station_id'].astype("category")

    def _clean_vehicle_positions_df(self, vehicles_df: DataFrame):


        # rename columns
        clean_vehicles_df = vehicles_df.rename(columns=lambda column: column.split(".")[-1])

        # keep only rapid transit route ids
        rapid_transit_route_ids = ["Blue", "Red", "Orange", "Green-B", "Green-C", "Green-D", "Green-E"]
        clean_vehicles_df = clean_vehicles_df.loc[clean_vehicles_df['route_id'].isin(rapid_transit_route_ids), :]

        # make route ids lowercase
        clean_vehicles_df.loc[:, 'route_id'] = clean_vehicles_df['route_id'].str.lower()

        # drop 71199 stop ids
        clean_vehicles_df = clean_vehicles_df.loc[clean_vehicles_df['stop_id'] != '71199', :]

        # drop rows without information
        clean_vehicles_df = clean_vehicles_df.dropna()

        # rename
        clean_vehicles_df = clean_vehicles_df.rename(columns={'stop_id': 'next_stop_id'})

        return clean_vehicles_df

    def _clean_trip_updates_df(self,
                               trip_updates_df: DataFrame,
                               routes_to_keep: List[str]):


        # rename columns
        clean_trip_updates_df = trip_updates_df.rename(columns=lambda column: column.split(".")[-1])

        # keep only rapid transit route ids
        rapid_transit_mask = clean_trip_updates_df['route_id'].isin(routes_to_keep)
        clean_trip_updates_df = clean_trip_updates_df.loc[rapid_transit_mask, :]

        # no longer need route id column
        clean_trip_updates_df = clean_trip_updates_df.drop(columns='route_id')

        # drop rows without information
        clean_trip_updates_df = clean_trip_updates_df.dropna()

        # convert stop_time_update, which is a column of lists, into a column of dicts by expanding rows
        clean_trip_updates_df = clean_trip_updates_df.explode('stop_time_update')

        # get sequence of stop_ids for each trip
        stop_id = json_normalize(clean_trip_updates_df['stop_time_update'],
                                    max_level=0)[['stop_id']]

        # drop original stop_time_updates column
        clean_trip_updates_df = clean_trip_updates_df.drop(columns='stop_time_update')

        # add stop_id column
        clean_trip_updates_df.loc[:, ['stop_id']] = stop_id.values

        return clean_trip_updates_df

    def get_train_positions(self):
        vehicle_positions_response = get(self._gtfs_rt_vehicle_positions)
        self._vehicle_positions_feed.ParseFromString(vehicle_positions_response.content)
        columns_to_keep = ['id', 'vehicle.trip.trip_id', 'vehicle.trip.route_id', 'vehicle.stop_id',
                           'vehicle.current_status', 'vehicle.trip.direction_id', 'vehicle.position.longitude',
                           'vehicle.position.latitude']
        vehicle_positions_df = json_normalize(protobuf_to_dict(self._vehicle_positions_feed)['entity'])[columns_to_keep]
        vehicle_positions_df = self._clean_vehicle_positions_df(vehicle_positions_df).set_index(
            'trip_id')
        trip_updates_response = get(self._gtfs_rt_trip_updates)
        self._trip_updates_feed.ParseFromString(trip_updates_response.content)
        columns_to_keep = ['trip_update.trip.trip_id',
                           'trip_update.stop_time_update',
                           'trip_update.trip.route_id']
        trip_updates_df = json_normalize(protobuf_to_dict(self._trip_updates_feed)['entity'])[columns_to_keep]

        routes_to_keep = ["Red"]
        trip_updates_df = self._clean_trip_updates_df(trip_updates_df, routes_to_keep)
        red_line_a_station_codes = ['334', '70093', '70094', '70261', '70091', '70092', '323', '70089', '70090',
                                    '70087', '70088']
        # true if red-a, false if red-b
        red_line_trips = (trip_updates_df
                          .groupby('trip_id')['stop_id']
                          .agg(lambda group: group.isin(red_line_a_station_codes).any()))
        red_a_trips = Series(red_line_trips.loc[red_line_trips].index)
        red_a_trips = red_a_trips.loc[red_a_trips.isin(vehicle_positions_df.index)]
        red_b_trips = Series(red_line_trips.loc[~red_line_trips].index)
        red_b_trips = red_b_trips.loc[red_b_trips.isin(vehicle_positions_df.index)]

        vehicle_positions_df.loc[red_a_trips, 'route_id'] = 'red-a'
        vehicle_positions_df.loc[red_b_trips, 'route_id'] = 'red-b'
        vehicle_positions_df = vehicle_positions_df.loc[vehicle_positions_df['route_id'] != 'red']

        vehicle_positions_df.loc[vehicle_positions_df['next_stop_id'].str.contains("Oak Grove",
                                                                                   regex=False,
                                                                                   na=False), 'next_stop_id'] = 70036
        vehicle_positions_df.loc[vehicle_positions_df['next_stop_id'].str.contains("Braintree",
                                                                                   regex=False,
                                                                                   na=False), 'next_stop_id'] = 38671
        vehicle_positions_df.loc[vehicle_positions_df['next_stop_id'].str.contains("Alewife",
                                                                                   regex=False,
                                                                                   na=False), 'next_stop_id'] = 141
        vehicle_positions_df.loc[vehicle_positions_df['next_stop_id'].str.contains("Forest Hills",
                                                                                   regex=False,
                                                                                   na=False), 'next_stop_id'] = 10642
        vehicle_positions_df.loc[vehicle_positions_df['next_stop_id'].str.contains("Union Square",
                                                                                   regex=False,
                                                                                   na=False), 'next_stop_id'] = 70503

        vehicle_positions_df.loc[:, 'next_stop_id'] = vehicle_positions_df['next_stop_id'].astype(int)
        vehicle_positions_df.loc[:, 'next_station_id'] = self._stop_code_to_station_id_crosswalk[
            vehicle_positions_df['next_stop_id']].values

        vehicle_positions_df = vehicle_positions_df.drop(columns='next_stop_id')

        return vehicle_positions_df
