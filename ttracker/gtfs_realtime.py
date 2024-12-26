from typing import List
from pandas import read_csv, Series, DataFrame, json_normalize
import polars as pl
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

    def _clean_vehicle_positions_df(self, vehicles_df: pl.LazyFrame):
        rapid_transit_route_ids = ["Blue", "Red", "Orange", "Green-B", "Green-C", "Green-D", "Green-E"]
        clean_vehicles_df = (
            vehicles_df
            .rename(lambda column: column.split(".")[-1])
            .filter(pl.col("route_id").is_in(rapid_transit_route_ids))
            .with_columns(route_id=pl.col("route_id").str.to_lowercase())
            .filter(pl.col("stop_id") != "71199")
            .drop_nulls()
            .drop_nans()
            .rename({'stop_id': 'next_stop_id'})
        )
        return clean_vehicles_df

    def _clean_trip_updates_df(self,
                               trip_updates_df: DataFrame,
                               routes_to_keep: List[str]):
        clean_trip_updates_df = (
            trip_updates_df
            .rename(lambda column: column.split(".")[-1])
            .filter(pl.col("route_id").is_in(routes_to_keep))
            .drop("route_id")
            .drop_nulls()
            .drop_nans()
            .explode("stop_time_update")
            .with_columns(
                stop_id=pl.col("stop_time_update").struct.json_encode().cast(pl.String).str.split("stop_id").list.get(
                    -1).str.extract('(\d+)').cast(pl.Int32))
            .drop("stop_time_update")
        )
        return clean_trip_updates_df

    def get_train_positions(self):
        # Pull and clean vehicle positions
        vehicle_positions_response = get(self._gtfs_rt_vehicle_positions)
        self._vehicle_positions_feed.ParseFromString(vehicle_positions_response.content)
        columns_to_keep = ['id', 'vehicle.trip.trip_id', 'vehicle.trip.route_id', 'vehicle.stop_id',
                           'vehicle.current_status', 'vehicle.trip.direction_id', 'vehicle.position.longitude',
                           'vehicle.position.latitude']
        vehicle_positions_df = pl.LazyFrame(
            json_normalize(protobuf_to_dict(self._vehicle_positions_feed)['entity'])[columns_to_keep])
        vehicle_positions_df = self._clean_vehicle_positions_df(vehicle_positions_df)

        # Pull and clean trip updates obtain a list of red line a, red line b trips, then set route_id column of vehicles df appropriately
        trip_updates_response = get(self._gtfs_rt_trip_updates)
        self._trip_updates_feed.ParseFromString(trip_updates_response.content)
        columns_to_keep = ['trip_update.trip.trip_id',
                           'trip_update.stop_time_update',
                           'trip_update.trip.route_id']
        trip_updates_df = pl.LazyFrame(
            json_normalize(protobuf_to_dict(self._trip_updates_feed)['entity'])[columns_to_keep])
        routes_to_keep = ["Red"]
        trip_updates_df = self._clean_trip_updates_df(trip_updates_df, routes_to_keep)
        red_line_a_station_codes = [334, 70093, 70094, 70261, 70091, 70092, 323, 70089, 70090,
                                    70087, 70088]
        # true if red-a, false if red-b
        red_line_a_or_b_trips_marked = (
            trip_updates_df
            .with_columns(red_line_a_stop=pl.col("stop_id").is_in(red_line_a_station_codes))
            .group_by("trip_id")
            .agg(pl.any("red_line_a_stop"))
        )
        red_a_trips = (
            red_line_a_or_b_trips_marked
            .filter(pl.col("red_line_a_stop") == True)
            .select(pl.col("trip_id"))
            .collect()
        )
        red_b_trips = (
            red_line_a_or_b_trips_marked
            .filter(pl.col("red_line_a_stop") == False)
            .select(pl.col("trip_id"))
            .collect()
        )
        vehicle_positions_df = (
            vehicle_positions_df
            .with_columns(
                pl.when(pl.col("trip_id").is_in(red_a_trips))
                .then(pl.lit("red-a"))
                .when(pl.col("trip_id").is_in(red_b_trips))
                .then(pl.lit("red-b"))
                .otherwise(pl.col("route_id"))
                .alias("route_id")

            )
        )

        # Harmonize stop IDs for station endpoints
        vehicle_positions_df = (
            vehicle_positions_df
            .with_columns(
                pl.when(pl.col("next_stop_id").str.contains("Oak Grove", literal=True))
                .then(70036)
                .when(pl.col("next_stop_id").str.contains("Braintree", literal=True))
                .then(38671)
                .when(pl.col("next_stop_id").str.contains("Alewife", literal=True))
                .then(141)
                .when(pl.col("next_stop_id").str.contains("Forest Hills", literal=True))
                .then(10642)
                .when(pl.col("next_stop_id").str.contains("Union Square", literal=True))
                .then(70503)
                .otherwise(pl.col("next_stop_id"))
                .cast(pl.Int64)
                .alias("next_stop_id")

            )
        ).collect()

        # Resume here
        # TODO:
        # Getting an error, not sure if it is here or elsewhere
        vehicle_positions_df = (
            vehicle_positions_df
            .with_columns(pl.Series(values=self._stop_code_to_station_id_crosswalk[
                vehicle_positions_df.get_column("next_stop_id").to_list()], name="next_station_id"))
            .drop("next_stop_id")
        )

        return vehicle_positions_df.to_pandas()
