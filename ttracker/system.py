from numpy import sqrt, where, newaxis
import pandas as pd

from ttracker.gtfs_realtime import GTFSRealtime



def _get_portion_of_distance_traveled(p1s, p2s, p3s, vehicle_stop_status):
    x1s, x2s, x3s = p1s[:, 0], p2s[:, 0], p3s[:, 0]
    y1s, y2s, y3s = p1s[:, 1], p2s[:, 1], p3s[:, 1]
    dxs, dys = x2s - x1s, y2s - y1s
    det = dxs * dxs + dys * dys
    det[det == 0] = 1  # avoid nan error
    a = (dys * (y3s - y1s) + dxs * (x3s - x1s)) / det
    lengths_p1s_p2s = sqrt(dxs * dxs + dys * dys)
    lengths_projections = sqrt((a * dxs) * (a * dxs) + (a * dys) * (a * dys))

    lengths_p1s_p2s = where(lengths_p1s_p2s == 0, 1, lengths_p1s_p2s)
    portions_of_distances_traveled = lengths_projections / lengths_p1s_p2s
    stopped_mask = (vehicle_stop_status == 1)
    portions_of_distances_traveled[stopped_mask] = 1
    return portions_of_distances_traveled


def _midpoint(p1s, p2s, percent):
    return p1s + (p2s - p1s) * percent[:, newaxis]


class System:
    station_data: pd.DataFrame
    links_data: pd.DataFrame
    _gtfs: GTFSRealtime
    _map_view: bool

    def __init__(self, path_to_station_data: str,
                 path_to_links_data: str,
                 path_to_stop_codes_station_id_crosswalk: str,
                 gtfs_realtime_url: str):
        cols_to_keep = ['station_id', 'name', 'x', 'y', 'stop_lat', 'stop_lon', 'endpoint']
        self.station_data = pd.read_csv(path_to_station_data, usecols=cols_to_keep, index_col='station_id')

        self.links_data = (pd.read_csv(path_to_links_data, dtype={'source_station_id': 'category',
                                                                 'target_station_id': 'category',
                                                                 'route_id': 'category'})
                           .drop(columns=['Unnamed: 0']))
        self._previous_station = pd.read_csv(path_to_links_data).set_index(
            ['route_id', 'target_station_id', 'direction'])

        self._gtfs = GTFSRealtime(gtfs_realtime_url, path_to_stop_codes_station_id_crosswalk)
    def update_trains(self):
        train_position_data = self._gtfs.get_train_positions()
        current_position_map = train_position_data[['longitude', 'latitude']].values

        # set "next station" equal to destination for moving trains and current station for stopped ones
        next_station_position_map = self.station_data.loc[
            train_position_data['next_station_id'], ['stop_lon', 'stop_lat']].values
        next_station_position_screen = self.station_data.loc[
            train_position_data['next_station_id'], ['x', 'y']].values

        # set "previous station" equal to origin for moving trains and last stopped station for stopped ones
        indexer = list(
            train_position_data[['route_id', 'next_station_id', 'direction_id']].itertuples(index=False, name=None))
        previous_station_position_map = self._previous_station.loc[
            indexer, ['lon_source', 'lat_source']].values
        previous_station_position_screen = self._previous_station.loc[
            indexer, ['x_source', 'y_source']].values

        # get portion of distance traveled between origin and destination stations
        portion_of_distance_traveled = _get_portion_of_distance_traveled(previous_station_position_map,
                                                                         next_station_position_map,
                                                                         current_position_map,
                                                                         train_position_data['current_status'].values)
        screen_coordinates_moving_trains = _midpoint(previous_station_position_screen,
                                                     next_station_position_screen,
                                                     portion_of_distance_traveled)

        # Train positions
        x = screen_coordinates_moving_trains[:, 0]
        y = screen_coordinates_moving_trains[:, 1]

        # Build hover text
        return (x,
                y,
                train_position_data['route_id'].str.split("-").str[0].values,
                (("Train: " + train_position_data['id'].reset_index(drop=True))
                 + "<br>" +
                 (train_position_data['current_status'].replace({0: "Next Stop: ",
                                                                            1: "Stopped At: ",
                                                                            2: "Next Stop: "}).reset_index(drop=True)) +
                 self.station_data.loc[train_position_data['next_station_id'], 'name'].reset_index(
                     drop=True)).values)