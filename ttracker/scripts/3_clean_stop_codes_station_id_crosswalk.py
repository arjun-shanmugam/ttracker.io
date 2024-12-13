import pandas as pd
import gtfs_kit
from pathlib import Path

MBTA_GTFS = "../../data/raw/MBTA_GTFS.zip"
OUTPUT_STATION_DATA = "../../data/clean/stop_codes_to_station_id_crosswalk.csv"


feed = gtfs_kit.read_feed(Path(MBTA_GTFS), dist_units='km')
stops_df = feed.get_stops()
stops_df = stops_df.loc[:, ['stop_code', 'parent_station']]
stops_df = stops_df.dropna().rename(columns={'parent_station': 'station_id'})
stop_codes_station_id_crosswalk = stops_df.set_index('stop_code')

stop_codes_station_id_crosswalk.to_csv(OUTPUT_STATION_DATA)