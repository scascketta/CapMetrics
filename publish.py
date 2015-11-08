#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sqlite3
import datetime
import subprocess

import arrow
import numpy as np
import pandas as pd

import utils


LOGGER = utils.LOGGER
CONFIG = utils._load_config()

POSITION_DTYPES = {
    'vehicle_id': np.str,
    'timestamp': np.str,
    'speed': np.float,
    'route_id': np.str,
    'trip_id': np.str,
    'latitude': np.float,
    'longitude': np.float,
}
SHAPE_DIST_CACHE = {}
OUTPUT_PATH = './data/vehicle_positions/'


class Trip:

    HEADSIGN_SQL = 'SELECT trips.trip_headsign FROM trips WHERE trips.trip_id = ?'

    def __init__(self, trip_id, conn):
        self.trip_id = trip_id
        self.conn = conn
        self._set_headsign()

    def _set_headsign(self):
        curr = self.conn.cursor()
        curr.execute(self.HEADSIGN_SQL, (self.trip_id,))
        data = curr.fetchone()
        self.headsign = data[0] if data is not None else ''


def process_positions(positions):
    LOGGER.info('Processing {} vehicle positions.'.format(len(positions)))
    positions = positions[(positions.route_id != np.NaN) & (positions.trip_id != np.NaN)]
    # Cannot add a new column to a subset of rows
    # Have to add empty headsign to all rows which will be set correctly later
    positions['trip_headsign'] = ''

    trip_ids = positions.trip_id.unique()

    with sqlite3.connect(utils.GTFS_DB) as conn:
        headsigns = {trip_id: Trip(trip_id, conn).headsign for trip_id in trip_ids}

    for trip_id in trip_ids:
        trip_pos = positions[positions.trip_id == trip_id]
        positions.loc[positions.trip_id == trip_id, 'trip_headsign'] = pd.Series([headsigns[trip_id]] * len(trip_pos), index=trip_pos.index)

    return positions


def get_positions(db_path, date=None):
    # Fetch vehicle positions for the date (in local time)
    if date is None:
        date = arrow.now()
    else:
        date = date.replace(days=1)

    date = arrow.now().replace(year=date.year, month=date.month, day=date.day, hour=0, minute=0, second=0, tzinfo='America/Chicago')
    day_before = date.replace(days=-1)
    LOGGER.info('Fetching positions from {} to {}.'.format(day_before.isoformat(), date.isoformat()))

    path = '/tmp/output.csv'

    args = ['capmetricsd', 'get', db_path, path, str(day_before.timestamp), str(date.timestamp)]
    code = subprocess.call(args)
    if int(code) != 0:
        raise Exception('Error getting data from capmetricsd: {}'.format(' '.join(args)))

    return pd.read_csv(path, dtype=POSITION_DTYPES)


def save_vehicle_positions(db_path, output, date=None):
    positions = get_positions(db_path, date)
    positions = process_positions(positions)

    if date is None:
        date = arrow.now().replace(days=-1)

    day = date.strftime('%Y-%m-%d')
    positions.to_csv('{}{}.csv'.format(output, day), index=False)


def save_range_vehicle_positions(db_path, output, start, end):
    num_days = (end - start).days + 1
    datelist = [end - datetime.timedelta(days=offset) for offset in range(num_days)]

    LOGGER.info('Saving data from {} to {}.'.format(start.isoformat(), end.isoformat()))
    for date in reversed(datelist):
        LOGGER.info('Saving data for {}'.format(date.isoformat()))
        save_vehicle_positions(db_path, output, date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Grab CapMetrics data from BoltDB and write it as a CSV file.')
    parser.add_argument('-d', '--db', required=True, type=str, help='Path to a BoltDB database.')
    parser.add_argument('-O', '--output', type=str, default=OUTPUT_PATH, help='File to write data to.')
    args = parser.parse_args()

    utils.load_gtfs_data(cache=True)
    save_vehicle_positions(args.db, args.output)
