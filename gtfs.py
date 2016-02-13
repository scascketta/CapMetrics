#!/usr/bin/env python
from __future__ import unicode_literals

import sqlite3
import zipfile
import tempfile

import pandas as pd


def load_gtfs_stops(gtfs_path):
    with zipfile.ZipFile(gtfs_path, 'r') as zf:
        tmpdir = tempfile.gettempdir()
        fpath = zf.extract('stops.txt', path=tmpdir)
        stops = pd.read_csv(fpath)
        stops = stops.loc[:, ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
        return stops


def load_gtfs_schedule(sqlite_path):
    sql = '''
    SELECT
        stop_times.trip_id,
        stop_times.arrival_time,
        stop_times.stop_id,
        calendar.sunday,
        calendar.monday,
        calendar.tuesday,
        calendar.wednesday,
        calendar.thursday,
        calendar.friday,
        calendar.saturday
    FROM stop_times, trips, calendar
    WHERE
        stop_times.trip_id = trips.trip_id AND
        trips.service_id = calendar.service_id;
    '''
    with sqlite3.connect(sqlite_path) as conn:
        df = pd.read_sql(sql, conn)
        df.trip_id = df.trip_id.astype(int)
        df.stop_id = df.stop_id.astype(int)
        return df
