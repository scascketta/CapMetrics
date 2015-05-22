#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import sqlite3
import datetime

import arrow
import rethinkdb

import utils
import metrics


LOGGER = utils.LOGGER
CONFIG = utils._load_config()


class Trip:

    HEADSIGN_SQL = 'SELECT trips.trip_headsign FROM trips WHERE trips.trip_id = ?'
    SHAPE_SQL = '''SELECT shapes.shape_pt_lat, shapes.shape_pt_lon, shapes.shape_pt_sequence, shapes.shape_id
        FROM
            shapes,  trips
        WHERE
            trips.trip_id = ? AND
            trips.shape_id = shapes.shape_id
        ORDER BY
            shape_pt_sequence ASC
    '''

    def __init__(self, trip_id, cursor):
        self.trip_id = trip_id
        self.positions = []
        self.cursor = cursor
        self._set_headsign()
        self._set_shape_data()

    def process_shape(self):
        if not self.missing_shape:
            self._filter_shape_points()
            self._calc_dist_between_pts()

    def add_position(self, pos):
        self.positions.append(pos)

    def _set_shape_data(self):
        self.cursor.execute(self.SHAPE_SQL, (self.trip_id,))
        pts = list(self.cursor)

        if not pts:
            self.missing_shape = True
        else:
            self.missing_shape = False
            self.shape_points = pts
            self.shape_id = pts[0][3]

    def _set_headsign(self):
        self.cursor.execute(self.HEADSIGN_SQL, (self.trip_id,))
        data = self.cursor.fetchone()
        self.headsign = data[0] if data is not None else ''

    def _filter_shape_points(self):
        new_shape_pts = []
        prev = self.shape_points[0]
        prev_ind = 1

        for pt in self.shape_points[1:]:
            if pt[0] != prev[0] and pt[1] != prev[1]:
                new_shape_pts.append((pt[0], pt[1], prev_ind, pt[3]))
                prev_ind += 1
            prev = pt

        self.shape_points = new_shape_pts

    def _calc_dist_between_pts(self):
        distances = []
        points = self.shape_points

        for ind in range(len(points) - 1):
            current_pt = (points[ind][0], points[ind][1])
            next_pt = (points[ind + 1][0], points[ind + 1][1])
            distances.append(metrics.distance(next_pt, current_pt))

        self.dist_between_pts = distances


def fetch_positions(conn, date):
    # Fetch vehicle positions for the date (in local time)
    if date is None:
        date = arrow.now()
    else:
        date = date.replace(days=1)

    date = arrow.now().replace(year=date.year, month=date.month, day=date.day, hour=0, minute=0, second=0, tzinfo='America/Chicago')
    day_before = date.replace(days=-1)
    LOGGER.info('Fetching positions from {} to {}.'.format(day_before.isoformat(), date.isoformat()))

    query = rethinkdb.table('vehicle_position') \
            .between(day_before.datetime, date.datetime, index='timestamp') \
            .order_by(index=rethinkdb.asc('timestamp')) \
            .without('id', 'bearing')

    return list(query.run(conn))


def _prep_trip_data(curr, positions):
    trips = {}

    for pos in positions:
        not_in_service = pos['route_id'] == '' or pos['trip_id'] == ''
        if not_in_service:
            continue

        trip_id = pos['trip_id']
        if trip_id not in trips:
            trip = Trip(trip_id, curr)
            trip.add_position(pos)
            trips[trip_id] = trip
        else:
            trips[trip_id].add_position(pos)

    for trip in trips.values():
        trip.process_shape()

    return trips


def _process_trips(trips):
    processed = []
    total_errors = 0

    for trip in trips.values():
        prev_dist_traveled = 0
        prev_pt_seq = None

        for ind, pos in enumerate(trip.positions):
            pos['trip_headsign'] = trip.headsign
            pos['timestamp'] = arrow.get(pos['timestamp']).to('America/Chicago').isoformat()
            pos['lon'] = pos['location']['coordinates'][0]
            pos['lat'] = pos['location']['coordinates'][1]
            del pos['location']

            if not trip.missing_shape:
                pos['dist_traveled'], prev_pt_seq, decreased = metrics.calc_dist_traveled(pos, trip.shape_points, trip.dist_between_pts, prev_pt_seq, prev_dist_traveled)
                prev_dist_traveled = pos['dist_traveled']
            else:
                pos['dist_traveled'] = -1

            processed.append(pos)

            if decreased:
                total_errors += 1

    return processed, total_errors


def process_positions(curr, positions):
    LOGGER.info('Processing {} vehicle positions.'.format(len(positions)))
    trips = _prep_trip_data(curr, positions)
    processed, total_errors = _process_trips(trips)
    LOGGER.info('Finished processing {} trips with {} errors.'.format(len(trips), total_errors))
    return processed


def write_csv(data, date=None):
    if date is None:
        date = arrow.now().replace(days=-1)

    day = date.strftime('%Y-%m-%d')

    if utils.PY2:
        f = open('./data/vehicle_positions/{day}.csv'.format(day=day), 'wb')
    else:
        f = open('./data/vehicle_positions/{day}.csv'.format(day=day), 'w', newline='')

    field_names = data[0].keys()
    dw = csv.DictWriter(f, field_names)
    dw.writeheader()
    dw.writerows(data)
    f.close()


def save_vehicle_positions(rethink_conn, sqlite_conn, date=None):
    positions = fetch_positions(rethink_conn, date)
    curr = sqlite_conn.cursor()
    data = process_positions(curr, positions)
    write_csv(data, date)


def save_range_vehicle_positions(rethink_conn, sqlite_conn, start, end):
    num_days = (end - start).days + 1
    datelist = [end - datetime.timedelta(days=offset) for offset in range(num_days)]

    LOGGER.info('Saving data from {} to {}.'.format(start.isoformat(), end.isoformat()))
    for date in reversed(datelist):
        LOGGER.info('Saving data for {}'.format(date.isoformat()))
        save_vehicle_positions(rethink_conn, sqlite_conn, date)


if __name__ == '__main__':
    rethink_conn = rethinkdb.connect(CONFIG['RETHINKDB_HOST'], 28015)
    rethink_conn.use(CONFIG['RETHINKDB_DB'])

    utils.load_gtfs_data(cache=True)
    with sqlite3.connect(utils.GTFS_DB) as sqlite_conn:
        save_vehicle_positions(rethink_conn, sqlite_conn)
