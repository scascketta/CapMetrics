#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import argparse

import arrow
import numpy as np
import pandas as pd

from utils import date_range


def get_metadata(day, name, data_dir):
    with open(os.path.join(data_dir, name, 'index.json'), 'r') as f:
        dates = json.load(f)

    dates['start'] = map(arrow.get, dates['start'])
    dates['end'] = map(arrow.get, dates['end'])

    start = filter(lambda t: day >= t, dates['start'])
    if len(start) > 1:
        start = start[-1]
    else:
        start = start[0]

    end = filter(lambda t: day <= t, dates['end'])[0]

    start_str = start.strftime('%Y%m%d')
    end_str = end.strftime('%Y%m%d')

    print('Get {} from {} to {}'.format(name, start_str, end_str))
    fpath = '{}_{}_{}.csv.gz'.format(name, start_str, end_str)
    return pd.read_csv(os.path.join(data_dir, name, fpath), compression='gzip')


def get_trip_stops(trip_id, stops, schedule):
    trip_schedule = schedule.loc[schedule.trip_id == trip_id]
    return stops.loc[stops.stop_id.isin(trip_schedule.stop_id)]


def get_nearest_stop(pos, trip_stops, stops):
    if len(trip_stops) == 0:
        return {}

    #  <series>.values accesses the underlying numpy array - faster
    lat_diff = pos.latitude - trip_stops.stop_lat.values
    lon_diff = pos.longitude - trip_stops.stop_lon.values

    distances = pd.Series(np.linalg.norm(zip(lat_diff, lon_diff), axis=1), index=trip_stops.index)
    stop_id = distances.argmin()

    return {
        'stop_id': stops.iat[stop_id, 0],
        'stop_lat': stops.iat[stop_id, 2],
        'stop_lon': stops.iat[stop_id, 3]
    }


def filter_schedule(schedule, trip_id, timestamp):
    match_trip = (schedule.trip_id == trip_id)
    match_weekday = (schedule[timestamp.strftime('%A').lower()] == 1)
    filtered = schedule.loc[(match_trip & match_weekday)]
    return filtered.loc[:, ('trip_id', 'arrival_time', 'stop_id')]


def get_arrival_times(trip_ids, positions, schedule):
    arrival_times = {}
    for trip_id in trip_ids:
        first = positions.loc[positions.trip_id == trip_id].iloc[0]

        trip_arrivals = filter_schedule(schedule, trip_id, first.timestamp)

        if trip_arrivals.empty:
            # try searching for arrival_times for the day before
            trip_arrivals = filter_schedule(schedule, trip_id, first.timestamp.replace(days=-1))

        if trip_arrivals.empty:
            # try searching for arrival_times for the day after
            trip_arrivals = filter_schedule(schedule, trip_id, first.timestamp.replace(days=1))

        trip_arrivals.stop_id = trip_arrivals.stop_id.astype(np.float64)
        arrival_times[trip_id] = trip_arrivals

    return arrival_times


def get_sched_time(pos, arrival_times):
    '''
    The hour of a GTFS arrival time is > 23 when a trip begins
    before midnight and ends after midnight. If the timestamp of the
    pos is just before midnight then the sched_time refers to the next
    day so we need to increment the sched_time's day
    '''
    trip_times = arrival_times[pos.trip_id]
    ix = np.where(trip_times.stop_id.values == pos.stop_id)
    if len(ix[0]) == 0:
        return None

    arrival_time = trip_times.iat[ix[0][0], 1]
    hour, minute, second = map(int, arrival_time.split(':'))

    days = 0
    if hour >= 24:
        days = 1 if pos.timestamp.datetime.hour == 23 else 0
        hour = hour - 24

    sched_time = pos.timestamp.replace(
        hour=hour,
        minute=minute,
        second=second,
        days=days
    )

    return sched_time


def get_sched_dev(pos):
    sched_dev = None
    if not pos.sched_time:
        return sched_dev

    if pos.timestamp < pos.sched_time:
        # ahead of schedule
        sched_dev = -(pos.sched_time - pos.timestamp).seconds
    else:
        # behind schedule
        sched_dev = (pos.timestamp - pos.sched_time).seconds

    return sched_dev


def select_pos_from_group(group):
    '''
    Let group be a group of positions where a vehicle has multiple positions
    recorded for a single stop. This most likely means that the bus was
    sitting there for a while, either ahead of schedule or behind schedule.

    For the purposes of measuring reliability, we really just want to know:

        A) Did it arrive ahead of schedule (causing people to miss the bus
           even if they get to the stop on time)?

        OR

        B) Did the bus arrive behind schedule, and if so, how late?

    In scenario A, all the positions have sched_dev < 0,
    which means the bus arrived at the stop early. We want to know how early
    the bus was when it left the stop, so we return the position with the
    largest sched_dev.

    In scenario B, at least one position has a sched_dev > 0, which means that
    the bus arrived at the stop past late. We want to know how late the bus was
    when it first arrived at the stop, so we return the position with the
    smallest sched_dev.
    '''

    if len(group) == 1:
        return group.iloc[0]

    # if no positions exist within 250m, they're probably not useful
    nearby = group.loc[group.distance_to_stop <= 0.25]
    if nearby.empty:
        return None

    nonneg = nearby.loc[nearby.sched_dev >= 0]
    if not nonneg.empty:
        # at least 1 position with sched_dev >= 0, so bus is on/behind schedule
        return nonneg.iloc[0]
    else:
        # no positions with sched_dev >= 0, so bus is ahead of schedule
        return nearby.iloc[-1]


def process_day(filepath, stops, schedule):
    print('Processing file:', filepath)
    positions = pd.read_csv(filepath)

    trip_stops = {}
    for trip_id in positions.trip_id.unique():
        trip_stops[trip_id] = get_trip_stops(trip_id, stops, schedule)

    positions.timestamp = positions.apply(lambda pos: arrow.get(pos.timestamp), axis=1)

    nearest_stop = lambda pos: get_nearest_stop(pos[1], trip_stops[pos[1].trip_id], stops)
    nearest_stop_dicts = map(nearest_stop, positions.iterrows())
    nearest_stops = pd.DataFrame(nearest_stop_dicts)

    positions['stop_lat'] = nearest_stops['stop_lat']
    positions['stop_lon'] = nearest_stops['stop_lon']
    positions['stop_id'] = nearest_stops['stop_id']

    arrival_times = get_arrival_times(positions.trip_id.unique(), positions, schedule)

    sched_times = positions.apply(lambda pos: get_sched_time(pos, arrival_times), axis=1)
    positions['sched_time'] = sched_times

    lat_diff = positions.latitude - positions.stop_lat
    lon_diff = positions.longitude - positions.stop_lon
    distances_to_stop = np.linalg.norm(zip(lat_diff, lon_diff), axis=1)
    positions['distance_to_stop'] = distances_to_stop * 100

    positions['sched_dev'] = positions.apply(lambda pos: get_sched_dev(pos), axis=1)

    positions['dayofweek'] = positions.apply(lambda pos: pos.timestamp.isoweekday(), axis=1)
    positions['hourofday'] = positions.apply(lambda pos: pos.timestamp.datetime.hour, axis=1)

    grouped = positions.groupby(['trip_id', 'stop_id'])
    selected_positions = grouped.apply(select_pos_from_group)

    return selected_positions


def main(start, end, data_dir):
    dates = date_range(arrow.get(start), arrow.get(end))
    print('Processing dates from {} to {}'.format(start, end))

    path = os.path.join(data_dir, 'vehicle_positions') + '/{}.csv'
    paths = map(lambda day: (path.format(day), arrow.get(day)), dates)

    results = []
    for fpath, day in paths:
        now = arrow.now()
        stops = get_metadata(day, 'stops', data_dir)
        schedule = get_metadata(day, 'schedule', data_dir)
        results.append(process_day(fpath, stops, schedule))
        print('Process {} in {}s'.format(day, (arrow.now() - now).seconds))

    combined = pd.concat(results)
    combined.to_csv('{}_{}.csv'.format(start, end), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', required=True, type=str, help='Start date as YEAR-MONTH-DAY')
    parser.add_argument('-e', '--end', required=True, type=str, help='End date as YEAR-MONTH-DAY')
    parser.add_argument('-d', '--data-dir', required=True, type=str, help='Path to data directory')
    args = parser.parse_args()

    main(args.start, args.end, args.data_dir)
