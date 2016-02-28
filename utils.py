#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals

import os

import gtfsdb
import requests
from gtfsdb.api import database_load

GTFS_DOWNLOAD_FILE = os.path.join('/tmp', 'capmetro_gtfs.zip')
GTFS_DB = os.path.join('/tmp', 'capmetro_gtfs_data.db')
FETCH_URL = 'https://data.texas.gov/download/r4v4-vz24/application/zip'


def _fetch_gtfs_data(gtfs_url):
    res = requests.get(gtfs_url, stream=True)
    assert res.ok, 'problem fetching data. status_code={}'.format(
        res.status_code)

    with open(GTFS_DOWNLOAD_FILE, 'wb') as f:
        for chunk in res.iter_content(1024):
            f.write(chunk)
    print('saved to {}'.format(GTFS_DOWNLOAD_FILE))


def load_gtfs_data(gtfs_url=FETCH_URL, cache=False):
    if cache and os.path.isfile(GTFS_DB):
        print('Using cached GTFS data at: {}'.format(GTFS_DB))
        return

    _fetch_gtfs_data(gtfs_url)

    database_load(
        filename=GTFS_DOWNLOAD_FILE,
        batch_size=gtfsdb.config.DEFAULT_BATCH_SIZE,
        schema=gtfsdb.config.DEFAULT_SCHEMA,
        is_geospatial=False,
        tables=None,
        url='sqlite:///{}'.format(GTFS_DB),
    )


if __name__ == '__main__':
    load_gtfs_data(FETCH_URL)
