# -*- coding: utf-8 -*-
import os
import sys
import json
import errno
import logging

import gtfsdb
import requests
from gtfsdb.api import database_load

PY2 = sys.version_info[0] == 2
GTFS_DOWNLOAD_FILE = os.path.join('/tmp', 'capmetro_gtfs.zip')
GTFS_DB = os.path.join('/tmp', 'capmetro_gtfs_data.db')


class Config(dict):
    """Config-like dict object, borrowed from Flask."""

    def __init__(self, root_path, defaults=None):
        dict.__init__(self, defaults or {})
        self.root_path = root_path

    def from_json(self, filename, silent=False):
        """Updates the values in the config from a JSON file."""
        filename = os.path.join(self.root_path, filename)

        try:
            with open(filename) as json_file:
                obj = json.loads(json_file.read())
        except IOError as e:
            if silent and e.errno in (errno.ENOENT, errno.EISDIR):
                return False
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        return self.from_mapping(obj)

    def from_mapping(self, *mapping, **kwargs):
        """Updates the config like :meth:`update` ignoring items with non-upper
        keys."""
        mappings = []
        if len(mapping) == 1:
            if hasattr(mapping[0], 'items'):
                mappings.append(mapping[0].items())
            else:
                mappings.append(mapping[0])
        elif len(mapping) > 1:
            raise TypeError(
                'expected at most 1 positional argument, got %d' % len(mapping)
            )
        mappings.append(kwargs.items())
        for mapping in mappings:
            for (key, value) in mapping:
                if key.isupper():
                    self[key] = value
        return True

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, dict.__repr__(self))


def _setup_logging():
    logger = logging.getLogger(__name__)
    logger.propagate = False
    fmt = '[%(levelname)s] %(asctime)s: %(message)s'
    formatter = logging.Formatter(fmt=fmt, datefmt='%Y-%m-%d %H:%M:%S')

    info_handler = logging.StreamHandler(sys.stdout)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    err_handler = logging.StreamHandler(sys.stderr)
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(formatter)

    logger.addHandler(info_handler)
    logger.addHandler(err_handler)
    return logger


def _load_config():
    cfg = Config(os.getcwd())
    cfg.from_json('config.json')
    return cfg


LOGGER = _setup_logging()


def _fetch_gtfs_data():
    gtfs_url = 'https://data.texas.gov/download/r4v4-vz24/application/zip'
    res = requests.get(gtfs_url, stream=True)
    assert res.ok, 'problem fetching data. status_code={}'.format(res.status_code)

    with open(GTFS_DOWNLOAD_FILE, 'wb') as f:
        for chunk in res.iter_content(1024):
            f.write(chunk)
    LOGGER.info('saved to {}'.format(GTFS_DOWNLOAD_FILE))


def load_gtfs_data(cache=False):
    if cache and os.path.isfile(GTFS_DB):
        LOGGER.info('Using cached GTFS data at: {}'.format(GTFS_DB))
        return

    _fetch_gtfs_data()

    database_load(
        filename=GTFS_DOWNLOAD_FILE,
        batch_size=gtfsdb.config.DEFAULT_BATCH_SIZE,
        schema=gtfsdb.config.DEFAULT_SCHEMA,
        is_geospatial=False,
        tables=None,
        url='sqlite:///{}'.format(GTFS_DB),
    )


if __name__ == '__main__':
    load_gtfs_data()
