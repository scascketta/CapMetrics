# CapMetrics
This repo contains vehicle location data provided by [Capital Metro](http://www.capmetro.org/). The data describing the routes, schedules etc is described by a standard format called [GTFS](https://developers.google.com/transit/gtfs/). You can find the GTFS data describing the routes and schedules for CapMetro on their [data portal](https://data.texas.gov/Capital-Metro/Capital-Metro-Google-Transit/r4v4-vz24).

## Query this dataset on BigQuery!

I've loaded some of the data here from March 2015 to December 2015 onto a [public dataset](https://bigquery.cloud.google.com/table/capmetrics:capmetrics.vehicle_position) on Google BigQuery. Go check it out!

# Available Data

The captured vehicle location data for each day is made available the next day under `data/vehicle_locations`. The data is available as CSV files. Do whatever you like, but please credit back here if you make something public.

## How the data is collected

CapMetro publishes all of the instantaneous vehicle positions on their data portal [here](https://data.texas.gov/Capital-Metro/Capital-Metro-Veh-Loc-pb/i5qp-g5fd). I continously capture the live vehicle positions, store it in a database, and publish it the next day.

## Data Format
At the moment, only [vehicle positions](https://developers.google.com/transit/gtfs-realtime/reference#VehiclePosition) are recorded.

Here's what their data looks like:

| field | description | GTFS-RT reference |
| --- | --- | --- |
| vehicle_id | ID of the transit vehicle | [VehicleDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#VehicleDescriptor) |
| speed | speed of the vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| longitude | longitude of vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| latitude | latitude of vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| route_id | ID of the route the vehicle is assigned to | [TripDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#TripDescriptor) |
| timestamp | Moment at which the vehicle's position was measured in the ISO 8601 format. | [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) |
| trip_id | Refers to a trip from the GTFS feed | [TripDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#TripDescriptor) |
| dist_traveled **DEPRECATED after 2015-09-30** | The distance (in miles) traveled by the vehicle along the shape of the current trip. If the shape for the trip is not available, this will be set to -1. This metric is not provided by CapMetro, so I calculate it as best I can (see the [code](https://github.com/scascketta/capmetrics/blob/5225ecf417fa641fbc4c65bb0d12986f534dd00f/metrics.py#L89-L101)). | N/A |
