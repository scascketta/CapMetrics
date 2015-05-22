# -*- coding: utf-8 -*-
from math import sqrt

from shapely.geometry import Point


def _magnitude(p1, p2):
    vect_x = p2[0] - p1[0]
    vect_y = p2[1] - p1[1]
    return sqrt(vect_x ** 2 + vect_y ** 2)


distance = _magnitude


def _intersect_point_to_line(point, line_start, line_end):
    line_magnitude = _magnitude((line_end.x, line_end.y), (line_start.x, line_start.y))
    u = ((point.x - line_start.x) * (line_end.x - line_start.x) +
         (point.y - line_start.y) * (line_end.y - line_start.y)) \
        / (line_magnitude ** 2)

    if u < 0.00001 or u > 1:
        # closest point does not fall within the line segment,
        # take the shorter distance to an endpoint
        ix = _magnitude((point.x, point.y), (line_start.x, line_start.y))
        iy = _magnitude((point.x, point.y), (line_end.x, line_end.y))
        if ix > iy:
            return line_end
        else:
            return line_start
    else:
        ix = line_start.x + u * (line_end.x - line_start.x)
        iy = line_start.y + u * (line_end.y - line_start.y)
        return Point([ix, iy])


def _get_closest_shape_point(shape_points, point, previous=None):
    if previous is None:
        points = shape_points[:100]
    else:
        points = shape_points[previous:previous + 100]

    sorted_by_distance = sorted(points, key=lambda pt: distance((pt[0], pt[1]), point))

    ind = 0
    closest = sorted_by_distance[ind]
    while previous is not None and ind < len(sorted_by_distance) and closest[2] < previous:
        ind += 1
        closest = sorted_by_distance[ind]

    return {'shape_pt_sequence': closest[2]}


def calc_dist_between_pts(points):
        distances = []
        for ind in range(len(points) - 1):
            current_pt = (points[ind][0], points[ind][1])
            next_pt = (points[ind + 1][0], points[ind + 1][1])
            distances.append(distance(next_pt, current_pt))

        return distances


def _project_vehicle_onto_shape(shape_pts, vehicle, nearby):
    vehicle_pt = Point([vehicle['lat'], vehicle['lon']])
    closest = Point([shape_pts[nearby][0], shape_pts[nearby][1]])

    pt_after, pt_before = None, None
    if nearby > 0:
        pt_before = Point([shape_pts[nearby - 1][0], shape_pts[nearby - 1][1]])
    if nearby < len(shape_pts) - 1:
        pt_after = Point([shape_pts[nearby + 1][0], shape_pts[nearby + 1][1]])

    vehicle_ahead = True
    if nearby == len(shape_pts) - 1 and nearby > 0:
        vehicle_ahead = False
        ix_pt = _intersect_point_to_line(vehicle_pt, pt_before, closest)
    elif nearby != 0:
        ix_pt = _intersect_point_to_line(vehicle_pt, closest, pt_after)
        if ix_pt.equals(closest) or ix_pt.equals(pt_after):
            vehicle_ahead = False
            ix_pt = _intersect_point_to_line(vehicle_pt, pt_before, closest)
    else:
        ix_pt = _intersect_point_to_line(vehicle_pt, closest, pt_after)

    return ix_pt, vehicle_ahead


def calc_dist_traveled(pos, shape_points, dist_between_pts, prev_pt_seq, prev_dt):
    closest_pt = _get_closest_shape_point(shape_points, (pos['lat'], pos['lon']), prev_pt_seq)
    closest_pt_seq = closest_pt['shape_pt_sequence'] - 1
    prev_pt_seq = closest_pt_seq

    vehicle, vehicle_ahead_of_closest_pt = _project_vehicle_onto_shape(shape_points, pos, closest_pt_seq)

    partial_dt = sum(dist_between_pts[:closest_pt_seq])
    dist_from_vehicle_and_closest_pt = distance((vehicle.x, vehicle.y), (shape_points[closest_pt_seq][0], shape_points[closest_pt_seq][1]))
    if vehicle_ahead_of_closest_pt:
        dist_traveled = partial_dt + dist_from_vehicle_and_closest_pt
    else:
        dist_traveled = partial_dt - dist_from_vehicle_and_closest_pt

    decreased = False
    if dist_traveled < prev_dt:
        # ¯\_(ツ)_/¯ we tried
        decreased = True
        dist_traveled = prev_dt

    return dist_traveled, prev_pt_seq, decreased
