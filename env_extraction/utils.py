"""Coordinate transforms, dateline handling, and spatial utilities."""

import numpy as np


def km_to_deg_lat(km: float) -> float:
    """Convert kilometers to degrees latitude."""
    return km / 111.0


def km_to_deg_lon(km: float, lat: float) -> float:
    """Convert kilometers to degrees longitude at a given latitude."""
    cos_lat = np.cos(np.radians(lat))
    if cos_lat < 1e-10:
        return 360.0  # Near poles, return full circle
    return km / (111.0 * cos_lat)


def haversine_km(lon1: float, lat1: float, lon2, lat2) -> np.ndarray:
    """Haversine distance in km. lon2/lat2 can be arrays."""
    lon1, lat1 = np.radians(lon1), np.radians(lat1)
    lon2, lat2 = np.radians(lon2), np.radians(lat2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def crosses_dateline(lon_min: float, lon_max: float) -> bool:
    """Check if a bounding box crosses the dateline."""
    return lon_min > lon_max or (lon_max - lon_min) > 350


def split_bbox_at_dateline(lon_min: float, lon_max: float):
    """Split a bounding box at the dateline into two boxes.

    Returns list of (lon_min, lon_max) tuples.
    """
    if not crosses_dateline(lon_min, lon_max):
        return [(lon_min, lon_max)]
    # Split: [lon_min, 180] and [-180, lon_max]
    return [(lon_min, 180.0), (-180.0, lon_max)]


def normalize_lon(lon: float) -> float:
    """Normalize longitude to [-180, 180]."""
    return ((lon + 180) % 360) - 180
