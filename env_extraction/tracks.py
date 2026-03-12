"""Load and validate track CSVs."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DEFAULT_SE_CAP_KM, DEFAULT_PADDING_KM
from .utils import km_to_deg_lat, km_to_deg_lon, normalize_lon

logger = logging.getLogger(__name__)


# Argos Location Class → SE (km) lookup table
ARGOS_LC_SE_KM = {
    "3": 0.25, "2": 0.5, "1": 1.5, "0": 5.0,
    "A": 10.0, "B": 20.0, "Z": 50.0,
}


def load_id_filter(csv_path: Path, id_col: str = "ID") -> list[str]:
    """Load animal IDs from a CSV filter file.

    Args:
        csv_path: Path to CSV with an ID column.
        id_col: Name of the column containing IDs.

    Returns:
        List of ID strings.
    """
    df = pd.read_csv(csv_path)
    ids = df[id_col].astype(str).tolist()
    logger.info(f"Loaded {len(ids)} animal IDs from {csv_path}")
    return ids


def load_tracks(
    input_dir: Path,
    pattern: str = "*_rw_predicted.csv",
    animal_ids: list[str] | None = None,
    se_cap_km: float = DEFAULT_SE_CAP_KM,
    track_format: "TrackFormat | None" = None,
) -> pd.DataFrame:
    """Load and merge track CSVs.

    Supports flexible column mapping via TrackFormat. When no TrackFormat is
    provided, assumes the legacy animotum format (id, date, lon, lat, x.se, y.se).

    Args:
        input_dir: Directory containing CSV files.
        pattern: Glob pattern for CSV files.
        animal_ids: If provided, only load tracks for these IDs.
        se_cap_km: Cap SE values at this value (km).
        track_format: Column mapping configuration. None = legacy defaults.

    Returns:
        DataFrame with standardized columns: id, date, lon, lat, se_x, se_y, se_capped
        plus any extra columns specified in track_format.
    """
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' in {input_dir}")

    # Resolve column mapping
    if track_format is not None:
        id_col = track_format.id_col
        date_col = track_format.date_col
        lon_col = track_format.lon_col
        lat_col = track_format.lat_col
        se_x_col = track_format.se_x_col
        se_y_col = track_format.se_y_col
        lc_col = track_format.location_class_col
        fixed_se = track_format.fixed_se_km
        extra_columns = track_format.extra_columns or []
        id_from_filename = track_format.id_from_filename
        id_filename_separator = track_format.id_filename_separator
        id_filename_index = track_format.id_filename_index
    else:
        # Legacy animotum defaults
        id_col = "id"
        date_col = "date"
        lon_col = "lon"
        lat_col = "lat"
        se_x_col = "x.se"
        se_y_col = "y.se"
        lc_col = None
        fixed_se = 50.0
        extra_columns = []
        id_from_filename = True
        id_filename_separator = "_"
        id_filename_index = 0

    frames = []
    for f in files:
        if id_from_filename:
            file_id = f.stem.split(id_filename_separator)[id_filename_index]
            if animal_ids is not None and file_id not in animal_ids:
                continue
        df = pd.read_csv(f)
        if id_from_filename and id_col not in df.columns:
            df[id_col] = f.stem.split(id_filename_separator)[id_filename_index]
        frames.append(df)

    if not frames:
        raise ValueError(f"No tracks found for requested animal IDs: {animal_ids}")

    tracks = pd.concat(frames, ignore_index=True)

    # Filter by animal_ids from column (for single-file datasets)
    if animal_ids is not None and not id_from_filename:
        tracks = tracks[tracks[id_col].astype(str).isin(animal_ids)]
        if tracks.empty:
            raise ValueError(f"No tracks found for requested animal IDs: {animal_ids}")

    # Standardize column names to internal schema
    rename_map = {
        id_col: "id",
        date_col: "date",
        lon_col: "lon",
        lat_col: "lat",
    }
    tracks = tracks.rename(columns=rename_map)
    tracks["id"] = tracks["id"].astype(str)
    tracks["date"] = pd.to_datetime(tracks["date"])

    # Handle SE columns: from explicit columns, location class, or fixed value
    if se_x_col and se_x_col in tracks.columns:
        tracks = tracks.rename(columns={se_x_col: "se_x", se_y_col: "se_y"})
    elif se_x_col and se_x_col in rename_map.values():
        pass  # Already renamed
    elif lc_col and lc_col in tracks.columns:
        # Derive SE from Argos location class
        tracks["se_x"] = tracks[lc_col].astype(str).map(ARGOS_LC_SE_KM).fillna(fixed_se)
        tracks["se_y"] = tracks["se_x"].copy()
        logger.info(f"Derived SE from location class column '{lc_col}'")
    else:
        # Use fixed SE for all positions
        tracks["se_x"] = fixed_se
        tracks["se_y"] = fixed_se
        logger.info(f"Using fixed SE = {fixed_se} km for all positions")

    # Ensure se_x/se_y exist (handle legacy x.se/y.se rename)
    if "se_x" not in tracks.columns and "x.se" in tracks.columns:
        tracks = tracks.rename(columns={"x.se": "se_x", "y.se": "se_y"})

    # Drop rows with missing critical columns
    required = ["lon", "lat", "se_x", "se_y"]
    n_before = len(tracks)
    tracks = tracks.dropna(subset=required)
    n_dropped = n_before - len(tracks)
    if n_dropped > 0:
        logger.warning(f"Dropped {n_dropped} rows with missing lon/lat/SE values")

    # Cap SE values
    tracks["se_capped"] = (tracks["se_x"] > se_cap_km) | (tracks["se_y"] > se_cap_km)
    n_capped = tracks["se_capped"].sum()
    if n_capped > 0:
        logger.info(f"Capped {n_capped} rows with SE > {se_cap_km} km")
    tracks["se_x"] = tracks["se_x"].clip(upper=se_cap_km)
    tracks["se_y"] = tracks["se_y"].clip(upper=se_cap_km)

    # Normalize longitudes
    tracks["lon"] = tracks["lon"].apply(normalize_lon)

    logger.info(
        f"Loaded {len(tracks)} positions for {tracks['id'].nunique()} animals "
        f"({tracks['date'].min().date()} to {tracks['date'].max().date()})"
    )

    # Build output columns
    out_cols = ["id", "date", "lon", "lat", "se_x", "se_y", "se_capped"]
    for col in extra_columns:
        if col in tracks.columns:
            out_cols.append(col)
    return tracks[out_cols]


def compute_bounding_box(
    tracks: pd.DataFrame, padding_km: float = DEFAULT_PADDING_KM
) -> dict:
    """Compute bounding box from tracks with padding and dateline handling.

    Returns:
        dict with keys: lat_min, lat_max, lon_min, lon_max, crosses_dateline,
              date_min, date_max
    """
    lat_min = tracks["lat"].min()
    lat_max = tracks["lat"].max()
    date_min = tracks["date"].min()
    date_max = tracks["date"].max()

    # Padding in degrees
    pad_lat = km_to_deg_lat(padding_km)
    mid_lat = (lat_min + lat_max) / 2
    pad_lon = km_to_deg_lon(padding_km, mid_lat)

    lat_min = max(lat_min - pad_lat, -90)
    lat_max = min(lat_max + pad_lat, 90)

    # Handle longitude with potential dateline crossing
    lons = tracks["lon"].values
    # Check if data spans across dateline by looking for large gaps
    sorted_lons = np.sort(lons)
    gaps = np.diff(sorted_lons)
    max_gap_idx = np.argmax(gaps)

    if gaps[max_gap_idx] > 180:
        # Dateline crossing detected
        # The "empty" gap is between sorted_lons[max_gap_idx] and sorted_lons[max_gap_idx+1]
        lon_min = sorted_lons[max_gap_idx + 1] - pad_lon  # Westernmost (positive side)
        lon_max = sorted_lons[max_gap_idx] + pad_lon       # Easternmost (negative side)
        crosses_dateline = True
    else:
        lon_min = normalize_lon(lons.min() - pad_lon)
        lon_max = normalize_lon(lons.max() + pad_lon)
        crosses_dateline = False

    bbox = {
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_min": lon_min,
        "lon_max": lon_max,
        "crosses_dateline": crosses_dateline,
        "date_min": date_min,
        "date_max": date_max,
    }
    logger.info(f"Bounding box: {bbox}")
    return bbox
