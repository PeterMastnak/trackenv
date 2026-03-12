"""Auto-detect column roles in track CSV files and write mapping to project.yaml."""

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Heuristic patterns for column role detection (lowercase matching)
_ID_PATTERNS = [
    "id", "animal_id", "ptt", "tag_id", "deploy_id", "individual",
    "animalid", "tagid", "deployid", "individual_id", "track_id",
]
_DATE_PATTERNS = [
    "date", "datetime", "timestamp", "time", "date_time", "utc",
    "gmt", "datetimeloc", "datetime_utc",
]
_LON_PATTERNS = ["lon", "longitude", "long", "x", "lng"]
_LAT_PATTERNS = ["lat", "latitude", "y"]
_SE_X_PATTERNS = [
    "x.se", "se_x", "error_lon", "lon_error", "x_error",
    "semi_major", "loc_error_km", "se.lon", "error_x",
]
_SE_Y_PATTERNS = [
    "y.se", "se_y", "error_lat", "lat_error", "y_error",
    "semi_minor", "se.lat", "error_y",
]
_LC_PATTERNS = ["lc", "loc_class", "location_class", "argos_lc"]


def _match_column(col: str, patterns: list[str]) -> bool:
    """Check if a column name matches any of the given patterns (case-insensitive)."""
    low = col.lower().strip()
    return low in patterns


def _find_candidates(columns: list[str], patterns: list[str]) -> list[str]:
    """Find all columns matching a set of patterns."""
    return [c for c in columns if _match_column(c, patterns)]


def _is_date_like(series: pd.Series, sample_size: int = 20) -> bool:
    """Check if a series contains date-like values."""
    sample = series.dropna().head(sample_size)
    if sample.empty:
        return False
    try:
        pd.to_datetime(sample)
        return True
    except (ValueError, TypeError):
        return False


def _is_lon_like(series: pd.Series) -> bool:
    """Check if numeric values are in a plausible longitude range."""
    if not pd.api.types.is_numeric_dtype(series):
        return False
    vals = series.dropna()
    if vals.empty:
        return False
    return vals.min() >= -180 and vals.max() <= 360


def _is_lat_like(series: pd.Series) -> bool:
    """Check if numeric values are in a plausible latitude range."""
    if not pd.api.types.is_numeric_dtype(series):
        return False
    vals = series.dropna()
    if vals.empty:
        return False
    return vals.min() >= -90 and vals.max() <= 90


def _pick_best(candidates: list[str], role: str) -> str | None:
    """Pick the best candidate, or None if empty."""
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.warning(
            f"Multiple candidates for {role}: {candidates}. Using '{candidates[0]}'."
        )
        return candidates[0]
    return None


def scan_tracks(
    path: Path,
    file_pattern: str = "*.csv",
) -> dict:
    """Scan track CSV files and auto-detect column roles.

    Args:
        path: Path to a single CSV file or a directory of CSVs.
        file_pattern: Glob pattern when path is a directory.

    Returns:
        Dict with detected mapping and summary information.
    """
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.glob(file_pattern))
    else:
        raise FileNotFoundError(f"Path not found: {path}")

    if not files:
        raise FileNotFoundError(f"No CSV files matching '{file_pattern}' in {path}")

    # Read first file to detect columns
    sample_df = pd.read_csv(files[0], nrows=100)
    columns = list(sample_df.columns)

    # Read all files for summary stats
    all_frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_frames.append((f, df))
        except Exception as e:
            logger.warning(f"Could not read {f}: {e}")

    # Detect column roles
    id_candidates = _find_candidates(columns, _ID_PATTERNS)
    date_candidates = _find_candidates(columns, _DATE_PATTERNS)
    lon_candidates = _find_candidates(columns, _LON_PATTERNS)
    lat_candidates = _find_candidates(columns, _LAT_PATTERNS)
    se_x_candidates = _find_candidates(columns, _SE_X_PATTERNS)
    se_y_candidates = _find_candidates(columns, _SE_Y_PATTERNS)
    lc_candidates = _find_candidates(columns, _LC_PATTERNS)

    # Value-based fallback for ambiguous cases
    if not date_candidates:
        for col in columns:
            if _is_date_like(sample_df[col]):
                date_candidates.append(col)

    if not lon_candidates:
        for col in columns:
            if col not in (lat_candidates + date_candidates + id_candidates):
                if _is_lon_like(sample_df[col]):
                    lon_candidates.append(col)

    if not lat_candidates:
        for col in columns:
            if col not in (lon_candidates + date_candidates + id_candidates):
                if _is_lat_like(sample_df[col]):
                    lat_candidates.append(col)

    # Pick best matches
    id_col = _pick_best(id_candidates, "ID")
    date_col = _pick_best(date_candidates, "date")
    lon_col = _pick_best(lon_candidates, "longitude")
    lat_col = _pick_best(lat_candidates, "latitude")
    se_x_col = _pick_best(se_x_candidates, "SE X")
    se_y_col = _pick_best(se_y_candidates, "SE Y")
    lc_col = _pick_best(lc_candidates, "location class")

    # Determine if ID comes from filename
    id_from_filename = False
    id_filename_separator = "_"
    id_filename_index = 0
    if id_col is None and len(files) > 1:
        # Check if filenames contain ID-like patterns
        stems = [f.stem for f in files]
        if all("_" in s for s in stems):
            id_from_filename = True
            logger.info("No ID column found; IDs appear to come from filenames")

    # Identify extra columns
    known_cols = {id_col, date_col, lon_col, lat_col, se_x_col, se_y_col, lc_col}
    known_cols.discard(None)
    extra_columns = [c for c in columns if c not in known_cols]

    # Compute summary stats
    all_data = pd.concat([df for _, df in all_frames], ignore_index=True)
    n_unique_ids = 0
    if id_col and id_col in all_data.columns:
        n_unique_ids = all_data[id_col].nunique()
    elif id_from_filename:
        n_unique_ids = len(files)

    date_range = (None, None)
    if date_col and date_col in all_data.columns:
        try:
            dates = pd.to_datetime(all_data[date_col])
            date_range = (dates.min(), dates.max())
        except (ValueError, TypeError):
            pass

    lat_range = lon_range = (None, None)
    if lat_col and lat_col in all_data.columns:
        lat_range = (all_data[lat_col].min(), all_data[lat_col].max())
    if lon_col and lon_col in all_data.columns:
        lon_range = (all_data[lon_col].min(), all_data[lon_col].max())

    se_info = None
    if se_x_col and se_x_col in all_data.columns:
        se_info = {
            "se_x_range": (all_data[se_x_col].min(), all_data[se_x_col].max()),
            "se_y_range": (all_data[se_y_col].min(), all_data[se_y_col].max()) if se_y_col else None,
        }

    # Ambiguities
    ambiguities = {}
    for role, cands in [
        ("ID", id_candidates), ("date", date_candidates),
        ("longitude", lon_candidates), ("latitude", lat_candidates),
    ]:
        if len(cands) > 1:
            ambiguities[role] = cands

    result = {
        "format": {
            "id_col": id_col,
            "date_col": date_col,
            "lon_col": lon_col,
            "lat_col": lat_col,
            "se_x_col": se_x_col,
            "se_y_col": se_y_col,
            "location_class_col": lc_col,
            "fixed_se_km": 50.0,
            "extra_columns": extra_columns,
            "id_from_filename": id_from_filename,
            "id_filename_separator": id_filename_separator,
            "id_filename_index": id_filename_index,
        },
        "summary": {
            "n_files": len(files),
            "n_positions": len(all_data),
            "n_unique_ids": n_unique_ids,
            "date_range": date_range,
            "lat_range": lat_range,
            "lon_range": lon_range,
            "se_info": se_info,
            "columns": columns,
            "extra_columns": extra_columns,
        },
        "ambiguities": ambiguities,
        "file_structure": "single_file" if len(files) == 1 else "multi_file",
    }

    return result


def print_scan_report(result: dict) -> None:
    """Print a human-readable scan report to terminal."""
    fmt = result["format"]
    summary = result["summary"]

    print("\n" + "=" * 60)
    print("TRACK SCAN REPORT")
    print("=" * 60)

    print(f"\n  File structure: {result['file_structure']}")
    print(f"  Files found:   {summary['n_files']}")
    print(f"  Total rows:    {summary['n_positions']}")
    print(f"  Unique IDs:    {summary['n_unique_ids']}")

    if summary["date_range"][0] is not None:
        print(f"  Date range:    {summary['date_range'][0]} to {summary['date_range'][1]}")
    if summary["lat_range"][0] is not None:
        print(f"  Lat range:     {summary['lat_range'][0]:.2f} to {summary['lat_range'][1]:.2f}")
    if summary["lon_range"][0] is not None:
        print(f"  Lon range:     {summary['lon_range'][0]:.2f} to {summary['lon_range'][1]:.2f}")

    print(f"\n  Detected column mapping:")
    print(f"    ID column:        {fmt['id_col'] or '(from filename)'}")
    print(f"    Date column:      {fmt['date_col']}")
    print(f"    Longitude column: {fmt['lon_col']}")
    print(f"    Latitude column:  {fmt['lat_col']}")
    print(f"    SE X column:      {fmt['se_x_col'] or '(none — will use fixed SE)'}")
    print(f"    SE Y column:      {fmt['se_y_col'] or '(none — will use fixed SE)'}")
    if fmt["location_class_col"]:
        print(f"    Location class:   {fmt['location_class_col']} (can derive SE from Argos LC)")
    if fmt["id_from_filename"]:
        print(f"    ID from filename: yes (sep='{fmt['id_filename_separator']}', index={fmt['id_filename_index']})")

    if summary["se_info"]:
        se = summary["se_info"]
        print(f"\n  SE ranges:")
        print(f"    SE X: {se['se_x_range'][0]:.2f} to {se['se_x_range'][1]:.2f}")
        if se["se_y_range"]:
            print(f"    SE Y: {se['se_y_range'][0]:.2f} to {se['se_y_range'][1]:.2f}")
    elif not fmt["se_x_col"]:
        print(f"\n  No SE columns found. Will use fixed_se_km = {fmt['fixed_se_km']} km")

    if summary["extra_columns"]:
        print(f"\n  Extra columns available: {', '.join(summary['extra_columns'])}")

    if result["ambiguities"]:
        print(f"\n  AMBIGUITIES (review and correct in project.yaml):")
        for role, cands in result["ambiguities"].items():
            print(f"    {role}: multiple matches → {cands}")

    print("\n" + "=" * 60)


def write_scan_to_yaml(result: dict, yaml_path: Path) -> None:
    """Write or update the tracks.format section in project.yaml.

    If the file exists, only the tracks.format section is updated.
    If it doesn't exist, a full starter YAML is written.
    """
    import yaml

    fmt = result["format"]

    if yaml_path.exists():
        with open(yaml_path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {
            "project": {"name": "my_study"},
            "tracks": {"directory": "tracks/", "file_pattern": "*.csv"},
            "extraction": {"se_cap_km": 200.0, "sigma_multiplier": 2.0, "padding_km": 50.0},
            "paths": {"raw_dir": "data/raw", "output_dir": "data/output"},
        }

    if "tracks" not in data:
        data["tracks"] = {}
    data["tracks"]["format"] = {
        "id_col": fmt["id_col"],
        "date_col": fmt["date_col"],
        "lon_col": fmt["lon_col"],
        "lat_col": fmt["lat_col"],
        "se_x_col": fmt["se_x_col"],
        "se_y_col": fmt["se_y_col"],
        "location_class_col": fmt["location_class_col"],
        "fixed_se_km": fmt["fixed_se_km"],
        "extra_columns": fmt["extra_columns"],
        "id_from_filename": fmt["id_from_filename"],
        "id_filename_separator": fmt["id_filename_separator"],
        "id_filename_index": fmt["id_filename_index"],
    }

    with open(yaml_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"\nWrote column mapping to {yaml_path}")
