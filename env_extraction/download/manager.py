"""Download orchestration and cache management."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import VariableConfig, VARIABLE_REGISTRY, DEFAULT_RAW_DIR, DEFAULT_PADDING_KM
from ..utils import km_to_deg_lat, km_to_deg_lon, normalize_lon, split_bbox_at_dateline
from .erddap import download_erddap, download_erddap_monthly, download_erddap_monthly_parallel
from .copernicus import download_copernicus

logger = logging.getLogger(__name__)


def _needs_monthly_download(var_config: VariableConfig) -> bool:
    """Determine if a variable should use per-month downloads (ERDDAP only)."""
    if var_config.source != "erddap":
        return False
    return var_config.resolution_deg < 0.1 or var_config.stride > 1


def compute_id_month_groups(
    tracks: pd.DataFrame,
    padding_km: float = DEFAULT_PADDING_KM,
) -> dict[tuple[str, str], dict]:
    """Group track positions by (animal_id, year-month) and compute bounding boxes.

    Returns:
        Dict mapping (animal_id, 'YYYY-MM') to bbox dict.
    """
    tracks = tracks.copy()
    tracks["year_month"] = tracks["date"].dt.to_period("M").astype(str)

    groups = {}
    for (animal_id, ym), grp in tracks.groupby(["id", "year_month"]):
        lat_min = grp["lat"].min()
        lat_max = grp["lat"].max()
        mid_lat = (lat_min + lat_max) / 2

        pad_lat = km_to_deg_lat(padding_km)
        pad_lon = km_to_deg_lon(padding_km, mid_lat)

        lat_min_padded = max(lat_min - pad_lat, -90)
        lat_max_padded = min(lat_max + pad_lat, 90)

        lons = grp["lon"].values
        lon_min = normalize_lon(lons.min() - pad_lon)
        lon_max = normalize_lon(lons.max() + pad_lon)

        # Simple dateline check
        sorted_lons = np.sort(lons)
        if len(sorted_lons) > 1:
            gaps = np.diff(sorted_lons)
            crosses = gaps.max() > 180
        else:
            crosses = False

        if crosses:
            max_gap_idx = np.argmax(np.diff(sorted_lons))
            lon_min = sorted_lons[max_gap_idx + 1] - pad_lon
            lon_max = sorted_lons[max_gap_idx] + pad_lon

        groups[(str(animal_id), ym)] = {
            "lat_min": lat_min_padded,
            "lat_max": lat_max_padded,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "crosses_dateline": crosses,
            "date_min": grp["date"].min(),
            "date_max": grp["date"].max(),
        }

    return groups


def compute_month_groups(
    tracks: pd.DataFrame,
    padding_km: float = DEFAULT_PADDING_KM,
) -> dict[str, dict]:
    """Group track positions by year-month (across all animals) and compute union bounding boxes.

    Instead of one download per (animal, month), produces one download per month
    with a bbox that covers all animals active in that month.

    Returns:
        Dict mapping 'YYYY-MM' to bbox dict.
    """
    tracks = tracks.copy()
    tracks["year_month"] = tracks["date"].dt.to_period("M").astype(str)

    groups = {}
    for ym, grp in tracks.groupby("year_month"):
        lat_min = grp["lat"].min()
        lat_max = grp["lat"].max()
        mid_lat = (lat_min + lat_max) / 2

        pad_lat = km_to_deg_lat(padding_km)
        pad_lon = km_to_deg_lon(padding_km, mid_lat)

        lat_min_padded = max(lat_min - pad_lat, -90)
        lat_max_padded = min(lat_max + pad_lat, 90)

        lons = grp["lon"].values
        lon_min = normalize_lon(lons.min() - pad_lon)
        lon_max = normalize_lon(lons.max() + pad_lon)

        # Simple dateline check
        sorted_lons = np.sort(lons)
        if len(sorted_lons) > 1:
            gaps = np.diff(sorted_lons)
            crosses = gaps.max() > 180
        else:
            crosses = False

        if crosses:
            max_gap_idx = np.argmax(np.diff(sorted_lons))
            lon_min = sorted_lons[max_gap_idx + 1] - pad_lon
            lon_max = sorted_lons[max_gap_idx] + pad_lon

        groups[ym] = {
            "lat_min": lat_min_padded,
            "lat_max": lat_max_padded,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "crosses_dateline": crosses,
            "date_min": grp["date"].min(),
            "date_max": grp["date"].max(),
        }

    return groups


def download_variable(
    var_config: VariableConfig,
    bbox: dict,
    raw_dir: Path = DEFAULT_RAW_DIR,
    tracks: pd.DataFrame | None = None,
    padding_km: float = DEFAULT_PADDING_KM,
    max_workers: int = 3,
) -> list[Path]:
    """Download all chunks for a single variable.

    For high-res ERDDAP datasets, uses merged per-month downloads with parallelism.
    For coarse datasets, uses yearly bbox downloads (parallel for Copernicus).

    Returns:
        List of all downloaded/cached file paths.
    """
    output_dir = raw_dir / var_config.short_name

    if _needs_monthly_download(var_config) and tracks is not None:
        month_groups = compute_month_groups(tracks, padding_km)
        logger.info(
            f"Variable '{var_config.short_name}': {len(month_groups)} "
            f"monthly groups for download (merged across animals)"
        )
        if var_config.source == "erddap":
            return download_erddap_monthly_parallel(
                var_config, month_groups, output_dir, max_workers=max_workers
            )
        else:
            raise ValueError(
                f"Monthly download not supported for source: {var_config.source}"
            )

    # Yearly bbox download
    year_min = bbox["date_min"].year
    year_max = bbox["date_max"].year
    years = [y for y in range(year_min, year_max + 1)]

    if var_config.source == "copernicus" and max_workers > 1:
        # Parallel yearly downloads for Copernicus
        all_files = []
        failed_years = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    download_copernicus, var_config, bbox, year, output_dir
                ): year
                for year in years
            }
            for future in as_completed(futures):
                year = futures[future]
                try:
                    files = future.result()
                    all_files.extend(files)
                except Exception as ex:
                    logger.error(
                        f"Failed to download {var_config.short_name} {year}: {ex}. Skipping."
                    )
                    failed_years.append(year)
        if failed_years:
            logger.warning(
                f"{var_config.short_name}: {len(failed_years)} years failed: "
                f"{sorted(failed_years)}. Re-run to retry."
            )
        # Sort by filename for consistent ordering
        all_files.sort(key=lambda p: p.name)
    else:
        all_files = []
        for year in years:
            if var_config.source == "erddap":
                files = download_erddap(var_config, bbox, year, output_dir)
            elif var_config.source == "copernicus":
                files = download_copernicus(var_config, bbox, year, output_dir)
            else:
                raise ValueError(f"Unknown source: {var_config.source}")
            all_files.extend(files)

    logger.info(
        f"Variable '{var_config.short_name}': {len(all_files)} files ready"
    )
    return all_files


def download_all(
    variables: list[str],
    bbox: dict,
    raw_dir: Path = DEFAULT_RAW_DIR,
    tracks: pd.DataFrame | None = None,
    padding_km: float = DEFAULT_PADDING_KM,
    max_workers: int = 3,
    var_registry: dict | None = None,
) -> dict[str, list[Path]]:
    """Download all requested variables.

    Args:
        variables: List of variable short names (keys in variable registry).
        bbox: Bounding box dict from compute_bounding_box().
        raw_dir: Root directory for raw data files.
        tracks: Track DataFrame (needed for per-month downloads).
        padding_km: Padding for per-month bounding boxes.
        max_workers: Max concurrent download threads.
        var_registry: Variable registry dict. None = use built-in defaults.

    Returns:
        Dict mapping variable name to list of file paths.
    """
    if var_registry is None:
        var_registry = VARIABLE_REGISTRY
    results = {}
    for var_name in variables:
        if var_name not in var_registry:
            raise ValueError(
                f"Unknown variable '{var_name}'. "
                f"Available: {list(var_registry.keys())}"
            )
        var_config = var_registry[var_name]
        results[var_name] = download_variable(
            var_config, bbox, raw_dir, tracks, padding_km, max_workers
        )
    return results
