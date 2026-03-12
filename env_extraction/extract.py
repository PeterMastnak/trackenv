"""Gaussian-weighted environmental data extraction at track positions."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

from .config import (
    VariableConfig,
    VARIABLE_REGISTRY,
    DEFAULT_RAW_DIR,
    DEFAULT_SIGMA_MULTIPLIER,
)
from .derivations import DERIVATION_REGISTRY
from .utils import haversine_km, km_to_deg_lat, km_to_deg_lon

logger = logging.getLogger(__name__)


def _open_env_datasets(
    var_name: str, raw_dir: Path
) -> xr.Dataset | None:
    """Open all NetCDF files for a variable as a single xarray Dataset."""
    var_dir = raw_dir / var_name
    nc_files = sorted(var_dir.glob("*.nc"))
    if not nc_files:
        logger.warning(f"No NetCDF files found in {var_dir}")
        return None

    ds = xr.open_mfdataset(nc_files, combine="by_coords", chunks="auto")
    return ds


def _open_id_month_file(
    var_name: str, animal_id: str, year_month: str, raw_dir: Path
) -> xr.Dataset | None:
    """Open a single per-animal per-month NetCDF file."""
    var_dir = raw_dir / var_name
    # Try exact match first, then with _part suffix for dateline splits
    candidates = sorted(var_dir.glob(f"{var_name}_{animal_id}_{year_month}*.nc"))
    if not candidates:
        return None
    ds = xr.open_mfdataset(candidates, combine="by_coords", chunks="auto")
    return ds


def _open_month_file(
    var_name: str, year_month: str, raw_dir: Path
) -> xr.Dataset | None:
    """Open a single per-month NetCDF file (merged across sharks)."""
    var_dir = raw_dir / var_name
    # Match {var}_{YYYY-MM}.nc and {var}_{YYYY-MM}_part*.nc
    candidates = sorted(var_dir.glob(f"{var_name}_{year_month}*.nc"))
    # Exclude shark-specific files (those have an extra ID segment: {var}_{id}_{YYYY-MM}.nc)
    candidates = [
        f for f in candidates
        if f.stem.count("_") <= 2  # var_YYYY-MM or var_YYYY-MM_partN
    ]
    if not candidates:
        return None
    ds = xr.open_mfdataset(candidates, combine="by_coords", chunks="auto")
    return ds


def _has_month_only_files(var_name: str, raw_dir: Path) -> bool:
    """Check if per-month (merged) files exist for this variable."""
    var_dir = raw_dir / var_name
    if not var_dir.exists():
        return False
    # Match {var}_YYYY-MM.nc — exactly one underscore before the date
    import re
    pattern = re.compile(rf"^{re.escape(var_name)}_\d{{4}}-\d{{2}}(_.+)?\.nc$")
    for f in var_dir.iterdir():
        if f.is_file() and pattern.match(f.name):
            # Exclude shark-specific files (have shark ID between var and date)
            parts = f.stem.split("_")
            # Month-only: ['sst', '2002-05'] or ['sst', '2002-05', 'part0']
            # Shark-month: ['sst', '170200201', '2002-05']
            if len(parts) >= 2 and "-" in parts[1] and len(parts[1]) == 7:
                return True
    return False


def _has_id_month_files(var_name: str, raw_dir: Path) -> bool:
    """Check if per-animal-month files exist (e.g., sst_170200201_2002-05.nc)."""
    var_dir = raw_dir / var_name
    if not var_dir.exists():
        return False
    # ID-month files have 3+ underscore-separated parts: {var}_{animalID}_{YYYY-MM}.nc
    for f in var_dir.glob(f"{var_name}_*_????-??.nc"):
        parts = f.stem.split("_")
        # parts[1] is the animal ID (numeric), parts[2] is YYYY-MM
        if len(parts) >= 3 and "-" in parts[2] and parts[1].isdigit():
            return True
    return False


def _identify_coord_names(ds: xr.Dataset) -> tuple[str, str, str]:
    """Identify longitude, latitude, and time coordinate names in a dataset.

    Returns:
        (lon_name, lat_name, time_name)
    """
    lon_name = lat_name = time_name = None
    for name in ds.coords:
        low = name.lower()
        if low in ("longitude", "lon", "x"):
            lon_name = name
        elif low in ("latitude", "lat", "y"):
            lat_name = name
        elif low in ("time", "t", "date"):
            time_name = name

    if lon_name is None or lat_name is None or time_name is None:
        raise ValueError(
            f"Could not identify coordinates. Found: {list(ds.coords)}. "
            f"Identified: lon={lon_name}, lat={lat_name}, time={time_name}"
        )
    return lon_name, lat_name, time_name


def gaussian_weighted_extract(
    ds: xr.Dataset,
    var_name: str,
    lon: float,
    lat: float,
    date: pd.Timestamp,
    radius_km: float,
    sigma_km: float,
    lon_coord: str,
    lat_coord: str,
    time_coord: str,
) -> tuple[float, float]:
    """Extract Gaussian-weighted mean and SD for a single position.

    Args:
        ds: Environmental dataset.
        var_name: Variable name in the dataset.
        lon, lat: Position coordinates.
        date: Position timestamp.
        radius_km: Spatial radius for extraction (2σ).
        sigma_km: Gaussian sigma in km.
        lon_coord, lat_coord, time_coord: Coordinate names in ds.

    Returns:
        (weighted_mean, weighted_sd) — NaN if no valid data.
    """
    # Convert radius to degrees for spatial slicing
    r_lat = km_to_deg_lat(radius_km)
    r_lon = km_to_deg_lon(radius_km, lat)

    lat_min, lat_max = lat - r_lat, lat + r_lat
    lon_min, lon_max = lon - r_lon, lon + r_lon

    # Select nearest time
    time_slice = ds.sel({time_coord: date}, method="nearest")

    # Drop any extra dimensions (depth/zlev/etc.) — keep only lat/lon
    for dim in list(time_slice.dims):
        if dim not in (lon_coord, lat_coord):
            if time_slice.sizes[dim] == 1:
                time_slice = time_slice.isel({dim: 0})
            else:
                # Multi-level dimension — select first level
                time_slice = time_slice.isel({dim: 0})

    # Spatial slice — use slice for efficient subsetting, ensuring at least
    # the nearest cell is included when radius < grid spacing
    spatial = time_slice.sel(
        {
            lat_coord: slice(lat_min, lat_max),
            lon_coord: slice(lon_min, lon_max),
        }
    )

    # If slice found 0 cells in either dim (radius < grid spacing),
    # fall back to the single nearest cell
    if spatial.sizes[lat_coord] == 0 or spatial.sizes[lon_coord] == 0:
        spatial = time_slice.sel(
            {lat_coord: lat, lon_coord: lon}, method="nearest"
        )
        if var_name not in spatial:
            return np.nan, np.nan
        val = float(spatial[var_name].values)
        return (val, 0.0) if np.isfinite(val) else (np.nan, np.nan)

    if var_name not in spatial:
        return np.nan, np.nan

    # Get data as 2D array (lat, lon)
    data = spatial[var_name].values
    # Squeeze out any remaining singleton dims but keep at least 2D
    while data.ndim > 2:
        data = data.squeeze(axis=0) if data.shape[0] == 1 else data
        if data.ndim > 2:
            break
    if data.ndim < 2:
        val = float(data)
        return (val, 0.0) if np.isfinite(val) else (np.nan, np.nan)

    # Build coordinate grids
    sub_lats = spatial[lat_coord].values
    sub_lons = spatial[lon_coord].values
    lon_grid, lat_grid = np.meshgrid(sub_lons, sub_lats)

    # Compute distances (km)
    distances = haversine_km(lon, lat, lon_grid, lat_grid)

    # Mask beyond radius and NaN data
    valid = (distances <= radius_km) & np.isfinite(data)
    if not valid.any():
        return np.nan, np.nan

    # Gaussian weights
    weights = np.exp(-distances**2 / (2 * sigma_km**2))
    weights[~valid] = 0.0
    w_sum = weights.sum()
    if w_sum == 0:
        return np.nan, np.nan

    # Weighted mean
    w_mean = np.nansum(weights * data) / w_sum

    # Weighted standard deviation
    w_var = np.nansum(weights * (data - w_mean) ** 2) / w_sum
    w_sd = np.sqrt(w_var)

    return float(w_mean), float(w_sd)


def _apply_derivation(ds: xr.Dataset, var_config: VariableConfig) -> xr.Dataset:
    """Apply a derived variable computation using the derivation registry."""
    if var_config.derivation and var_config.derivation in DERIVATION_REGISTRY:
        return DERIVATION_REGISTRY[var_config.derivation](ds)
    # Fallback for legacy configs without explicit derivation key
    if var_config.derived and var_config.short_name == "eke":
        return DERIVATION_REGISTRY["eke_from_geostrophic"](ds)
    raise ValueError(
        f"No derivation found for '{var_config.short_name}'. "
        f"Available: {list(DERIVATION_REGISTRY.keys())}"
    )


def extract_along_track(
    tracks: pd.DataFrame,
    variables: list[str],
    raw_dir: Path = DEFAULT_RAW_DIR,
    sigma_multiplier: float = DEFAULT_SIGMA_MULTIPLIER,
    var_registry: dict | None = None,
) -> pd.DataFrame:
    """Extract environmental variables along all track positions.

    For variables with per-animal-month files, opens the specific file for each
    group. Otherwise, opens all files as a single multi-file dataset.

    Args:
        tracks: DataFrame with id, date, lon, lat, se_x, se_y columns.
        variables: List of variable short names to extract.
        raw_dir: Directory containing raw NetCDF files.
        sigma_multiplier: Radius = multiplier * max(se_x, se_y).

    Returns:
        tracks DataFrame with added {var}_mean and {var}_sd columns.
    """
    if var_registry is None:
        var_registry = VARIABLE_REGISTRY
    result = tracks.copy()

    for var_name in variables:
        var_config = var_registry[var_name]
        logger.info(f"Extracting {var_name}...")

        use_month_only = _has_month_only_files(var_name, raw_dir)
        use_id_month = _has_id_month_files(var_name, raw_dir)

        if use_month_only:
            result = _extract_monthly(
                result, var_name, var_config, raw_dir, sigma_multiplier
            )
        elif use_id_month:
            result = _extract_id_month(
                result, var_name, var_config, raw_dir, sigma_multiplier
            )
        else:
            result = _extract_bulk(
                result, var_name, var_config, raw_dir, sigma_multiplier
            )

    return result


def _extract_bulk(
    result: pd.DataFrame,
    var_name: str,
    var_config: VariableConfig,
    raw_dir: Path,
    sigma_multiplier: float,
) -> pd.DataFrame:
    """Extract from bulk (yearly) NetCDF files."""
    ds = _open_env_datasets(var_name, raw_dir)
    if ds is None:
        result[f"{var_name}_mean"] = np.nan
        result[f"{var_name}_sd"] = np.nan
        return result

    # Compute derived variables
    if var_config.derived:
        ds = _apply_derivation(ds, var_config)

    lon_coord, lat_coord, time_coord = _identify_coord_names(ds)
    extract_var = var_config.extract_variable

    means = np.full(len(result), np.nan)
    sds = np.full(len(result), np.nan)

    for i, row in tqdm(
        result.iterrows(),
        total=len(result),
        desc=f"  {var_name}",
    ):
        sigma_km = max(row["se_x"], row["se_y"])
        radius_km = sigma_multiplier * sigma_km

        m, s = gaussian_weighted_extract(
            ds=ds,
            var_name=extract_var,
            lon=row["lon"],
            lat=row["lat"],
            date=row["date"],
            radius_km=radius_km,
            sigma_km=sigma_km,
            lon_coord=lon_coord,
            lat_coord=lat_coord,
            time_coord=time_coord,
        )
        idx = result.index.get_loc(i)
        means[idx] = m
        sds[idx] = s

    result[f"{var_name}_mean"] = means
    result[f"{var_name}_sd"] = sds

    ds.close()
    logger.info(
        f"  {var_name}: {np.isfinite(means).sum()}/{len(means)} positions extracted"
    )
    return result


def _extract_id_month(
    result: pd.DataFrame,
    var_name: str,
    var_config: VariableConfig,
    raw_dir: Path,
    sigma_multiplier: float,
) -> pd.DataFrame:
    """Extract from per-animal per-month NetCDF files."""
    extract_var = var_config.extract_variable
    means = np.full(len(result), np.nan)
    sds = np.full(len(result), np.nan)

    # Group positions by (animal, year-month)
    result_copy = result.copy()
    result_copy["_year_month"] = result_copy["date"].dt.to_period("M").astype(str)

    grouped = result_copy.groupby(["id", "_year_month"])
    total_groups = len(grouped)
    processed = 0

    for (animal_id, year_month), grp in tqdm(
        grouped, total=total_groups, desc=f"  {var_name} (id-month)"
    ):
        ds = _open_id_month_file(var_name, animal_id, year_month, raw_dir)
        if ds is None:
            logger.warning(
                f"No file for {var_name} id={animal_id} {year_month}, skipping"
            )
            continue

        try:
            lon_coord, lat_coord, time_coord = _identify_coord_names(ds)
        except ValueError:
            logger.warning(f"Could not identify coords in {var_name} {animal_id} {year_month}")
            ds.close()
            continue

        for i, row in grp.iterrows():
            sigma_km = max(row["se_x"], row["se_y"])
            radius_km = sigma_multiplier * sigma_km

            m, s = gaussian_weighted_extract(
                ds=ds,
                var_name=extract_var,
                lon=row["lon"],
                lat=row["lat"],
                date=row["date"],
                radius_km=radius_km,
                sigma_km=sigma_km,
                lon_coord=lon_coord,
                lat_coord=lat_coord,
                time_coord=time_coord,
            )
            idx = result.index.get_loc(i)
            means[idx] = m
            sds[idx] = s

        ds.close()
        processed += 1

    result[f"{var_name}_mean"] = means
    result[f"{var_name}_sd"] = sds

    logger.info(
        f"  {var_name}: {np.isfinite(means).sum()}/{len(means)} positions extracted "
        f"({processed}/{total_groups} id-month groups)"
    )
    return result


def _extract_monthly(
    result: pd.DataFrame,
    var_name: str,
    var_config: VariableConfig,
    raw_dir: Path,
    sigma_multiplier: float,
) -> pd.DataFrame:
    """Extract from per-month NetCDF files (merged across sharks)."""
    extract_var = var_config.extract_variable
    means = np.full(len(result), np.nan)
    sds = np.full(len(result), np.nan)

    # Group positions by year-month (across all sharks)
    result_copy = result.copy()
    result_copy["_year_month"] = result_copy["date"].dt.to_period("M").astype(str)

    grouped = result_copy.groupby("_year_month")
    total_groups = len(grouped)
    processed = 0

    for year_month, grp in tqdm(
        grouped, total=total_groups, desc=f"  {var_name} (monthly)"
    ):
        ds = _open_month_file(var_name, year_month, raw_dir)
        if ds is None:
            logger.warning(
                f"No file for {var_name} {year_month}, skipping"
            )
            continue

        try:
            lon_coord, lat_coord, time_coord = _identify_coord_names(ds)
        except ValueError:
            logger.warning(f"Could not identify coords in {var_name} {year_month}")
            ds.close()
            continue

        for i, row in grp.iterrows():
            sigma_km = max(row["se_x"], row["se_y"])
            radius_km = sigma_multiplier * sigma_km

            m, s = gaussian_weighted_extract(
                ds=ds,
                var_name=extract_var,
                lon=row["lon"],
                lat=row["lat"],
                date=row["date"],
                radius_km=radius_km,
                sigma_km=sigma_km,
                lon_coord=lon_coord,
                lat_coord=lat_coord,
                time_coord=time_coord,
            )
            idx = result.index.get_loc(i)
            means[idx] = m
            sds[idx] = s

        ds.close()
        processed += 1

    result[f"{var_name}_mean"] = means
    result[f"{var_name}_sd"] = sds

    logger.info(
        f"  {var_name}: {np.isfinite(means).sum()}/{len(means)} positions extracted "
        f"({processed}/{total_groups} monthly groups)"
    )
    return result
