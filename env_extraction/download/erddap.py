"""ERDDAP data downloads via erddapy."""

import logging
import socket
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Set a default socket timeout so urllib (used internally by erddapy/pandas)
# doesn't hang indefinitely on slow ERDDAP responses.
socket.setdefaulttimeout(300)  # 5 minutes

import httpx
import pandas as pd
from erddapy import ERDDAP

from ..config import VariableConfig, ERDDAP_SERVER
from ..utils import split_bbox_at_dateline

logger = logging.getLogger(__name__)

# Generous timeout for large ERDDAP requests (10 minutes)
DOWNLOAD_TIMEOUT = httpx.Timeout(10.0, read=600.0)
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 60


def _lon_to_360(lon: float) -> float:
    """Convert longitude from [-180, 180] to [0, 360] range."""
    return lon % 360


def _init_erddap(server: str, dataset_id: str) -> ERDDAP:
    """Initialize ERDDAP with retries for HTTP errors (408, 503, etc.)."""
    for attempt in range(MAX_RETRIES):
        try:
            e = ERDDAP(server=server, protocol="griddap")
            e.dataset_id = dataset_id
            return e
        except (httpx.HTTPError, urllib.error.HTTPError, ConnectionError, TimeoutError, OSError) as ex:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_WAIT_SECONDS * (attempt + 1)
                logger.warning(
                    f"ERDDAP error on init (attempt {attempt + 1}/{MAX_RETRIES}): {ex}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                raise


def _download_nc(url: str, out_file: Path) -> None:
    """Download a NetCDF file from a URL with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            with httpx.stream("GET", url, timeout=DOWNLOAD_TIMEOUT) as response:
                response.raise_for_status()
                with open(out_file, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
            return  # Success
        except (httpx.HTTPStatusError, httpx.ReadTimeout, httpx.ConnectError,
                httpx.RemoteProtocolError, httpx.ConnectTimeout,
                ConnectionError, TimeoutError, OSError) as ex:
            if out_file.exists():
                out_file.unlink()
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_WAIT_SECONDS * (attempt + 1)
                logger.warning(
                    f"Download failed (attempt {attempt + 1}/{MAX_RETRIES}): {ex}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts: {ex}")
                raise


def _fix_360_longitude(out_file: Path) -> None:
    """Convert longitude from 0-360 to -180..180 in a NetCDF file."""
    import xarray as xr
    ds = xr.open_dataset(out_file)
    if "longitude" in ds.coords:
        lon_vals = ds["longitude"].values
        if lon_vals.max() > 180:
            lon_vals = ((lon_vals + 180) % 360) - 180
            ds = ds.assign_coords(longitude=lon_vals)
            ds = ds.sortby("longitude")
            ds.to_netcdf(out_file)
    ds.close()


def _set_constraints(
    e: ERDDAP,
    var_config: VariableConfig,
    time_min: pd.Timestamp,
    time_max: pd.Timestamp,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
) -> None:
    """Set constraints on an ERDDAP object, including stride."""
    # Detect if the dataset uses 0-360 longitude convention
    init_lon_max = e.constraints.get("longitude<=", 0)
    uses_360 = init_lon_max > 180

    if uses_360:
        eff_lon_min = _lon_to_360(lon_min)
        eff_lon_max = _lon_to_360(lon_max)
    else:
        eff_lon_min = lon_min
        eff_lon_max = lon_max

    e.constraints["time>="] = time_min.strftime("%Y-%m-%dT00:00:00Z")
    e.constraints["time<="] = time_max.strftime("%Y-%m-%dT00:00:00Z")
    e.constraints["latitude>="] = lat_min
    e.constraints["latitude<="] = lat_max
    e.constraints["longitude>="] = eff_lon_min
    e.constraints["longitude<="] = eff_lon_max

    # Apply stride for spatial subsampling
    if var_config.stride > 1:
        e.constraints["latitude_step"] = var_config.stride
        e.constraints["longitude_step"] = var_config.stride

    # Handle zlev dimension if present
    if "zlev>=" in e.constraints:
        e.constraints["zlev>="] = 0.0
        e.constraints["zlev<="] = 0.0

    e.variables = var_config.variable_names


def download_erddap(
    var_config: VariableConfig,
    bbox: dict,
    year: int,
    output_dir: Path,
) -> list[Path]:
    """Download a variable from ERDDAP for a given year (bulk bbox).

    Handles dateline crossing by splitting into two requests.

    Returns:
        List of downloaded file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Date range for this year, clipped to bbox dates
    year_start = max(pd.Timestamp(f"{year}-01-01"), bbox["date_min"])
    year_end = min(pd.Timestamp(f"{year}-12-31"), bbox["date_max"])
    if year_start > year_end:
        return []

    lon_ranges = split_bbox_at_dateline(bbox["lon_min"], bbox["lon_max"]) \
        if bbox["crosses_dateline"] else [(bbox["lon_min"], bbox["lon_max"])]

    downloaded = []
    for i, (lon_min, lon_max) in enumerate(lon_ranges):
        suffix = f"_part{i}" if len(lon_ranges) > 1 else ""
        out_file = output_dir / f"{var_config.short_name}_{year}{suffix}.nc"

        if out_file.exists():
            logger.info(f"Cache hit: {out_file}")
            downloaded.append(out_file)
            continue

        logger.info(
            f"Downloading {var_config.short_name} {year}{suffix} from ERDDAP "
            f"[{lon_min:.1f},{lon_max:.1f}] x [{bbox['lat_min']:.1f},{bbox['lat_max']:.1f}]"
        )

        e = _init_erddap(var_config.server, var_config.dataset_id)
        _set_constraints(
            e, var_config,
            year_start, year_end,
            bbox["lat_min"], bbox["lat_max"],
            lon_min, lon_max,
        )

        url = e.get_download_url(response="nc")
        logger.debug(f"Download URL: {url}")
        _download_nc(url, out_file)

        # Convert longitude back to -180..180 if dataset used 0-360
        init_lon_max = e.constraints.get("longitude<=", 0)
        if init_lon_max > 180 or _lon_to_360(lon_min) != lon_min:
            _fix_360_longitude(out_file)

        logger.info(f"Saved {out_file}")
        downloaded.append(out_file)

    return downloaded


def download_erddap_monthly(
    var_config: VariableConfig,
    id_month_groups: dict[tuple[str, str], dict],
    output_dir: Path,
) -> list[Path]:
    """Download per-animal per-month NetCDF files from ERDDAP.

    Args:
        var_config: Variable configuration.
        id_month_groups: Dict mapping (animal_id, 'YYYY-MM') to bbox dict with
            keys: lat_min, lat_max, lon_min, lon_max, date_min, date_max, crosses_dateline
        output_dir: Directory for output files.

    Returns:
        List of all downloaded file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for (animal_id, year_month), group_bbox in id_month_groups.items():
        lon_ranges = split_bbox_at_dateline(
            group_bbox["lon_min"], group_bbox["lon_max"]
        ) if group_bbox["crosses_dateline"] else [
            (group_bbox["lon_min"], group_bbox["lon_max"])
        ]

        for i, (lon_min, lon_max) in enumerate(lon_ranges):
            suffix = f"_part{i}" if len(lon_ranges) > 1 else ""
            out_file = output_dir / (
                f"{var_config.short_name}_{animal_id}_{year_month}{suffix}.nc"
            )

            if out_file.exists():
                logger.info(f"Cache hit: {out_file}")
                downloaded.append(out_file)
                continue

            logger.info(
                f"Downloading {var_config.short_name} id={animal_id} "
                f"{year_month}{suffix} "
                f"[{lon_min:.2f},{lon_max:.2f}] x "
                f"[{group_bbox['lat_min']:.2f},{group_bbox['lat_max']:.2f}]"
            )

            e = _init_erddap(var_config.server, var_config.dataset_id)
            _set_constraints(
                e, var_config,
                group_bbox["date_min"], group_bbox["date_max"],
                group_bbox["lat_min"], group_bbox["lat_max"],
                lon_min, lon_max,
            )

            url = e.get_download_url(response="nc")
            logger.debug(f"Download URL: {url}")
            _download_nc(url, out_file)
            _fix_360_longitude(out_file)

            logger.info(f"Saved {out_file}")
            downloaded.append(out_file)

    return downloaded


def _download_single_month(
    var_config: VariableConfig,
    year_month: str,
    group_bbox: dict,
    output_dir: Path,
) -> list[Path]:
    """Download a single month's NetCDF file from ERDDAP (union bbox across all animals).

    Returns:
        List of downloaded file paths for this month (usually 1, or 2 if dateline split).
    """
    lon_ranges = split_bbox_at_dateline(
        group_bbox["lon_min"], group_bbox["lon_max"]
    ) if group_bbox["crosses_dateline"] else [
        (group_bbox["lon_min"], group_bbox["lon_max"])
    ]

    downloaded = []
    for i, (lon_min, lon_max) in enumerate(lon_ranges):
        suffix = f"_part{i}" if len(lon_ranges) > 1 else ""
        out_file = output_dir / f"{var_config.short_name}_{year_month}{suffix}.nc"

        if out_file.exists():
            logger.info(f"Cache hit: {out_file}")
            downloaded.append(out_file)
            continue

        logger.info(
            f"Downloading {var_config.short_name} {year_month}{suffix} "
            f"[{lon_min:.2f},{lon_max:.2f}] x "
            f"[{group_bbox['lat_min']:.2f},{group_bbox['lat_max']:.2f}]"
        )

        e = _init_erddap(var_config.server, var_config.dataset_id)
        _set_constraints(
            e, var_config,
            group_bbox["date_min"], group_bbox["date_max"],
            group_bbox["lat_min"], group_bbox["lat_max"],
            lon_min, lon_max,
        )

        url = e.get_download_url(response="nc")
        logger.debug(f"Download URL: {url}")
        _download_nc(url, out_file)
        _fix_360_longitude(out_file)

        logger.info(f"Saved {out_file}")
        downloaded.append(out_file)

    return downloaded


def download_erddap_monthly_parallel(
    var_config: VariableConfig,
    month_groups: dict[str, dict],
    output_dir: Path,
    max_workers: int = 3,
) -> list[Path]:
    """Download per-month NetCDF files from ERDDAP with parallelism.

    Each month has a union bbox covering all animals active that month.

    Args:
        var_config: Variable configuration.
        month_groups: Dict mapping 'YYYY-MM' to bbox dict (from compute_month_groups).
        output_dir: Directory for output files.
        max_workers: Max concurrent download threads.

    Returns:
        List of all downloaded file paths, sorted by name.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if max_workers <= 1:
        # Sequential fallback
        all_files = []
        for year_month, group_bbox in sorted(month_groups.items()):
            files = _download_single_month(
                var_config, year_month, group_bbox, output_dir
            )
            all_files.extend(files)
        return all_files

    all_files = []
    failed_months = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _download_single_month,
                var_config, year_month, group_bbox, output_dir,
            ): year_month
            for year_month, group_bbox in month_groups.items()
        }
        for future in as_completed(futures):
            year_month = futures[future]
            try:
                files = future.result()
                all_files.extend(files)
            except Exception as ex:
                logger.error(
                    f"Failed to download {var_config.short_name} {year_month} "
                    f"after retries: {ex}. Skipping."
                )
                failed_months.append(year_month)

    if failed_months:
        logger.warning(
            f"{var_config.short_name}: {len(failed_months)} months failed: "
            f"{sorted(failed_months)}. Re-run to retry."
        )

    all_files.sort(key=lambda p: p.name)
    return all_files
