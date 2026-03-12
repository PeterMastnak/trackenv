"""Copernicus Marine Service data downloads."""

import logging
from pathlib import Path

import copernicusmarine
import pandas as pd

from ..config import VariableConfig
from ..utils import split_bbox_at_dateline

logger = logging.getLogger(__name__)


def download_copernicus(
    var_config: VariableConfig,
    bbox: dict,
    year: int,
    output_dir: Path,
) -> list[Path]:
    """Download a variable from Copernicus Marine for a given year.

    Returns:
        List of downloaded file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

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
            f"Downloading {var_config.short_name} {year}{suffix} from Copernicus Marine "
            f"[{lon_min:.1f},{lon_max:.1f}] x [{bbox['lat_min']:.1f},{bbox['lat_max']:.1f}]"
        )

        subset_kwargs = dict(
            dataset_id=var_config.dataset_id,
            variables=var_config.variable_names,
            minimum_longitude=lon_min,
            maximum_longitude=lon_max,
            minimum_latitude=bbox["lat_min"],
            maximum_latitude=bbox["lat_max"],
            start_datetime=year_start.strftime("%Y-%m-%dT00:00:00"),
            end_datetime=year_end.strftime("%Y-%m-%dT23:59:59"),
            output_filename=str(out_file.name),
            output_directory=str(output_dir),
            force_download=True,
        )

        # Add depth constraint if specified
        if var_config.depth is not None:
            subset_kwargs["minimum_depth"] = var_config.depth
            subset_kwargs["maximum_depth"] = var_config.depth

        try:
            copernicusmarine.subset(**subset_kwargs)
            logger.info(f"Saved {out_file}")
            downloaded.append(out_file)
        except Exception as ex:
            logger.error(f"Failed to download {var_config.short_name} {year}: {ex}")
            raise

    return downloaded
