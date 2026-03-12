#!/usr/bin/env python3
"""Pre-flight analysis tool for track data.

Author: Peter Mastnak

Loads tracks and prints a manifest of what would be downloaded,
without actually downloading anything. Works with any species/region.

Usage:
    python analyze_tracks.py animotum/ --ids 170200201
    python analyze_tracks.py animotum/
    python analyze_tracks.py --id-csv my_ids.csv animotum/
    python analyze_tracks.py --config project.yaml animotum/

For the full trackenv CLI, use:
    trackenv analyze animotum/
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from env_extraction.config import (
    VARIABLE_REGISTRY,
    DEFAULT_TRACK_PATTERN,
    DEFAULT_ID_FILTER_CSV,
    DEFAULT_PADDING_KM,
    DEFAULT_SE_CAP_KM,
)
from env_extraction.tracks import load_id_filter, load_tracks
from env_extraction.download.manager import compute_id_month_groups, compute_month_groups


def estimate_file_size_mb(
    lat_range: float,
    lon_range: float,
    n_days: int,
    resolution_deg: float,
    stride: int = 1,
    n_vars: int = 1,
    bytes_per_value: int = 4,
) -> float:
    """Estimate NetCDF file size in MB for a given bbox and time range."""
    eff_resolution = resolution_deg * stride
    n_lat = max(1, int(lat_range / eff_resolution))
    n_lon = max(1, int(lon_range / eff_resolution))
    n_values = n_lat * n_lon * n_days * n_vars
    return n_values * bytes_per_value / (1024 * 1024)


def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight analysis: summarize tracks and estimate download sizes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "track_dir",
        type=Path,
        help="Directory containing track CSV files",
    )
    parser.add_argument(
        "--config", "-c",
        type=Path,
        default=None,
        help="Path to project.yaml configuration file",
    )
    parser.add_argument(
        "--id-csv",
        type=Path,
        default=None,
        help="CSV with ID column listing animal IDs",
    )
    parser.add_argument(
        "--ids",
        nargs="+",
        default=None,
        help="Manual list of animal IDs",
    )
    parser.add_argument(
        "--pattern",
        default=DEFAULT_TRACK_PATTERN,
        help="Glob pattern for track CSV files",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=DEFAULT_PADDING_KM,
        help="Bounding box padding in km (default: 50)",
    )
    parser.add_argument(
        "--variables",
        nargs="+",
        default=None,
        help="Variables to estimate (default: all)",
    )

    args = parser.parse_args()

    # Load project config if provided
    project_config = None
    track_format = None
    if args.config:
        from env_extraction.project_config import load_project_config
        project_config = load_project_config(args.config)
        track_format = project_config.track_format

    # Resolve animal IDs
    animal_ids = args.ids
    if animal_ids is None:
        project_root = args.track_dir.resolve().parent
        csv_path = args.id_csv or (project_root / DEFAULT_ID_FILTER_CSV)
        if csv_path.exists():
            id_col = "ID"
            if project_config and project_config.id_filter_col:
                id_col = project_config.id_filter_col
            animal_ids = load_id_filter(csv_path, id_col=id_col)
        else:
            animal_ids = None  # Load all available

    # Load tracks
    try:
        tracks = load_tracks(
            args.track_dir.resolve(),
            args.pattern,
            animal_ids,
            DEFAULT_SE_CAP_KM,
            track_format=track_format,
        )
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    var_registry = VARIABLE_REGISTRY
    if project_config and project_config.variables:
        var_registry = project_config.variables
    variables = args.variables or list(var_registry.keys())

    # === Track Summary ===
    print("\n" + "=" * 70)
    print("TRACK SUMMARY")
    print("=" * 70)
    n_animals = tracks["id"].nunique()
    n_positions = len(tracks)
    date_min = tracks["date"].min()
    date_max = tracks["date"].max()
    print(f"  Animals:    {n_animals}")
    print(f"  Positions:  {n_positions}")
    print(f"  Date range: {date_min.date()} to {date_max.date()}")
    print(f"  Lat range:  {tracks['lat'].min():.2f} to {tracks['lat'].max():.2f}")
    print(f"  Lon range:  {tracks['lon'].min():.2f} to {tracks['lon'].max():.2f}")
    print(f"  SE stats:")
    print(f"    se_x: mean={tracks['se_x'].mean():.1f} km, "
          f"median={tracks['se_x'].median():.1f} km, "
          f"max={tracks['se_x'].max():.1f} km")
    print(f"    se_y: mean={tracks['se_y'].mean():.1f} km, "
          f"median={tracks['se_y'].median():.1f} km, "
          f"max={tracks['se_y'].max():.1f} km")

    # === Per-animal summary ===
    print(f"\n  Per-animal breakdown:")
    for animal_id, grp in tracks.groupby("id"):
        print(f"    {animal_id}: {len(grp)} positions, "
              f"{grp['date'].min().date()} to {grp['date'].max().date()}")

    # === ID-month groups ===
    id_month_groups = compute_id_month_groups(tracks, args.padding)
    n_id_months = len(id_month_groups)

    # === Merged month groups (union bbox across animals per month) ===
    month_groups = compute_month_groups(tracks, args.padding)
    n_months = len(month_groups)

    print(f"\n  ID-month groups: {n_id_months} (per-animal-month approach)")
    print(f"  Merged month groups: {n_months} (merged approach, "
          f"{n_id_months - n_months} fewer requests)")

    # === Download estimates per variable ===
    print("\n" + "=" * 70)
    print("DOWNLOAD ESTIMATES")
    print("=" * 70)

    for var_name in variables:
        vc = var_registry[var_name]
        print(f"\n  {var_name} ({vc.dataset_id})")
        print(f"    Resolution: {vc.resolution_deg}\u00b0 (stride={vc.stride}, "
              f"effective={vc.resolution_deg * vc.stride}\u00b0)")
        print(f"    Source: {vc.source}")

        uses_monthly = vc.resolution_deg < 0.1 or vc.stride > 1
        total_mb = 0.0

        if uses_monthly and vc.source == "erddap":
            print(f"    Strategy: merged per-month ({n_months} files, "
                  f"was {n_id_months} per-animal-month)")
            print()
            print(f"    {'Month':<10} {'Animals':>8} {'Positions':>10} "
                  f"{'Lat range':>12} {'Lon range':>12} {'Est. MB':>10}")
            print(f"    {'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*12} {'-'*10}")

            for ym in sorted(month_groups.keys()):
                bbox_m = month_groups[ym]
                lat_range = bbox_m["lat_max"] - bbox_m["lat_min"]
                lon_range = bbox_m["lon_max"] - bbox_m["lon_min"]
                if lon_range < 0:
                    lon_range += 360
                n_days = (bbox_m["date_max"] - bbox_m["date_min"]).days + 1

                mb = estimate_file_size_mb(
                    lat_range, lon_range, n_days,
                    vc.resolution_deg, vc.stride, len(vc.variable_names),
                )

                mask = tracks["date"].dt.to_period("M").astype(str) == ym
                month_positions = mask.sum()
                month_animals = tracks.loc[mask, "id"].nunique()

                print(f"    {ym:<10} {month_animals:>8} {month_positions:>10} "
                      f"{lat_range:>11.1f}\u00b0 {lon_range:>11.1f}\u00b0 "
                      f"{mb:>9.1f}")
                total_mb += mb

        else:
            lat_range = tracks["lat"].max() - tracks["lat"].min() + \
                2 * (args.padding / 111.0)
            lon_range = tracks["lon"].max() - tracks["lon"].min() + \
                2 * (args.padding / (111.0 * np.cos(np.radians(tracks["lat"].mean()))))
            n_years = date_max.year - date_min.year + 1
            total_days = (date_max - date_min).days + 1
            print(f"    Strategy: yearly bbox ({n_years} files)")

            total_mb = estimate_file_size_mb(
                lat_range, lon_range, total_days,
                vc.resolution_deg, vc.stride, len(vc.variable_names),
            )

        print(f"\n    TOTAL: {total_mb:.1f} MB ({total_mb / 1024:.2f} GB)")

    print("\n" + "=" * 70)
    print("No data was downloaded. Use run_extraction.py or 'trackenv extract' to download and extract.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
