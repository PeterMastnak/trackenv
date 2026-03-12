"""Unified CLI for trackenv — multi-species environmental extraction tool."""

import argparse
import logging
import sys
from pathlib import Path

from .config import VARIABLE_REGISTRY


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add arguments shared across subcommands."""
    parser.add_argument(
        "--config", "-c",
        type=Path,
        default=None,
        help="Path to project.yaml configuration file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )


def cmd_scan(args: argparse.Namespace) -> None:
    """Handle the 'scan' subcommand."""
    from .scan_tracks import scan_tracks, print_scan_report, write_scan_to_yaml

    result = scan_tracks(args.path, file_pattern=args.pattern)
    print_scan_report(result)

    if args.write:
        yaml_path = args.output or Path("project.yaml")
        write_scan_to_yaml(result, yaml_path)


def cmd_init(args: argparse.Namespace) -> None:
    """Handle the 'init' subcommand."""
    from .project_config import generate_starter_yaml

    out_path = args.output or Path("project.yaml")
    if out_path.exists() and not args.force:
        print(f"Error: {out_path} already exists. Use --force to overwrite.",
              file=sys.stderr)
        sys.exit(1)

    out_path.write_text(generate_starter_yaml())
    print(f"Created starter config: {out_path}")


def cmd_extract(args: argparse.Namespace) -> None:
    """Handle the 'extract' subcommand."""
    from .pipeline import run_pipeline
    from .project_config import load_project_config

    project_config = None
    if args.config:
        project_config = load_project_config(args.config)

    # Determine project root
    if args.track_dir:
        project_root = args.track_dir.resolve().parent
    elif args.config:
        project_root = args.config.resolve().parent
    else:
        project_root = Path.cwd()

    # Apply download strategy overrides
    var_registry = VARIABLE_REGISTRY
    if project_config and project_config.variables:
        var_registry = project_config.variables

    if args.download_strategy == "yearly-bbox":
        for vc in var_registry.values():
            vc.stride = 1
            if vc.resolution_deg < 0.1:
                vc.resolution_deg = 0.25
    elif args.download_strategy == "monthly-shark":
        for vc in var_registry.values():
            if vc.source == "erddap":
                vc.resolution_deg = 0.01

    try:
        out_file = run_pipeline(
            project_root=project_root,
            id_filter_csv=args.id_csv.resolve() if args.id_csv else None,
            animal_ids=args.ids,
            track_dir=args.track_dir.resolve() if args.track_dir else None,
            track_pattern=args.pattern,
            variables=args.variables,
            raw_dir=args.raw_dir.resolve() if args.raw_dir else None,
            output_dir=args.output_dir.resolve() if args.output_dir else None,
            se_cap_km=args.se_cap,
            sigma_multiplier=args.sigma_multiplier,
            padding_km=args.padding,
            skip_download=args.skip_download,
            max_workers=args.max_workers,
            project_config=project_config,
            output_name=args.output_name,
        )
        print(f"\nDone! Output: {out_file}")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


def cmd_analyze(args: argparse.Namespace) -> None:
    """Handle the 'analyze' subcommand."""
    import numpy as np
    import pandas as pd
    from .config import DEFAULT_TRACK_PATTERN, DEFAULT_ID_FILTER_CSV, DEFAULT_PADDING_KM, DEFAULT_SE_CAP_KM
    from .tracks import load_id_filter, load_tracks
    from .download.manager import compute_id_month_groups, compute_month_groups
    from .project_config import load_project_config

    project_config = None
    track_format = None
    if args.config:
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
            animal_ids = None

    pattern = args.pattern or DEFAULT_TRACK_PATTERN
    if project_config:
        pattern = project_config.tracks_file_pattern or pattern

    try:
        tracks = load_tracks(
            args.track_dir.resolve(),
            pattern,
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
    padding = args.padding or DEFAULT_PADDING_KM
    id_month_groups = compute_id_month_groups(tracks, padding)
    n_id_months = len(id_month_groups)

    # === Merged month groups ===
    month_groups = compute_month_groups(tracks, padding)
    n_months = len(month_groups)

    print(f"\n  ID-month groups: {n_id_months} (per-animal-month approach)")
    print(f"  Merged month groups: {n_months} (merged approach, "
          f"{n_id_months - n_months} fewer requests)")

    # === Download estimates per variable ===
    print("\n" + "=" * 70)
    print("DOWNLOAD ESTIMATES")
    print("=" * 70)

    def estimate_file_size_mb(lat_range, lon_range, n_days, resolution_deg,
                              stride=1, n_vars=1, bytes_per_value=4):
        eff_resolution = resolution_deg * stride
        n_lat = max(1, int(lat_range / eff_resolution))
        n_lon = max(1, int(lon_range / eff_resolution))
        n_values = n_lat * n_lon * n_days * n_vars
        return n_values * bytes_per_value / (1024 * 1024)

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
                2 * (padding / 111.0)
            lon_range = tracks["lon"].max() - tracks["lon"].min() + \
                2 * (padding / (111.0 * np.cos(np.radians(tracks["lat"].mean()))))
            n_years = date_max.year - date_min.year + 1
            total_days = (date_max - date_min).days + 1
            print(f"    Strategy: yearly bbox ({n_years} files)")

            total_mb = estimate_file_size_mb(
                lat_range, lon_range, total_days,
                vc.resolution_deg, vc.stride, len(vc.variable_names),
            )

        print(f"\n    TOTAL: {total_mb:.1f} MB ({total_mb / 1024:.2f} GB)")

    print("\n" + "=" * 70)
    print("No data was downloaded. Use 'trackenv extract' to download and extract.")
    print("=" * 70 + "\n")


def cmd_download(args: argparse.Namespace) -> None:
    """Handle the 'download' subcommand (download only, no extraction)."""
    from .config import DEFAULT_TRACK_PATTERN, DEFAULT_ID_FILTER_CSV, DEFAULT_SE_CAP_KM, DEFAULT_PADDING_KM
    from .tracks import load_id_filter, load_tracks, compute_bounding_box
    from .download.manager import download_all
    from .project_config import load_project_config

    project_config = None
    track_format = None
    if args.config:
        project_config = load_project_config(args.config)
        track_format = project_config.track_format

    # Resolve animal IDs
    animal_ids = args.ids
    if animal_ids is None and args.id_csv:
        id_col = "ID"
        if project_config and project_config.id_filter_col:
            id_col = project_config.id_filter_col
        animal_ids = load_id_filter(args.id_csv, id_col=id_col)

    pattern = args.pattern or DEFAULT_TRACK_PATTERN
    if project_config:
        pattern = project_config.tracks_file_pattern or pattern

    tracks = load_tracks(
        args.track_dir.resolve(), pattern, animal_ids, DEFAULT_SE_CAP_KM,
        track_format=track_format,
    )
    bbox = compute_bounding_box(tracks, args.padding or DEFAULT_PADDING_KM)

    var_registry = VARIABLE_REGISTRY
    if project_config and project_config.variables:
        var_registry = project_config.variables
    variables = args.variables or list(var_registry.keys())

    raw_dir = args.raw_dir or Path("data/raw")
    if project_config:
        raw_dir = Path(project_config.raw_dir)

    download_all(
        variables, bbox, raw_dir, tracks=tracks,
        padding_km=args.padding or DEFAULT_PADDING_KM,
        max_workers=args.max_workers,
        var_registry=var_registry,
    )
    print("Downloads complete.")


def main():
    parser = argparse.ArgumentParser(
        prog="trackenv",
        description="Multi-species environmental data extraction tool for animal tracking positions.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- scan ---
    p_scan = subparsers.add_parser(
        "scan",
        help="Auto-detect column mapping in track CSV files",
    )
    p_scan.add_argument("path", type=Path, help="Path to track CSV file or directory")
    p_scan.add_argument("--pattern", default="*.csv", help="File pattern (default: *.csv)")
    p_scan.add_argument("--write", action="store_true", help="Write detected mapping to project.yaml")
    p_scan.add_argument("--output", "-o", type=Path, default=None, help="Output YAML path (default: project.yaml)")
    _add_common_args(p_scan)

    # --- init ---
    p_init = subparsers.add_parser(
        "init",
        help="Generate a starter project.yaml template",
    )
    p_init.add_argument("--output", "-o", type=Path, default=None, help="Output path (default: project.yaml)")
    p_init.add_argument("--force", action="store_true", help="Overwrite existing file")
    _add_common_args(p_init)

    # --- extract ---
    p_extract = subparsers.add_parser(
        "extract",
        help="Run the full extraction pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # With project.yaml:
  trackenv extract --config project.yaml

  # Direct (legacy) usage:
  trackenv extract animotum/ --ids 170200201 --variables sst

  # All animals, all variables, skip downloads:
  trackenv extract animotum/ --skip-download
""",
    )
    p_extract.add_argument("track_dir", type=Path, nargs="?", default=None,
                           help="Directory containing track CSV files")
    p_extract.add_argument("--id-csv", type=Path, default=None,
                           help="CSV with ID column listing animal IDs to process")
    p_extract.add_argument("--ids", nargs="+", default=None,
                           help="Manual list of animal IDs (overrides --id-csv)")
    p_extract.add_argument("--variables", nargs="+", default=None,
                           help="Variables to extract (default: all)")
    p_extract.add_argument("--pattern", default="*_rw_predicted.csv",
                           help="Glob pattern for track CSV files")
    p_extract.add_argument("--se-cap", type=float, default=200.0,
                           help="Cap SE values at this value in km (default: 200)")
    p_extract.add_argument("--sigma-multiplier", type=float, default=2.0,
                           help="Radius = multiplier * max(se_x, se_y) (default: 2.0)")
    p_extract.add_argument("--padding", type=float, default=50.0,
                           help="Bounding box padding in km (default: 50)")
    p_extract.add_argument("--raw-dir", type=Path, default=None,
                           help="Directory for raw NetCDF downloads (default: data/raw/)")
    p_extract.add_argument("--output-dir", type=Path, default=None,
                           help="Directory for output CSV (default: data/output/)")
    p_extract.add_argument("--output-name", default=None,
                           help="Explicit output filename prefix (e.g., 'blue_whale_study')")
    p_extract.add_argument("--skip-download", action="store_true",
                           help="Skip data download step (use cached files)")
    p_extract.add_argument("--max-workers", type=int, default=3,
                           help="Max concurrent download threads (default: 3)")
    p_extract.add_argument("--download-strategy",
                           choices=["auto", "monthly-shark", "yearly-bbox"], default="auto",
                           help="Download chunking strategy")
    _add_common_args(p_extract)

    # --- analyze ---
    p_analyze = subparsers.add_parser(
        "analyze",
        help="Pre-flight analysis: summarize tracks and estimate download sizes",
    )
    p_analyze.add_argument("track_dir", type=Path,
                           help="Directory containing track CSV files")
    p_analyze.add_argument("--id-csv", type=Path, default=None,
                           help="CSV with ID column listing animal IDs")
    p_analyze.add_argument("--ids", nargs="+", default=None,
                           help="Manual list of animal IDs")
    p_analyze.add_argument("--pattern", default=None,
                           help="Glob pattern for track CSV files")
    p_analyze.add_argument("--padding", type=float, default=None,
                           help="Bounding box padding in km (default: 50)")
    p_analyze.add_argument("--variables", nargs="+", default=None,
                           help="Variables to estimate (default: all)")
    _add_common_args(p_analyze)

    # --- download ---
    p_download = subparsers.add_parser(
        "download",
        help="Download environmental data only (no extraction)",
    )
    p_download.add_argument("track_dir", type=Path,
                            help="Directory containing track CSV files")
    p_download.add_argument("--id-csv", type=Path, default=None,
                            help="CSV with ID column listing animal IDs")
    p_download.add_argument("--ids", nargs="+", default=None,
                            help="Manual list of animal IDs")
    p_download.add_argument("--variables", nargs="+", default=None,
                            help="Variables to download (default: all)")
    p_download.add_argument("--pattern", default=None,
                            help="Glob pattern for track CSV files")
    p_download.add_argument("--padding", type=float, default=None,
                            help="Bounding box padding in km (default: 50)")
    p_download.add_argument("--raw-dir", type=Path, default=None,
                            help="Directory for raw NetCDF downloads")
    p_download.add_argument("--max-workers", type=int, default=3,
                            help="Max concurrent download threads (default: 3)")
    _add_common_args(p_download)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Dispatch
    commands = {
        "scan": cmd_scan,
        "init": cmd_init,
        "extract": cmd_extract,
        "analyze": cmd_analyze,
        "download": cmd_download,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
