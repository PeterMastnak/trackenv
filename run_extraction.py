#!/usr/bin/env python3
"""CLI entry point for the environmental data extraction pipeline.

Author: Peter Mastnak

This script provides backward-compatible CLI access. For the full trackenv CLI
with subcommands (scan, init, extract, analyze, download), use:

    python -m env_extraction <command>

Or directly:

    trackenv <command>
"""

import argparse
import logging
import sys
from pathlib import Path

from env_extraction.pipeline import run_pipeline
from env_extraction.config import VARIABLE_REGISTRY
from env_extraction.project_config import load_project_config


def main():
    parser = argparse.ArgumentParser(
        description="Extract environmental data at animal tracking positions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Single animal, SST only:
  python run_extraction.py animotum/ --ids 170200201 --variables sst

  # All animals, all variables:
  python run_extraction.py animotum/

  # Use a custom ID CSV, skip downloads:
  python run_extraction.py animotum/ --id-csv my_ids.csv --skip-download

  # Use project.yaml configuration:
  python run_extraction.py --config project.yaml
""",
    )
    parser.add_argument(
        "track_dir",
        type=Path,
        nargs="?",
        default=None,
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
        help="CSV with ID column listing animal IDs to process",
    )
    parser.add_argument(
        "--ids",
        nargs="+",
        default=None,
        help="Manual list of animal IDs (overrides --id-csv)",
    )
    parser.add_argument(
        "--variables",
        nargs="+",
        default=None,
        help=f"Variables to extract (default: all). "
        f"Choices: {', '.join(VARIABLE_REGISTRY.keys())}",
    )
    parser.add_argument(
        "--pattern",
        default="*_rw_predicted.csv",
        help="Glob pattern for track CSV files (default: *_rw_predicted.csv)",
    )
    parser.add_argument(
        "--se-cap",
        type=float,
        default=200.0,
        help="Cap SE values at this value in km (default: 200)",
    )
    parser.add_argument(
        "--sigma-multiplier",
        type=float,
        default=2.0,
        help="Radius = multiplier * max(se_x, se_y) (default: 2.0)",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=50.0,
        help="Bounding box padding in km (default: 50)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Directory for raw NetCDF downloads (default: data/raw/)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for output CSV (default: data/output/)",
    )
    parser.add_argument(
        "--output-name",
        default=None,
        help="Explicit output filename prefix (e.g., 'blue_whale_study')",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip data download step (use cached files)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help="Max concurrent download threads (default: 3)",
    )
    parser.add_argument(
        "--download-strategy",
        choices=["auto", "monthly-shark", "yearly-bbox"],
        default="auto",
        help="Download chunking strategy",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load project config if provided
    project_config = None
    if args.config:
        project_config = load_project_config(args.config)

    # Apply download strategy overrides
    if args.download_strategy == "yearly-bbox":
        for vc in VARIABLE_REGISTRY.values():
            vc.stride = 1
            if vc.resolution_deg < 0.1:
                vc.resolution_deg = 0.25
    elif args.download_strategy == "monthly-shark":
        for vc in VARIABLE_REGISTRY.values():
            if vc.source == "erddap":
                vc.resolution_deg = 0.01

    # Determine project root
    if args.track_dir:
        project_root = args.track_dir.resolve().parent
    elif args.config:
        project_root = args.config.resolve().parent
    else:
        project_root = Path.cwd()

    try:
        out_file = run_pipeline(
            project_root=project_root,
            id_filter_csv=args.id_csv,
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


if __name__ == "__main__":
    main()
