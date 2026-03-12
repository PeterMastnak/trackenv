"""End-to-end extraction pipeline orchestration."""

import logging
from pathlib import Path

import pandas as pd

from .config import (
    VARIABLE_REGISTRY,
    DEFAULT_RAW_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_TRACK_DIR,
    DEFAULT_TRACK_PATTERN,
    DEFAULT_ID_FILTER_CSV,
    DEFAULT_SE_CAP_KM,
    DEFAULT_SIGMA_MULTIPLIER,
    DEFAULT_PADDING_KM,
)
from .tracks import load_id_filter, load_tracks, compute_bounding_box
from .download.manager import download_all
from .extract import extract_along_track

logger = logging.getLogger(__name__)


def run_pipeline(
    project_root: Path,
    id_filter_csv: Path | None = None,
    animal_ids: list[str] | None = None,
    track_dir: Path | None = None,
    track_pattern: str = DEFAULT_TRACK_PATTERN,
    variables: list[str] | None = None,
    raw_dir: Path | None = None,
    output_dir: Path | None = None,
    se_cap_km: float = DEFAULT_SE_CAP_KM,
    sigma_multiplier: float = DEFAULT_SIGMA_MULTIPLIER,
    padding_km: float = DEFAULT_PADDING_KM,
    skip_download: bool = False,
    max_workers: int = 3,
    project_config: "ProjectConfig | None" = None,
    output_name: str | None = None,
) -> Path:
    """Run the full environmental extraction pipeline.

    Args:
        project_root: Project root directory.
        id_filter_csv: Path to CSV with animal IDs (has 'ID' column).
        animal_ids: Manual list of animal IDs (overrides id_filter_csv).
        track_dir: Directory with track CSV files.
        track_pattern: Glob pattern for track files.
        variables: Variables to extract. None = all.
        raw_dir: Directory for raw NetCDF downloads.
        output_dir: Directory for output CSV.
        se_cap_km: Cap for SE values in km.
        sigma_multiplier: Radius = multiplier * sigma.
        skip_download: Skip download step (use cached data).
        padding_km: Padding for bounding box in km.
        project_config: Optional ProjectConfig from YAML.
        output_name: Optional explicit output filename (without extension).

    Returns:
        Path to output CSV.
    """
    # Apply project config overrides if provided
    track_format = None
    if project_config is not None:
        track_dir = track_dir or (project_root / project_config.tracks_directory)
        track_pattern = project_config.tracks_file_pattern or track_pattern
        raw_dir = raw_dir or (project_root / project_config.raw_dir)
        output_dir = output_dir or (project_root / project_config.output_dir)
        se_cap_km = project_config.se_cap_km or se_cap_km
        sigma_multiplier = project_config.sigma_multiplier or sigma_multiplier
        padding_km = project_config.padding_km or padding_km
        track_format = project_config.track_format
        if project_config.id_filter_csv:
            id_filter_csv = id_filter_csv or (project_root / project_config.id_filter_csv)
        if output_name is None and project_config.project_name:
            output_name = project_config.project_name

    # Resolve paths
    track_dir = track_dir or (project_root / DEFAULT_TRACK_DIR)
    raw_dir = raw_dir or (project_root / DEFAULT_RAW_DIR)
    output_dir = output_dir or (project_root / DEFAULT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve animal IDs
    if animal_ids is None:
        csv_path = id_filter_csv or (project_root / DEFAULT_ID_FILTER_CSV)
        if csv_path.exists():
            id_col = "ID"
            if project_config and project_config.id_filter_col:
                id_col = project_config.id_filter_col
            animal_ids = load_id_filter(csv_path, id_col=id_col)
        else:
            animal_ids = None  # Load all available tracks

    # Resolve variable registry (YAML overrides if present)
    var_registry = VARIABLE_REGISTRY
    if project_config is not None and project_config.variables:
        var_registry = project_config.variables

    # All variables if none specified
    if variables is None:
        variables = list(var_registry.keys())

    # Validate variables
    for v in variables:
        if v not in var_registry:
            raise ValueError(
                f"Unknown variable '{v}'. Available: {list(var_registry.keys())}"
            )

    # Step 1: Load tracks
    logger.info("=" * 60)
    logger.info("Step 1: Loading tracks")
    logger.info("=" * 60)
    tracks = load_tracks(track_dir, track_pattern, animal_ids, se_cap_km, track_format)

    # Step 2: Compute bounding box
    bbox = compute_bounding_box(tracks, padding_km)

    # Step 3: Download data
    if not skip_download:
        logger.info("=" * 60)
        logger.info("Step 2: Downloading environmental data")
        logger.info("=" * 60)
        download_all(
            variables, bbox, raw_dir, tracks=tracks,
            padding_km=padding_km, max_workers=max_workers,
            var_registry=var_registry,
        )
    else:
        logger.info("Skipping downloads (--skip-download)")

    # Step 4: Extract along track
    logger.info("=" * 60)
    logger.info("Step 3: Extracting environmental data along tracks")
    logger.info("=" * 60)
    result = extract_along_track(
        tracks, variables, raw_dir, sigma_multiplier, var_registry=var_registry,
    )

    # Step 5: Save output
    if output_name:
        out_file = output_dir / f"{output_name}_env_extract.csv"
    else:
        ids_str = "_".join(sorted(set(str(i) for i in (animal_ids or [])))[:3])
        if animal_ids and len(animal_ids) > 3:
            ids_str += f"_and{len(animal_ids)-3}more"
        out_file = output_dir / f"env_extract_{ids_str or 'all'}.csv"
    result.to_csv(out_file, index=False)
    logger.info(f"Output saved to {out_file}")
    logger.info(f"Shape: {result.shape}")

    # Summary statistics
    for v in variables:
        mean_col = f"{v}_mean"
        if mean_col in result.columns:
            valid = result[mean_col].notna().sum()
            logger.info(f"  {v}: {valid}/{len(result)} valid extractions")

    return out_file
