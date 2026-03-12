"""YAML-based project configuration for trackenv."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .config import VariableConfig, VARIABLE_REGISTRY

logger = logging.getLogger(__name__)


@dataclass
class TrackFormat:
    """Column mapping for track CSV files."""
    id_col: str = "id"
    date_col: str = "date"
    lon_col: str = "lon"
    lat_col: str = "lat"
    se_x_col: Optional[str] = "x.se"
    se_y_col: Optional[str] = "y.se"
    location_class_col: Optional[str] = None
    fixed_se_km: float = 50.0
    extra_columns: list[str] = field(default_factory=list)
    id_from_filename: bool = True
    id_filename_separator: str = "_"
    id_filename_index: int = 0


@dataclass
class ProjectConfig:
    """Full project configuration, loaded from project.yaml."""
    project_name: str = "my_study"
    tracks_directory: str = "tracks/"
    tracks_file_pattern: str = "*.csv"
    track_format: TrackFormat = field(default_factory=TrackFormat)
    id_filter_csv: Optional[str] = None
    id_filter_col: str = "ID"
    se_cap_km: float = 200.0
    sigma_multiplier: float = 2.0
    padding_km: float = 50.0
    raw_dir: str = "data/raw"
    output_dir: str = "data/output"
    variables: dict[str, VariableConfig] = field(default_factory=dict)


def _parse_track_format(data: dict) -> TrackFormat:
    """Parse the tracks.format section of project.yaml."""
    fmt = data.get("format", {})
    return TrackFormat(
        id_col=fmt.get("id_col", "id"),
        date_col=fmt.get("date_col", "date"),
        lon_col=fmt.get("lon_col", "lon"),
        lat_col=fmt.get("lat_col", "lat"),
        se_x_col=fmt.get("se_x_col", "x.se"),
        se_y_col=fmt.get("se_y_col", "y.se"),
        location_class_col=fmt.get("location_class_col"),
        fixed_se_km=fmt.get("fixed_se_km", 50.0),
        extra_columns=fmt.get("extra_columns", []),
        id_from_filename=fmt.get("id_from_filename", True),
        id_filename_separator=fmt.get("id_filename_separator", "_"),
        id_filename_index=fmt.get("id_filename_index", 0),
    )


def _parse_variables(data: dict) -> dict[str, VariableConfig]:
    """Parse the variables section of project.yaml into VariableConfig objects."""
    variables = {}
    for name, vdata in data.items():
        variables[name] = VariableConfig(
            short_name=name,
            source=vdata.get("source", "erddap"),
            dataset_id=vdata.get("dataset_id", ""),
            variable_names=vdata.get("variable_names", []),
            extract_variable=vdata.get("extract_variable", ""),
            server=vdata.get("server", ""),
            depth=vdata.get("depth"),
            derived=vdata.get("derived", False),
            resolution_deg=vdata.get("resolution_deg", 0.25),
            stride=vdata.get("stride", 1),
            derivation=vdata.get("derivation"),
        )
    return variables


def load_project_config(yaml_path: Path) -> ProjectConfig:
    """Load project configuration from a YAML file.

    Args:
        yaml_path: Path to project.yaml.

    Returns:
        Populated ProjectConfig.
    """
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    if raw is None:
        return ProjectConfig()

    project = raw.get("project", {})
    tracks = raw.get("tracks", {})
    extraction = raw.get("extraction", {})
    paths = raw.get("paths", {})
    variables_raw = raw.get("variables", {})

    track_format = _parse_track_format(tracks)

    # Parse variables: use YAML overrides, fall back to built-in registry
    if variables_raw:
        variables = _parse_variables(variables_raw)
    else:
        variables = dict(VARIABLE_REGISTRY)

    config = ProjectConfig(
        project_name=project.get("name", "my_study"),
        tracks_directory=tracks.get("directory", "tracks/"),
        tracks_file_pattern=tracks.get("file_pattern", "*.csv"),
        track_format=track_format,
        id_filter_csv=tracks.get("id_filter_csv"),
        id_filter_col=tracks.get("id_filter_col", "ID"),
        se_cap_km=extraction.get("se_cap_km", 200.0),
        sigma_multiplier=extraction.get("sigma_multiplier", 2.0),
        padding_km=extraction.get("padding_km", 50.0),
        raw_dir=paths.get("raw_dir", "data/raw"),
        output_dir=paths.get("output_dir", "data/output"),
        variables=variables,
    )

    logger.info(f"Loaded project config from {yaml_path}: '{config.project_name}'")
    return config


def generate_starter_yaml() -> str:
    """Generate a starter project.yaml template string."""
    return """\
project:
  name: "my_study"

tracks:
  directory: "tracks/"
  file_pattern: "*.csv"
  format:
    id_col: "id"
    date_col: "date"
    lon_col: "lon"
    lat_col: "lat"
    se_x_col: null            # null → use fixed_se_km or location_class
    se_y_col: null            # null → use fixed_se_km or location_class
    location_class_col: null  # e.g., "lc" — derives SE from Argos LC table
    fixed_se_km: 50.0        # fallback when no SE or LC columns
    extra_columns: []         # additional columns to preserve (e.g., ["depth", "speed"])
    id_from_filename: false
    id_filename_separator: "_"
    id_filename_index: 0
  id_filter_csv: null         # optional CSV to filter IDs
  id_filter_col: "ID"

extraction:
  se_cap_km: 200.0
  sigma_multiplier: 2.0
  padding_km: 50.0

paths:
  raw_dir: "data/raw"
  output_dir: "data/output"

# variables:                  # uncomment to override built-in defaults
#   sst:
#     source: erddap
#     server: "https://coastwatch.pfeg.noaa.gov/erddap"
#     dataset_id: jplMURSST41
#     variable_names: [analysed_sst]
#     extract_variable: analysed_sst
#     resolution_deg: 0.01
#     stride: 5
"""
