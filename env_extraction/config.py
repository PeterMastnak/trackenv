"""Variable registry, paths, and configuration."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class VariableConfig:
    """Configuration for a single environmental variable."""
    short_name: str          # e.g. "sst"
    source: str              # "erddap" or "copernicus"
    dataset_id: str
    variable_names: list[str]  # NetCDF variable name(s) to download
    extract_variable: str    # Variable used in extraction (may be derived)
    server: str = ""
    depth: Optional[float] = None  # For 3D datasets, depth level to select
    derived: bool = False    # True if variable must be computed (e.g. EKE)
    resolution_deg: float = 0.25
    stride: int = 1          # Subsample stride for high-res datasets
    derivation: str | None = None  # Key into DERIVATION_REGISTRY (e.g. "eke_from_geostrophic")


# ERDDAP server
ERDDAP_SERVER = "https://coastwatch.pfeg.noaa.gov/erddap"

# Variable registry
VARIABLE_REGISTRY: dict[str, VariableConfig] = {
    "sst": VariableConfig(
        short_name="sst",
        source="erddap",
        dataset_id="jplMURSST41",
        variable_names=["analysed_sst"],
        extract_variable="analysed_sst",
        server=ERDDAP_SERVER,
        resolution_deg=0.01,
        stride=5,
    ),
    "ssta": VariableConfig(
        short_name="ssta",
        source="erddap",
        dataset_id="jplMURSST41anom1day",
        variable_names=["sstAnom"],
        extract_variable="sstAnom",
        server=ERDDAP_SERVER,
        resolution_deg=0.01,
        stride=5,
    ),
    "chl": VariableConfig(
        short_name="chl",
        source="erddap",
        dataset_id="erdMH1chla8day",
        variable_names=["chlorophyll"],
        extract_variable="chlorophyll",
        server=ERDDAP_SERVER,
        resolution_deg=0.04,
    ),
    "sal": VariableConfig(
        short_name="sal",
        source="copernicus",
        dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
        variable_names=["so"],
        extract_variable="so",
        depth=0.49402499198913574,
        resolution_deg=0.083,
    ),
    "ssh": VariableConfig(
        short_name="ssh",
        source="copernicus",
        dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
        variable_names=["zos"],
        extract_variable="zos",
        resolution_deg=0.083,
    ),
    "eke": VariableConfig(
        short_name="eke",
        source="copernicus",
        dataset_id="cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D",
        variable_names=["ugosa", "vgosa"],
        extract_variable="eke",
        derived=True,
        resolution_deg=0.125,
        derivation="eke_from_geostrophic",
    ),
}

# Default paths (relative to project root)
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_OUTPUT_DIR = Path("data/output")
DEFAULT_TRACK_DIR = Path("tracks")
DEFAULT_TRACK_PATTERN = "*.csv"
DEFAULT_ID_FILTER_CSV = Path("id_filter.csv")

# Extraction parameters
DEFAULT_SE_CAP_KM = 200.0
DEFAULT_SIGMA_MULTIPLIER = 2.0  # radius = multiplier * max(x.se, y.se)
DEFAULT_PADDING_KM = 50.0
