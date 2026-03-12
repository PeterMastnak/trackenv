# trackenv

Environmental data extraction pipeline for animal tracking positions. Given a set of satellite-tagged animal tracks (latitude, longitude, timestamp), this tool downloads gridded oceanographic datasets and extracts spatially-weighted environmental values at each observed position.

## Methodology

### Overview

The pipeline operates in three stages:

1. **Track loading and normalization** — Read CSV track files, map columns to a canonical schema (`id`, `date`, `lon`, `lat`, `se_x`, `se_y`), filter by animal ID, cap standard errors, and normalize longitudes to [-180, 180].
2. **Environmental data download** — Fetch gridded NetCDF datasets from ERDDAP and Copernicus Marine Service servers, chunked by time (yearly or monthly) and spatially bounded to the track extent plus configurable padding.
3. **Gaussian-weighted extraction** — For each track position, select a spatial neighborhood from the environmental grid, compute haversine distances from the position to every grid cell, apply a Gaussian kernel, and return the weighted mean and weighted standard deviation.

### Track Loading (`env_extraction/tracks.py`)

Track CSV files are loaded from a directory matching a glob pattern (default: `*_rw_predicted.csv`). Column mapping is controlled by a `TrackFormat` dataclass, which can be set explicitly in `project.yaml` or auto-detected by the `scan` command.

**Column resolution order for standard error (SE):**

1. **Explicit SE columns** (`se_x_col`, `se_y_col`) — If the track CSV contains columns with per-position spatial uncertainty estimates in kilometers (e.g., `x.se` and `y.se` from `aniMotum` SSM output), these are used directly.
2. **Argos location class** (`location_class_col`) — If an Argos LC column is present (e.g., `lc`), SE values are derived from a hardcoded lookup table mapping each class to a standard error in km:
   - `3` → 0.25 km, `2` → 0.5 km, `1` → 1.5 km, `0` → 5.0 km
   - `A` → 10.0 km, `B` → 20.0 km, `Z` → 50.0 km
3. **Fixed fallback** (`fixed_se_km`) — If neither SE columns nor location class are available, a constant SE (default: 50.0 km) is assigned to all positions.

**SE capping:** SE values exceeding `se_cap_km` (default: 200.0 km) are clipped to that threshold. A boolean `se_capped` column flags affected rows.

**Animal ID resolution:** IDs can come from a column within the CSV (`id_col`), or be extracted from the filename by splitting on a separator character (e.g., `170200201_rw_predicted.csv` → ID `170200201`). An optional ID filter CSV or explicit `--ids` list restricts which animals are loaded.

**Longitude normalization:** All longitudes are normalized to the [-180, 180] range via `((lon + 180) % 360) - 180`.

### Bounding Box Computation (`env_extraction/tracks.py`)

A global bounding box is computed from all loaded positions with configurable padding (default: 50 km). Padding is converted from kilometers to degrees using:

- Latitude: `pad_deg = pad_km / 111.0`
- Longitude: `pad_deg = pad_km / (111.0 * cos(lat_midpoint))`, where `lat_midpoint` is the mean of the latitude extent.

**Dateline handling:** The pipeline detects International Date Line crossings by sorting all longitudes and finding the largest gap. If the largest gap exceeds 180 degrees, the data is treated as wrapping across the dateline. The bounding box is then split into two sub-boxes at +/-180 degrees, and separate download requests are issued for each half.

### Environmental Data Download

Downloads are organized by variable. Each variable is defined by a `VariableConfig` specifying:

| Field | Description |
|---|---|
| `source` | `"erddap"` or `"copernicus"` |
| `dataset_id` | Remote dataset identifier |
| `variable_names` | NetCDF variable name(s) to fetch (multiple for derived variables) |
| `extract_variable` | The variable name used during extraction (may differ if derived) |
| `resolution_deg` | Native grid resolution in degrees |
| `stride` | Spatial subsampling factor (every Nth grid point) |
| `depth` | Depth level in meters for 3D datasets (e.g., 0.494 m for surface salinity) |
| `derived` | Whether the extraction variable must be computed post-download |
| `derivation` | Key into `DERIVATION_REGISTRY` (e.g., `"eke_from_geostrophic"`) |

#### Built-in Variable Registry (`env_extraction/config.py`)

Six variables are pre-configured:

| Short name | Dataset | Source | Resolution | Notes |
|---|---|---|---|---|
| `sst` | `jplMURSST41` | ERDDAP (CoastWatch) | 0.01 deg (stride 5 → effective 0.05 deg) | Multi-scale Ultra-high Resolution SST |
| `ssta` | `jplMURSST41anom1day` | ERDDAP (CoastWatch) | 0.01 deg (stride 5 → effective 0.05 deg) | SST anomaly |
| `chl` | `erdMH1chla8day` | ERDDAP (CoastWatch) | 0.04 deg | MODIS chlorophyll-a, 8-day composite |
| `sal` | `cmems_mod_glo_phy_my_0.083deg_P1D-m` | Copernicus Marine | 0.083 deg | Surface salinity (`so`) at 0.494 m depth |
| `ssh` | `cmems_mod_glo_phy_my_0.083deg_P1D-m` | Copernicus Marine | 0.083 deg | Sea surface height (`zos`) |
| `eke` | `cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D` | Copernicus Marine | 0.125 deg | Eddy Kinetic Energy, derived from `ugosa` and `vgosa` |

Variables can be overridden or extended in `project.yaml` under the `variables:` section.

#### Download Strategies (`env_extraction/download/manager.py`)

The download strategy is chosen automatically per variable based on resolution and source:

**Merged monthly downloads** (ERDDAP, high-resolution variables where `resolution_deg < 0.1` or `stride > 1`):
- Track positions are grouped by calendar month across all animals.
- For each month, a union bounding box is computed that covers every animal active in that month, plus padding.
- One NetCDF file is downloaded per month: `{var}_{YYYY-MM}.nc`.
- Downloads run in parallel across months (configurable `max_workers`, default 3).
- This strategy avoids downloading redundant overlapping regions when multiple animals occupy similar areas in the same month.

**Yearly bounding-box downloads** (coarse-resolution variables, or Copernicus sources):
- The full global bounding box (across all animals and all time) is used.
- One NetCDF file is downloaded per year: `{var}_{YYYY}.nc`.
- Copernicus downloads run in parallel across years.

**Caching:** If a target NetCDF file already exists on disk, the download is skipped (cache hit). Re-running the pipeline after a partial failure resumes from where it left off.

**Retry logic (ERDDAP):** Failed HTTP requests are retried up to 3 times with exponential backoff (60s, 120s, 180s). A 5-minute socket timeout and 10-minute read timeout are set to handle slow ERDDAP responses.

#### ERDDAP Downloads (`env_extraction/download/erddap.py`)

Uses the `erddapy` library to construct griddap URLs. The ERDDAP server defaults to `https://coastwatch.pfeg.noaa.gov/erddap`. Constraints are set on time, latitude, longitude, and optionally zlev (depth). Spatial stride is applied via `latitude_step` and `longitude_step` constraints.

If the dataset uses 0-360 longitude convention, coordinates are automatically converted. After download, longitudes in the NetCDF file are normalized back to [-180, 180] and re-sorted.

Downloads are streamed to disk using `httpx` in chunked mode (8 KB chunks).

#### Copernicus Downloads (`env_extraction/download/copernicus.py`)

Uses the `copernicusmarine` Python package (`copernicusmarine.subset()`). Depth constraints are applied when `var_config.depth` is set. Dateline-crossing bounding boxes are split into two subset requests.

### Gaussian-Weighted Extraction (`env_extraction/extract.py`)

For each track position, extraction proceeds as follows:

1. **Determine extraction radius.** The radius in kilometers is `sigma_multiplier * max(se_x, se_y)`, where `sigma_multiplier` defaults to 2.0. This means the extraction window extends to 2 standard deviations of the position's spatial uncertainty.

2. **Convert radius to degrees.** Latitude radius: `r_km / 111.0`. Longitude radius: `r_km / (111.0 * cos(lat))`.

3. **Temporal selection.** Select the nearest time step in the gridded dataset to the track position's timestamp using `xr.Dataset.sel(method="nearest")`.

4. **Spatial slicing.** Slice the dataset to `[lat - r_lat, lat + r_lat]` x `[lon - r_lon, lon + r_lon]`. If the slice yields zero cells in either dimension (radius smaller than grid spacing), fall back to the single nearest grid cell.

5. **Dimension handling.** Any extra dimensions beyond latitude and longitude (e.g., depth levels) are collapsed by selecting the first index.

6. **Distance computation.** Compute haversine distances (km) from the track position to the center of every grid cell in the spatial neighborhood:

   ```
   a = sin(dlat/2)^2 + cos(lat1) * cos(lat2) * sin(dlon/2)^2
   distance = 6371.0 * 2 * arcsin(sqrt(a))
   ```

7. **Validity mask.** A cell is valid if its distance is within the radius AND its data value is finite (not NaN).

8. **Gaussian weighting.** Weights are computed as:

   ```
   w_i = exp(-d_i^2 / (2 * sigma^2))
   ```

   where `d_i` is the haversine distance in km and `sigma` is `max(se_x, se_y)` in km. Weights for invalid cells are set to zero.

9. **Weighted statistics.**
   - Weighted mean: `sum(w_i * x_i) / sum(w_i)`
   - Weighted standard deviation: `sqrt(sum(w_i * (x_i - mean)^2) / sum(w_i))`

10. **Output columns.** For each variable `{var}`, two columns are appended: `{var}_mean` and `{var}_sd`.

#### NetCDF File Resolution Strategy (`env_extraction/extract.py`)

The extraction module auto-detects which download file layout was used:

- **Merged monthly files** (`{var}_{YYYY-MM}.nc`) — Positions are grouped by calendar month. One file is opened per month and used for all positions in that month.
- **Per-animal monthly files** (`{var}_{animalID}_{YYYY-MM}.nc`) — Positions are grouped by (animal, month). The corresponding file is opened for each group.
- **Bulk yearly files** (`{var}_{YYYY}.nc`) — All files for a variable are opened as a single multi-file xarray dataset via `xr.open_mfdataset()` with Dask chunking.

Detection is based on filename pattern matching against files present in `data/raw/{var}/`.

### Derived Variables (`env_extraction/derivations.py`)

Some variables require post-download computation. The `DERIVATION_REGISTRY` maps derivation names to functions that take an `xr.Dataset` and return it with the derived variable added.

Currently implemented:

- **`eke_from_geostrophic`** — Computes Eddy Kinetic Energy from geostrophic velocity anomalies:

  ```
  EKE = 0.5 * (ugosa^2 + vgosa^2)
  ```

  where `ugosa` and `vgosa` are the eastward and northward geostrophic velocity anomalies (m/s). The result is in m^2/s^2.

New derivations can be registered by adding a function to `DERIVATION_REGISTRY` in `derivations.py`.

### Column Auto-Detection (`env_extraction/scan_tracks.py`)

The `trackenv scan` command inspects track CSV files and heuristically maps columns to roles. Detection uses two methods:

1. **Name matching** — Column names are compared (case-insensitive) against lists of known patterns for each role (ID, date, longitude, latitude, SE X, SE Y, location class).
2. **Value-based fallback** — If name matching fails, values are inspected: date-like columns are tested with `pd.to_datetime()`, longitude candidates must fall within [-180, 360], and latitude candidates within [-90, 90].

When multiple candidates match a single role, the first match is used and a warning is logged. The detected mapping can be written directly into a `project.yaml` file with `--write`.

---

## Scripts

### `run_extraction.py` — Standalone Extraction Pipeline

A self-contained CLI entry point that runs the full pipeline (load tracks, download data, extract values, save CSV). This is the legacy interface; the equivalent `trackenv extract` subcommand is preferred for new usage.

**Usage:**

```bash
# Single animal, SST only
python run_extraction.py animotum/ --ids 170200201 --variables sst

# All animals, all variables
python run_extraction.py animotum/

# From project.yaml configuration
python run_extraction.py --config project.yaml

# Use a custom ID filter, skip downloads (use cached NetCDFs)
python run_extraction.py animotum/ --id-csv my_ids.csv --skip-download
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `track_dir` | (positional, optional) | Directory containing track CSV files. |
| `--config`, `-c` | `None` | Path to `project.yaml`. When provided, overrides track directory, column mapping, paths, and variable definitions from the YAML. |
| `--id-csv` | `None` | Path to a CSV file with an `ID` column listing animal IDs to include. |
| `--ids` | `None` | Space-separated list of animal IDs. Overrides `--id-csv`. |
| `--variables` | all registered | Space-separated list of variable short names to extract (e.g., `sst chl sal`). |
| `--pattern` | `*_rw_predicted.csv` | Glob pattern for matching track CSV files in `track_dir`. |
| `--se-cap` | `200.0` | Maximum allowed SE value in km. Values above this are clipped. |
| `--sigma-multiplier` | `2.0` | Gaussian extraction radius = multiplier * max(se_x, se_y). |
| `--padding` | `50.0` | Bounding box padding in km added to all sides of the track extent. |
| `--raw-dir` | `data/raw/` | Directory where downloaded NetCDF files are stored. |
| `--output-dir` | `data/output/` | Directory for the output CSV. |
| `--output-name` | auto-generated | Explicit output filename prefix. If omitted, the filename is generated from animal IDs or defaults to `env_extract_all.csv`. |
| `--skip-download` | `false` | Skip the download step entirely. Extraction proceeds using whatever NetCDF files already exist in `--raw-dir`. |
| `--max-workers` | `3` | Maximum number of concurrent download threads. |
| `--download-strategy` | `auto` | Download chunking strategy. `auto` selects per-variable based on resolution. `monthly-shark` forces high-res monthly downloads for ERDDAP sources. `yearly-bbox` forces coarse yearly bbox downloads for all sources (overrides resolution to 0.25 deg and stride to 1). |
| `--log-level` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |

**Output:** A single CSV file in `--output-dir` with all original track columns plus `{var}_mean` and `{var}_sd` columns for each extracted variable.

**Pipeline steps (in order):**

1. Load and validate tracks from CSV files.
2. Compute a padded bounding box from all positions.
3. Download NetCDF data for each variable (skipped with `--skip-download`).
4. Extract Gaussian-weighted values at each position for each variable.
5. Save the merged result to CSV and log per-variable extraction success rates.

---

### `analyze_tracks.py` — Pre-Flight Analysis

A dry-run tool that loads tracks and reports what *would* be downloaded, without fetching any data. Use this to verify track loading, inspect data coverage, and estimate download sizes before committing to a full extraction run.

**Usage:**

```bash
# Analyze all tracks in a directory
python analyze_tracks.py animotum/

# Analyze specific animals
python analyze_tracks.py animotum/ --ids 170200201 170200301

# Analyze with project config
python analyze_tracks.py --config project.yaml animotum/

# Estimate for specific variables only
python analyze_tracks.py animotum/ --variables sst chl
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `track_dir` | (positional, required) | Directory containing track CSV files. |
| `--config`, `-c` | `None` | Path to `project.yaml`. |
| `--id-csv` | `None` | CSV with ID column for filtering. |
| `--ids` | `None` | Manual list of animal IDs. |
| `--pattern` | `*.csv` | Glob pattern for track files. |
| `--padding` | `50.0` | Bounding box padding in km. |
| `--variables` | all registered | Variables to include in download estimates. |

**Output sections:**

1. **Track Summary** — Number of animals, total positions, date range, lat/lon extent, SE statistics (mean, median, max for both `se_x` and `se_y`).
2. **Per-animal breakdown** — Position count and date range for each individual animal.
3. **Download grouping comparison** — Number of ID-month groups (per-animal-month) vs. merged month groups (union across animals). Reports how many fewer download requests the merged strategy requires.
4. **Download estimates per variable** — For each variable, reports the download strategy that would be used (merged monthly or yearly bbox), the estimated file size per chunk in MB, and the total estimated download size. Estimates are computed as:

   ```
   n_values = (lat_range / effective_resolution) * (lon_range / effective_resolution) * n_days * n_variables
   size_mb  = n_values * 4 bytes / (1024 * 1024)
   ```

   For monthly-strategy variables, a per-month table is printed showing the number of active animals, positions, spatial extent, and estimated size for each calendar month.

---

### `trackenv` CLI (`env_extraction/cli.py`)

The unified CLI entry point, invoked as `python -m env_extraction <command>` or `trackenv <command>` (if installed). Provides all functionality through subcommands.

**Subcommands:**

#### `trackenv init`

Generates a starter `project.yaml` template with all configurable fields and commented-out variable override examples.

```bash
trackenv init                     # Creates project.yaml in current directory
trackenv init -o my_project.yaml  # Custom output path
trackenv init --force             # Overwrite existing file
```

#### `trackenv scan <path>`

Auto-detects column roles in track CSV files and prints a report. Optionally writes the detected mapping into a `project.yaml` file.

```bash
trackenv scan tracks/                    # Scan a directory of CSVs
trackenv scan tracks/my_data.csv         # Scan a single file
trackenv scan tracks/ --write            # Write mapping to project.yaml
trackenv scan tracks/ --write -o p.yaml  # Write to custom path
```

The report includes: file structure (single vs. multi-file), row/ID counts, date and coordinate ranges, detected column mapping, SE range statistics, extra (unmapped) columns, and any ambiguities where multiple columns matched a single role.

#### `trackenv analyze <track_dir>`

Identical to `analyze_tracks.py`. See the standalone script section above for full details.

#### `trackenv extract [track_dir]`

Identical to `run_extraction.py`. See the standalone script section above for full details. The `track_dir` argument is optional when `--config` is provided (the track directory is read from the YAML).

#### `trackenv download <track_dir>`

Downloads environmental data without running extraction. Useful for pre-fetching data or running downloads and extraction as separate steps.

```bash
trackenv download animotum/ --variables sst chl
trackenv download animotum/ --max-workers 5
```

Accepts the same `--id-csv`, `--ids`, `--pattern`, `--padding`, `--variables`, `--raw-dir`, and `--max-workers` arguments as `extract`.

---

## Module Reference

```
env_extraction/
  __init__.py           Package marker.
  __main__.py           Entry point for `python -m env_extraction`.
  cli.py                Unified CLI with argparse subcommands (init, scan, analyze, extract, download).
  config.py             VariableConfig dataclass, built-in variable registry, default paths and parameters.
  project_config.py     YAML-based ProjectConfig loader, TrackFormat dataclass, starter template generator.
  scan_tracks.py        Column auto-detection heuristics and YAML writer.
  tracks.py             Track CSV loading, column normalization, SE derivation, bounding box computation.
  extract.py            Gaussian-weighted extraction engine. Opens NetCDF files, iterates positions, computes weighted stats.
  derivations.py        Derived variable computation registry (EKE from geostrophic velocities).
  pipeline.py           End-to-end orchestration: load → download → extract → save.
  utils.py              Spatial utilities: km↔degree conversions, haversine distance, dateline splitting, longitude normalization.
  download/
    __init__.py         Package marker.
    manager.py          Download orchestration: strategy selection, month grouping, parallelism, caching.
    erddap.py           ERDDAP downloads via erddapy with retry logic, stride support, and longitude normalization.
    copernicus.py       Copernicus Marine Service downloads via copernicusmarine.subset().
```

## Configuration (`project.yaml`)

All pipeline behavior can be controlled through a `project.yaml` file. See `project.yaml.example` for the full schema. Key sections:

- **`project.name`** — Used in output file naming.
- **`tracks.directory`** — Path to track CSV files (relative to the YAML file's parent directory).
- **`tracks.file_pattern`** — Glob pattern for track files.
- **`tracks.format`** — Column mapping (`id_col`, `date_col`, `lon_col`, `lat_col`, `se_x_col`, `se_y_col`, `location_class_col`, `fixed_se_km`, `extra_columns`, `id_from_filename`, `id_filename_separator`, `id_filename_index`).
- **`tracks.id_filter_csv`** / **`tracks.id_filter_col`** — Optional CSV-based ID filtering.
- **`extraction.se_cap_km`** — SE clipping threshold.
- **`extraction.sigma_multiplier`** — Gaussian radius multiplier.
- **`extraction.padding_km`** — Bounding box padding.
- **`paths.raw_dir`** / **`paths.output_dir`** — Data directories.
- **`variables`** — Override or extend the built-in variable registry. Each entry specifies `source`, `dataset_id`, `variable_names`, `extract_variable`, `resolution_deg`, `stride`, `depth`, `derived`, and `derivation`.

## Dependencies

```
pandas>=2.0
xarray>=2024.1
netcdf4>=1.6
dask>=2024.1
erddapy>=2.1
copernicusmarine>=1.0
numpy>=1.26
tqdm>=4.66
pyyaml>=6.0
```

Install with:

```bash
pip install -r requirements.txt
```
