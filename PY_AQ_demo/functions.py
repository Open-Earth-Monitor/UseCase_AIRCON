"""Python translation of `functions.R`.

This module ports the key AQ-interpolation helpers from R to Python using
common geospatial/ML libraries (geopandas, rasterio, rioxarray, scikit-learn,
pykrige).  The API follows the original R names as closely as reasonable.  Some
niche parts of the R code (e.g. exact file-naming heuristics or COG creation)
were omitted or simplified; add as needed.

Dependencies (already listed in `environment.yml`):
    pandas, numpy, geopandas, rasterio, rioxarray, xarray, scikit-learn,
    pykrige, matplotlib, tqdm, pyproj

Usage example
-------------
> from functions import load_aq, load_covariates, linear_aq_model
> aq = load_aq("PM10", "perc", y=2015, m=1)
> dem = rioxarray.open_rasterio("supplementary/static/COP-DEM/... .tif")
> cov = load_covariates(aq, dem)
> model = linear_aq_model(aq, cov)
"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from matplotlib import pyplot as plt
from pykrige.ok import OrdinaryKriging
from pyproj import CRS, Transformer
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from sklearn.linear_model import LinearRegression
from tqdm import tqdm

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT_DIR = Path(__file__).resolve().parent  # points to PY_AQ_demo
DATA_DIR = ROOT_DIR / "Data"                 # For DEM, TIFF covariates, and now AQ Parquet files
AQ_DATA_DIR = DATA_DIR                       # AQ data is also in the 'Data' directory
SUPPL_DIR = ROOT_DIR / "supplementary"      # ...
STATIONS_META = DATA_DIR / "aq_o3_mean_2020.gpkg" # Updated path for station metadata

# -----------------------------------------------------------------------------
# Helper utilities
# -----------------------------------------------------------------------------

def find_poll(columns: Sequence[str]) -> Optional[str]:
    """Return pollutant name if one of known names appears in *columns*."""
    for p in ("PM10", "PM2.5", "O3", "NO2", "SO2"):
        if p in columns:
            return p
    return None


def _load_meta() -> pd.DataFrame:
    if STATIONS_META.exists():
        return pd.read_parquet(STATIONS_META)
    LOG.warning("Station meta table not found → continuing without metadata.")
    return pd.DataFrame()


def add_meta(df: pd.DataFrame) -> pd.DataFrame:
    """Attach station metadata columns if they are missing."""
    want = {"Countrycode", "Station.Type", "Station.Area", "Longitude", "Latitude"}
    if want.issubset(df.columns):
        return df
    meta = _load_meta()
    if meta.empty:
        return df
    out = df.merge(meta, how="left", on="AirQualityStationEoICode")
    return out


def create_dir_if(paths: Sequence[os.PathLike | str], recursive: bool = True):
    for p in paths:
        Path(p).mkdir(parents=recursive, exist_ok=True)


# -----------------------------------------------------------------------------
# Loading AQ measurement tables (Arrow-equivalent via pandas CSV/parquet)
# -----------------------------------------------------------------------------

_def_patterns = {
    "01_daily": re.compile(r"{poll}.*{stat}.*", re.I),
    "02_monthly": re.compile(r"{poll}.*{stat}.*", re.I),
    "03_annual": re.compile(r"{poll}.*{stat}.*", re.I),
}


def _open_dataset(path: Path, filters: dict[str, int]) -> pd.DataFrame:
    """Load measurement data from Parquet, CSV, or GeoPackage."""
    LOG.info(f"Attempting to open dataset: {path} with filters: {filters}")
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix == ".gpkg":
        # Read with geopandas, but we'll treat it as a pandas DataFrame for filtering
        # The geometry handling is done later in load_aq if needed
        df = gpd.read_file(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix} for {path}")

    # Apply filters
    for col, val in filters.items():
        df = df[df[col] == val]
    return df


def load_aq(pollutant: str, stat: str, y: int, m: int = 0, d: int = 0,
            include_geometry: bool = True, verbose: bool = True) -> gpd.GeoDataFrame:
    """Load AQ measurements similar to R `load_aq()`.

    Parameters
    ----------
    pollutant : str
        One of PM10, PM2.5, O3, NO2, SO2
    stat : str
        'perc', 'mean' or 'max'
    y, m, d : int
        Year, (optionally) month, or day-of-year to filter.
    include_geometry : bool
        Return GeoDataFrame (ETRS89 / LAEA Europe EPSG:3035) if True.
    """
    if pollutant not in {"PM10", "PM2.5", "O3", "NO2", "SO2"}:
        raise ValueError("Unsupported pollutant")
    if stat not in {"perc", "mean", "max"}:
        raise ValueError("stat must be perc/mean/max")

    # freq_dir logic is no longer used for path construction if files are flat in AQ_DATA_DIR
    # freq_dir = "01_daily" if d > 0 else ("02_monthly" if m > 0 else "03_annual")
    
    # The pattern needs to be specific enough if year is not part of it and multiple years exist.
    # For now, assume the pattern is sufficient or only one year's file matches.
    # Original patterns from _def_patterns were generic: {poll}.*{stat}.*
    # We might need to make them more specific if files for multiple years/resolutions are in 'Data'
    # e.g., include year in the pattern or filename itself.
    # Current pattern: {poll}.*{stat}.*
    # Let's assume for now the filename itself will distinguish if needed, or only one relevant file exists.

    # Construct the pattern based on pollutant and stat.
    # The original _def_patterns were identical for all freq_dir, so we can simplify.
    pattern_str = f"{pollutant}.*{stat}.*" # Case-insensitive due to re.I later
    compiled_pattern = re.compile(pattern_str, re.I)
    LOG.info(f"Searching for AQ Parquet files in: {AQ_DATA_DIR} with pattern: '{pattern_str}'")

    # Search directly in AQ_DATA_DIR (which is now PY_AQ_demo/Data/)
    candidates = [p for p in AQ_DATA_DIR.glob("*.parquet") if compiled_pattern.search(p.name)]
    
    selected_candidate = None

    if candidates:
        # If Parquet files are found, try to select the correct one by year
        if len(candidates) > 1:
            LOG.warning(f"Multiple Parquet files found matching pattern '{pattern_str}': {candidates}. Attempting to filter by year '{y}' in filename.")
            year_pattern = re.compile(f".*{y}.*") # Simple check for year in filename
            year_specific_candidates = [p for p in candidates if year_pattern.search(p.name)]
            if len(year_specific_candidates) == 1:
                selected_candidate = year_specific_candidates[0]
                LOG.info(f"Selected year-specific Parquet file: {selected_candidate}")
            elif len(year_specific_candidates) > 1:
                LOG.warning(f"Multiple Parquet files found even after filtering for year '{y}' in filename: {year_specific_candidates}. Using the first one: {year_specific_candidates[0]}")
                selected_candidate = year_specific_candidates[0]
            else:
                LOG.warning(f"No Parquet files found matching pattern '{pattern_str}' and year '{y}' in filename. Using the first generic Parquet match: {candidates[0]}")
                selected_candidate = candidates[0]
        elif len(candidates) == 1:
            selected_candidate = candidates[0]
            LOG.info(f"Selected Parquet file: {selected_candidate}")
    
    # If no suitable Parquet candidate was found, and specific conditions are met, try the GPKG file.
    if not selected_candidate and pollutant == "O3" and y == 2020:
        LOG.info(f"No Parquet file found for O3, 2020 (pattern: '{pattern_str}'). Attempting to load data from STATIONS_META: {STATIONS_META}")
        if STATIONS_META.exists():
            selected_candidate = STATIONS_META
        else:
            raise FileNotFoundError(f"Fallback GPKG file {STATIONS_META} not found for O3 2020 data.")

    if not selected_candidate:
        # This case is reached if no Parquet was found and the GPKG fallback conditions weren't met or GPKG didn't exist.
        raise FileNotFoundError(f"No AQ data file found in {AQ_DATA_DIR} for pattern '{pattern_str}' (for year {y}), and fallback conditions not met or fallback file not found.")

    path = selected_candidate
    LOG.info(f"Proceeding to load AQ data from: {path}")
    filters = {"year": y}
    if m > 0:
        filters["month"] = m
    if d > 0:
        filters["day"] = d

    df = _open_dataset(path, filters)

    # If data was loaded from the STATIONS_META gpkg, ensure station ID column is renamed
    if path == STATIONS_META and "Air.Quality.Station.EoI.Code" in df.columns:
        LOG.info(f"Renaming 'Air.Quality.Station.EoI.Code' to 'AirQualityStationEoICode' for data loaded from {STATIONS_META}")
        df = df.rename(columns={"Air.Quality.Station.EoI.Code": "AirQualityStationEoICode"})

    if df.empty:
        LOG.warning(f"No data found for {pollutant} {stat} y={y} m={m} d={d} in {path.name}")
    df = add_meta(df)

    if include_geometry:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.Longitude, df.Latitude, crs="EPSG:4326"),
        ).to_crs("EPSG:3035")
    else:
        gdf = df

    gdf.attrs.update({"stat": stat, "y": y, "m": m, "d": d, "pollutant": pollutant})
    if verbose:
        LOG.info("Loaded %s rows from %s", len(gdf), path.name)
    return gdf


# -----------------------------------------------------------------------------
# Raster helpers
# -----------------------------------------------------------------------------

def warp_to_target(src: xr.DataArray | xr.Dataset, target: xr.DataArray | xr.Dataset,
                   method: str = "bilinear", mask: bool = True, nodata: float = np.nan) -> xr.DataArray:
    """Reproject *src* to match *target* grid (resolution, crs, extent)."""
    if isinstance(src, xr.Dataset):
        src = next(iter(src.data_vars.values()))
    if isinstance(target, xr.Dataset):
        target = next(iter(target.data_vars.values()))

    # Use rioxarray's built-in reproject_match
    reprojected = src.rio.reproject_match(target, resampling=getattr(Resampling, method))
    if mask:
        reprojected = reprojected.where(~target.isnull())
    if nodata is not np.nan:
        reprojected = reprojected.fillna(nodata)
    return reprojected


def read_n_warp(path: os.PathLike | str, band: int, target: xr.DataArray,
                name: str | None = None) -> xr.DataArray:
    """Read *band* of *path* and warp to *target* grid."""
    da = rxr.open_rasterio(path, masked=True).sel(band=band)
    da = warp_to_target(da, target)
    if name:
        da.name = name
    return da


# -----------------------------------------------------------------------------
# Covariate loading (greatly simplified compared to R version)
# -----------------------------------------------------------------------------

@dataclass
class CovariateConfig:
    pollutant: str
    stat: str
    y: int
    m: int
    d: int


# -----------------------------------------------------------------------------
# Covariate loading helpers
# -----------------------------------------------------------------------------

def _sanitize_varname_for_filename(varname: str) -> str:
    """Sanitize variable name for use in filenames (e.g., PM2.5 -> PM2p5)."""
    return varname.replace(".", "p")


_COVARIATE_FILENAME_MAP = {
    "DEM": "dem",
    "CAMS_PM10": "cams",
    "CAMS_PM2.5": "cams",
    "CAMS_O3": "cams",
    "CAMS_NO2": "cams",
    "CAMS_SO2": "cams",
    "WindSpeed": "ws",
    "RelativeHumidity": "rh",
    "BoundaryLayerHeight": "blh",
    "SolarRadiation": "solar",
    "TROPOMI_NO2": "tropomi_no2", 
    "PopulationDensity": "pop", 
    "Imperviousness": "imperv", 
}

def _get_covariate_path(
    var_name: str, 
    year: int, 
    base_dir: Path = DATA_DIR,      
    file_extension: str = ".tif", 
    time_res_folder: Optional[str] = None, 
    perc_str: Optional[str] = None, 
    pollutant_code: Optional[str] = None 
) -> Optional[Path]:
    """Find covariate file path, supporting TIFFs from DATA_DIR and NetCDFs from SUPPL_DIR."""
    
    found_files = []
    # Ensure stripped_ext handles both .tif and .tiff for the primary case
    primary_stripped_ext = file_extension.lstrip('.')
    alternative_stripped_ext = None
    if primary_stripped_ext == "tif":
        alternative_stripped_ext = "tiff"
    elif primary_stripped_ext == "tiff":
        alternative_stripped_ext = "tif"

    if primary_stripped_ext in ["tif", "tiff"]:
        mapped_var_name = _COVARIATE_FILENAME_MAP.get(var_name, var_name.lower())
        
        # Attempt to infer pollutant_code if not provided and var_name suggests it (e.g., CAMS_O3)
        if not pollutant_code and var_name.startswith("CAMS_") and '_' in var_name:
            try:
                inferred_poll = var_name.split('_', 1)[1].lower()
                # Basic check if inferred part could be a pollutant code (e.g. o3, pm10, pm2p5)
                if len(inferred_poll) > 1 and len(inferred_poll) < 6: # crude check
                    pollutant_code = inferred_poll
                    LOG.info(f"Inferred pollutant_code '{pollutant_code}' from var_name '{var_name}'")
            except IndexError:
                pass # Could not infer

        patterns_to_try = []
        if pollutant_code:
            # Specific pattern: aq_cov_{pollutant}_{year}_{variable}.tif / .tiff
            patterns_to_try.append(f"aq_cov_{pollutant_code}_{year}_{mapped_var_name}.{primary_stripped_ext}")
            if alternative_stripped_ext:
                patterns_to_try.append(f"aq_cov_{pollutant_code}_{year}_{mapped_var_name}.{alternative_stripped_ext}")
            # Allow for DEM to also be found with more generic names if the specific one fails
            if var_name == "DEM":
                patterns_to_try.extend([
                    f"*{mapped_var_name}*{year}.{primary_stripped_ext}", 
                    f"*{mapped_var_name}.{primary_stripped_ext}"
                ])
                if alternative_stripped_ext:
                    patterns_to_try.extend([
                        f"*{mapped_var_name}*{year}.{alternative_stripped_ext}", 
                        f"*{mapped_var_name}.{alternative_stripped_ext}"
                    ])
        else:
            # Generic patterns if no pollutant_code: *{variable}*{year}.tif / .tiff or *{variable}.tif / .tiff
            patterns_to_try.extend([
                f"*{mapped_var_name}*{year}.{primary_stripped_ext}", 
                f"*{mapped_var_name}.{primary_stripped_ext}"
            ])
            if alternative_stripped_ext:
                patterns_to_try.extend([
                    f"*{mapped_var_name}*{year}.{alternative_stripped_ext}", 
                    f"*{mapped_var_name}.{alternative_stripped_ext}"
                ])

        LOG.info(f"Searching for TIFF covariate '{var_name}' (mapped: '{mapped_var_name}', pollutant_code: {pollutant_code}) in '{base_dir}' using patterns: {patterns_to_try}")
        for pattern in patterns_to_try:
            found_files.extend(list(base_dir.glob(pattern)))
            if found_files: break

    elif primary_stripped_ext == "nc":
        if base_dir == DATA_DIR: # If default DATA_DIR is used for .nc, switch to SUPPL_DIR or warn
            LOG.warning(f"Searching for NetCDF (.nc) but base_dir is DATA_DIR. Switching to SUPPL_DIR for this search.")
            effective_base_dir = SUPPL_DIR
        else:
            effective_base_dir = base_dir

        sane_varname = _sanitize_varname_for_filename(var_name)
        pattern_parts = [sane_varname.lower(), "*"]
        if perc_str:
            pattern_parts.append(f"{perc_str}*")
        pattern_parts.append(f"{year}.{primary_stripped_ext}")
        glob_pattern = "".join(pattern_parts)
        
        search_path = effective_base_dir / time_res_folder if time_res_folder else effective_base_dir
        LOG.info(f"Searching for NetCDF covariate '{var_name}' in '{search_path}' using pattern: '{glob_pattern}'")
        if search_path.is_dir():
            found_files.extend(list(search_path.glob(glob_pattern)))
        else:
            LOG.warning(f"NetCDF search directory does not exist: {search_path}")
    else:
        LOG.error(f"Unsupported file extension '{file_extension}' for covariate '{var_name}'")
        return None

    if not found_files:
        LOG.warning(f"Covariate file for '{var_name}' (ext: {file_extension}, pollutant_code: {pollutant_code}) not found with tried patterns.")
        return None
    
    if len(found_files) > 1:
        # Simple preference: if pollutant_code was used, prefer filenames containing it.
        if pollutant_code and primary_stripped_ext in ["tif", "tiff"]:
            preferred_files = [f for f in found_files if pollutant_code in f.name and str(year) in f.name and mapped_var_name in f.name]
            if preferred_files:
                LOG.info(f"Multiple files found for TIFF covariate '{var_name}'. Using preferred (matched pollutant_code, year, var_name): {preferred_files[0]}")
                return preferred_files[0]
        LOG.warning(f"Multiple files found for covariate '{var_name}': {found_files}. Using first one: {found_files[0]}")
    
    LOG.info(f"Found covariate file for '{var_name}': {found_files[0]}")
    return found_files[0]


def load_covariates(aq: gpd.GeoDataFrame, dem: xr.DataArray) -> xr.Dataset:
    """Load and prepare all covariate data, aligning with the R script's logic.

    Args:
        aq: GeoDataFrame with AQ measurements and attributes (pollutant, stat, y, m, d).
        dem: xr.DataArray of DEM, used as the target for warping other covariates.
             This 'dem' is the template grid, not necessarily the DEM covariate itself.

    Returns:
        xr.Dataset containing all loaded and processed covariates.
    """
    attrs = aq.attrs
    pollutant = attrs.get("pollutant", "unknown")
    stat = attrs.get("stat", "unknown")
    year = attrs.get("y")
    month = attrs.get("m", 0)
    day = attrs.get("d", 0)

    if not all([pollutant, stat, year is not None]):
        LOG.error("Missing critical attributes (pollutant, stat, year) in aq input.")
        return xr.Dataset(attrs=attrs)
    
    pollutant_lower = pollutant.lower()

    # Determine temporal resolution and layer index for NetCDFs (less relevant for single-band TIFFs)
    # This logic might still be useful if some covariates remain NetCDF or are multi-band TIFFs
    time_res_folder = "static" # Default, can be overridden
    layer_idx = 1 # Default to first band for single-band TIFFs or static NetCDFs
    perc_val_for_filename = None # For percentile-based NetCDF filenames

    if month == 0 and day == 0: # Annual
        time_res_folder = "annual"
        # layer_idx = 1 (usually annual data is single layer for the year)
        if stat == "P10": perc_val_for_filename = "10"
        if stat == "P50": perc_val_for_filename = "50"
        if stat == "P90": perc_val_for_filename = "90"
        # If stat is "mean", perc_val_for_filename remains None
    elif month != 0 and day == 0: # Monthly
        time_res_folder = "monthly"
        layer_idx = month
    elif month != 0 and day != 0: # Daily
        time_res_folder = "daily"
        # Assuming day is day_of_year if it's for daily files
        # If daily files are per month, then day is day_of_month
        # R script uses `lubridate::yday(make_date(y,m,d))` for daily layer index
        # For simplicity, if 'd' is passed, assume it's the correct layer_idx for daily files
        # This might need adjustment based on actual daily file structure
        try:
            layer_idx = pd.Timestamp(year, month, day).dayofyear
        except ValueError:
            LOG.warning(f"Could not determine dayofyear for {year}-{month}-{day}, using day ({day}) as layer_idx for daily data.")
            layer_idx = day 
    LOG.info(f"Determined temporal resolution: {time_res_folder}, layer_idx: {layer_idx}, perc_str: {perc_val_for_filename} for NetCDF patterns.")

    # --- Load DEM Covariate --- 
    # The 'dem' parameter to this function is the *target grid*.
    # Here we load the actual DEM data to be included as a covariate.
    dem_covariate_path = _get_covariate_path(
        var_name="DEM", 
        year=year, 
        pollutant_code=pollutant_lower # e.g., for aq_cov_o3_2020_dem.tif
    )
    if not dem_covariate_path:
        LOG.error(f"DEM covariate file (e.g., aq_cov_{pollutant_lower}_{year}_dem.tif) not found. Cannot proceed with covariate loading.")
        return xr.Dataset(attrs=attrs)
    dem_covariate_arr = read_n_warp(dem_covariate_path, band=1, target=dem, name="DEM")
    loaded_covariates_list = [dem_covariate_arr]
    LOG.info(f"Loaded DEM covariate: {dem_covariate_path}")

    # --- Load Common Covariates (now mostly TIFFs from DATA_DIR) ---
    # CAMS data (e.g., CAMS_PM10)
    cams_var_name_in_file = f"CAMS_{pollutant}" # e.g., CAMS_O3, CAMS_PM10
    cams_path = _get_covariate_path(
        var_name=cams_var_name_in_file, 
        year=year, 
        pollutant_code=pollutant_lower
    )
    if cams_path:
        LOG.info(f"Found CAMS data at: {cams_path}")
        cams_data_arr = read_n_warp(cams_path, band=layer_idx, target=dem, name=f"CAMS_{pollutant.upper()}")
        # Log-transform for PM10 and PM2.5 as in R script
        if pollutant in ["PM10", "PM2.5"]:
            cams_data_arr = xr.ufuncs.log(cams_data_arr)
            cams_data_arr.name = f"logCAMS_{pollutant.upper()}"
            LOG.info(f"Applied log-transform to CAMS data for {pollutant}")
        loaded_covariates_list.append(cams_data_arr)
    else:
        LOG.warning(f"CAMS data for {pollutant} not found.")

    # WindSpeed
    ws_path = _get_covariate_path(
        var_name="WindSpeed", 
        year=year, 
        pollutant_code=pollutant_lower # Assuming ws file is like aq_cov_o3_2020_ws.tif
    )
    if ws_path:
        LOG.info(f"Found WindSpeed data at: {ws_path}")
        wind_speed_arr = read_n_warp(ws_path, band=layer_idx, target=dem, name="WindSpeed")
        loaded_covariates_list.append(wind_speed_arr)
    else:
        LOG.warning("WindSpeed data not found.")

    # --- Load Pollutant-Specific Covariates ---
    # Note: file_extension defaults to .tif, base_dir to DATA_DIR in _get_covariate_path
    if pollutant == "PM10":
        # Relative Humidity for PM10
        rh_path = _get_covariate_path(
            var_name="RelativeHumidity", 
            year=year, 
            pollutant_code=pollutant_lower
        )
        if rh_path:
            rh_arr = read_n_warp(rh_path, band=layer_idx, target=dem, name="RelativeHumidity")
            loaded_covariates_list.append(rh_arr)
        else:
            LOG.warning("RelativeHumidity data for PM10 not found.")

    elif pollutant == "O3":
        # SolarRadiation for O3
        sr_path = _get_covariate_path(
            var_name="SolarRadiation", 
            year=year, 
            pollutant_code=pollutant_lower
        )
        if sr_path:
            sr_arr = read_n_warp(sr_path, band=layer_idx, target=dem, name="SolarRadiation")
            loaded_covariates_list.append(sr_arr)
        else:
            LOG.warning("SolarRadiation data for O3 not found.")

    elif pollutant == "NO2":
        # TROPOMI NO2 data for NO2
        s5p_path = _get_covariate_path(
            var_name="TROPOMI_NO2", 
            year=year, 
            pollutant_code=pollutant_lower
        )
        if s5p_path:
            s5p_arr = read_n_warp(s5p_path, band=layer_idx, target=dem, name="TROPOMI_NO2")
            loaded_covariates_list.append(s5p_arr)
        else:
            LOG.warning("TROPOMI_NO2 data for NO2 not found.")

    elif pollutant == "SO2":
        # PopulationDensity (static) for SO2
        # Assuming popden file might be generic or year-specific but not pollutant-specific in name
        # Or if it is: aq_cov_so2_2020_pop.tif - then add pollutant_code
        pop_path = _get_covariate_path(
            var_name="PopulationDensity", 
            year=year # Or a fixed year if truly static, e.g., 2020
            # pollutant_code=pollutant_lower # if filename includes 'so2'
        ) 
        if pop_path:
            pop_arr = read_n_warp(pop_path, band=1, target=dem, name="PopulationDensity") # Static, so band=1
            loaded_covariates_list.append(pop_arr)
        else:
            LOG.warning("PopulationDensity data for SO2 not found.")
        
        # Imperviousness (static) for SO2
        imp_path = _get_covariate_path(
            var_name="Imperviousness", 
            year=year # Or a fixed year if truly static
            # pollutant_code=pollutant_lower # if filename includes 'so2'
        )
        if imp_path:
            imp_arr = read_n_warp(imp_path, band=1, target=dem, name="Imperviousness") # Static, so band=1
            loaded_covariates_list.append(imp_arr)
        else:
            LOG.warning("Imperviousness data for SO2 not found.")

    elif pollutant == "PM2.5":
        # Relative Humidity for PM2.5
        rh_path_pm25 = _get_covariate_path(
            var_name="RelativeHumidity", 
            year=year, 
            pollutant_code=pollutant_lower
        )
        if rh_path_pm25:
            rh_arr_pm25 = read_n_warp(rh_path_pm25, band=layer_idx, target=dem, name="RelativeHumidity")
            loaded_covariates_list.append(rh_arr_pm25)
        else:
            LOG.warning("RelativeHumidity data for PM2.5 not found.")

        # Boundary Layer Height (blh) for PM2.5
        blh_path = _get_covariate_path(
            var_name="BoundaryLayerHeight", 
            year=year, 
            pollutant_code=pollutant_lower
        )
        if blh_path:
            blh_arr = read_n_warp(blh_path, band=layer_idx, target=dem, name="BoundaryLayerHeight")
            loaded_covariates_list.append(blh_arr)
        else:
            LOG.warning("BoundaryLayerHeight data for PM2.5 not found.")

    # Merge all loaded covariates into a single xarray.Dataset
    if not loaded_covariates_list:
        LOG.warning("No covariates were loaded. Returning an empty dataset.")
        return xr.Dataset(attrs=attrs)
    
    covariates_ds = xr.merge(loaded_covariates_list)
    covariates_ds.attrs.update(attrs) # Propagate original attributes
    LOG.info(f"Successfully loaded and merged {len(covariates_ds.data_vars)} covariates: {list(covariates_ds.data_vars.keys())}")
    return covariates_ds


# -----------------------------------------------------------------------------
# Modelling
# -----------------------------------------------------------------------------

def linear_aq_model(aq: gpd.GeoDataFrame, covariates: xr.Dataset) -> Optional[LinearRegression]:
    """Fit a linear model to predict pollutant concentrations from covariates.

    Parameters
    ----------
    aq : gpd.GeoDataFrame
        Air quality measurements, including geometry and target pollutant values.
        Must have 'pollutant' in its attrs.
    covariates : xr.Dataset
        Dataset of covariate rasters, aligned and warped.

    Returns
    -------
    Optional[LinearRegression]
        Fitted scikit-learn LinearRegression model, or None if fitting fails.
    """
    pollutant = aq.attrs.get("pollutant")
    if not pollutant:
        LOG.error("Attribute 'pollutant' missing from input GeoDataFrame 'aq'. Cannot determine target variable.")
        return None
    if pollutant not in aq.columns:
        LOG.error(f"Target pollutant column '{pollutant}' not found in GeoDataFrame 'aq'.")
        return None

    LOG.info(f"Preparing data for linear model for pollutant: {pollutant}")

    cov_vals_list = []
    valid_feature_names = []

    # Prepare station coordinates as DataArrays for .sel()
    # Ensure there are geometries to sample
    if aq.geometry.empty or aq.geometry.is_empty.all():
        LOG.error("Input GeoDataFrame 'aq' has no valid geometries for sampling.")
        return None
        
    station_x = xr.DataArray([g.x for g in aq.geometry], dims="station_id", name="x_coords")
    station_y = xr.DataArray([g.y for g in aq.geometry], dims="station_id", name="y_coords")

    for cov_name, da_original in covariates.data_vars.items():
        da = da_original.copy() # Work on a copy to avoid modifying the input dataset

        # Ensure 'da' is 2D (y, x) for sampling, selecting first band if multiple exist
        if 'band' in da.dims:
            if da.sizes['band'] > 1:
                LOG.warning(f"Covariate '{cov_name}' has multiple bands ({da.sizes['band']}). Using data from the first band (index 0).")
            da = da.isel(band=0)
        elif 'band' in da.coords: # If 'band' is a non-dimension coordinate
            if da['band'].size > 1:
                 LOG.warning(f"Covariate '{cov_name}' has multiple band coordinates. Using data associated with the first: {da['band'].data[0]}.")
                 da = da.sel(band=da['band'].data[0])
            # Drop scalar 'band' coordinate if it exists and is not a dimension, to avoid issues with .sel on x,y
            if 'band' in da.coords and 'band' not in da.dims:
                da = da.drop_vars('band', errors='ignore')

        if not (da.ndim == 2 and 'x' in da.dims and 'y' in da.dims):
            LOG.error(f"Covariate '{cov_name}' could not be resolved to 2D (y,x) for sampling. Current dims: {da.dims}. Skipping.")
            cov_vals_list.append(np.full(station_x.size, np.nan)) # Add NaNs to maintain column structure
            valid_feature_names.append(f"{cov_name}_SKIPPED_WRONG_DIMS") # Mark skipped
            continue

        try:
            sampled_points = da.sel(x=station_x, y=station_y, method="nearest")
            cov_vals_list.append(sampled_points.to_numpy())
            valid_feature_names.append(cov_name)
        except Exception as e:
            LOG.error(f"Failed to sample covariate '{cov_name}' using xarray.sel: {e}. Appending NaNs.")
            cov_vals_list.append(np.full(station_x.size, np.nan))
            valid_feature_names.append(f"{cov_name}_SKIPPED_SAMPLING_ERROR") # Mark skipped

    if not cov_vals_list:
        LOG.error("No covariate values could be extracted. Cannot fit linear model.")
        return None

    X_candidate = np.column_stack(cov_vals_list)
    y_candidate = aq[pollutant].values

    # Filter out columns in X_candidate that are all NaNs (e.g. if a covariate failed entirely)
    all_nan_cols_mask = np.isnan(X_candidate).all(axis=0)
    X_filtered_cols = X_candidate[:, ~all_nan_cols_mask]
    final_feature_names = [name for i, name in enumerate(valid_feature_names) if not all_nan_cols_mask[i]]

    if X_filtered_cols.shape[1] == 0:
        LOG.error("All covariate columns are NaN after sampling. Cannot fit linear model.")
        return None

    # Handle row-wise NaNs (mimicking R's na.omit)
    nan_mask_X_rows = np.isnan(X_filtered_cols).any(axis=1)
    nan_mask_y_rows = np.isnan(y_candidate)
    combined_nan_mask_rows = nan_mask_X_rows | nan_mask_y_rows
    
    X_final = X_filtered_cols[~combined_nan_mask_rows]
    y_final = y_candidate[~combined_nan_mask_rows]

    if X_final.shape[0] == 0:
        LOG.error("After NaN removal (rows and full NaN columns), no data remains. Cannot fit linear model.")
        return None
    if X_final.ndim == 1: # If only one feature column remains after filtering
        X_final = X_final.reshape(-1, 1)
    if X_final.shape[1] == 0: # No features left
        LOG.error("No feature columns remain after NaN filtering. Cannot fit linear model.")
        return None
    if X_final.shape[0] < X_final.shape[1]:
        LOG.warning(f"After NaN removal, number of samples ({X_final.shape[0]}) is less than number of features ({X_final.shape[1]}). Model fitting might be problematic.")
    if X_final.shape[0] < 5: # Arbitrary small number of samples
        LOG.warning(f"After NaN removal, very few samples ({X_final.shape[0]}) remain. Model results may not be reliable.")

    model = LinearRegression()
    try:
        model.fit(X_final, y_final)
    except ValueError as e:
        LOG.error(f"Error during LinearRegression fitting: {e}. X_final shape: {X_final.shape}, y_final shape: {y_final.shape}")
        return None

    # Store feature names in the model if possible (sklearn convention)
    try:
        model.feature_names_in_ = np.array(final_feature_names, dtype=object)
    except Exception as e:
        LOG.warning(f"Could not set feature_names_in_ on the model: {e}")

    r2 = model.score(X_final, y_final)
    LOG.info(f"Linear model R²: {r2:.3f} using {X_final.shape[0]} samples and {X_final.shape[1]} features: {final_feature_names}")

    # Add residuals to the input GeoDataFrame (on the original, pre-NaN-removal index)
    # Predict on X_filtered_cols (before row-wise NaN removal) to get predictions for as many points as possible
    if X_filtered_cols.shape[0] > 0:
        # predictions_all_potential_rows = model.predict(X_filtered_cols) # This line caused ValueError due to NaNs in X_filtered_cols and is not essential for current residual calculation
        # Create a full residuals array with NaNs
        residuals_full = np.full(aq.shape[0], np.nan, dtype=float) # Ensure float for NaNs
        # Place predictions back where they belong, then calculate residuals
        # ~combined_nan_mask_rows are the indices from X_filtered_cols/y_candidate that were used for fitting
        # We need to map these back to the original 'aq' indices.
        
        # Create an intermediate prediction array aligned with y_candidate
        # (which is aq[pollutant].values, so it has the original length)
        # Rows that had NaNs in X_filtered_cols or y_candidate will have their predictions effectively ignored
        # or we can predict on X_filtered_cols and then filter. 
        # The model was fit on X_final, y_final.
        # To get residuals for the points used in fitting:
        predictions_on_fitted_data = model.predict(X_final)
        residuals_on_fitted_data = y_final - predictions_on_fitted_data
        
        # Place these residuals back into an array aligned with the original 'aq' DataFrame
        # The original 'aq' has 'aq.shape[0]' rows.
        # 'combined_nan_mask_rows' refers to 'y_candidate' and 'X_filtered_cols'.
        # Its inverse, '~combined_nan_mask_rows', gives the rows that were *not* NaN and were used.
        original_indices_used_for_fitting = np.where(~combined_nan_mask_rows)[0]
        
        # Ensure 'residuals_full' is float to accept NaNs
        residuals_full = np.full(aq.shape[0], np.nan, dtype=float)
        if original_indices_used_for_fitting.size == residuals_on_fitted_data.size:
            residuals_full[original_indices_used_for_fitting] = residuals_on_fitted_data
        else:
            LOG.error("Mismatch in size for placing residuals. Residuals not added to GeoDataFrame.")

        aq["residual"] = residuals_full
        LOG.info(f"Residuals calculated and added to GeoDataFrame. Mean residual: {np.nanmean(residuals_full):.3f}")
    else:
        LOG.warning("No data to predict on after column NaN filtering. Residuals not calculated.")
        aq["residual"] = np.nan

    return model


# -----------------------------------------------------------------------------
# Residual kriging (simplified via PyKrige OrdinaryKriging)
# -----------------------------------------------------------------------------

def krige_aq_residuals(
    aq: gpd.GeoDataFrame, 
    covariates: xr.Dataset, 
    model: Optional[LinearRegression] = None, # Kept for signature, not used for residual calculation
    n_max: int = 10 # Kept for signature, currently not directly mapped to PyKrige in this basic setup
) -> xr.Dataset:
    """Krige residuals using PyKrige OrdinaryKriging.
    Assumes 'residual' column already exists in 'aq' from linear_aq_model.
    The grid for kriging is defined by the 'covariates' Dataset.
    Returns an xr.Dataset with 'pred' (kriged residuals) and 'se' (standard error).
    """
    # Ensure PyKrige is available
    try:
        from pykrige.ok import OrdinaryKriging
    except ImportError:
        LOG.error("PyKrige is not installed. Please install it to use kriging (e.g., pip install pykrige).")
        if 'x' in covariates.coords and 'y' in covariates.coords:
            nan_data_pred = np.full((len(covariates.y), len(covariates.x)), np.nan)
            nan_data_se = np.full((len(covariates.y), len(covariates.x)), np.nan)
            return xr.Dataset(
                {"pred": (('y', 'x'), nan_data_pred), "se": (('y', 'x'), nan_data_se)},
                coords={'y': covariates.y, 'x': covariates.x}
            )
        return xr.Dataset() # Return empty Dataset

    if "residual" not in aq.columns:
        LOG.error("Input GeoDataFrame 'aq' must contain a 'residual' column.")
        if 'x' in covariates.coords and 'y' in covariates.coords:
            nan_data_pred = np.full((len(covariates.y), len(covariates.x)), np.nan)
            nan_data_se = np.full((len(covariates.y), len(covariates.x)), np.nan)
            return xr.Dataset(
                {"pred": (('y', 'x'), nan_data_pred), "se": (('y', 'x'), nan_data_se)},
                coords={'y': covariates.y, 'x': covariates.x}
            )
        return xr.Dataset()

    if n_max != 10: # Default value in signature
        LOG.warning(f"Parameter 'n_max' ({n_max}) is currently not directly used by PyKrige in this basic setup.")

    # Filter out rows with NaN residuals or invalid/empty geometries
    valid_aq = aq[aq["residual"].notna() & aq.geometry.is_valid & ~aq.geometry.is_empty].copy()

    if valid_aq.empty:
        LOG.warning("No valid data points (non-NaN residuals and valid geometry) for kriging.")
        if 'x' in covariates.coords and 'y' in covariates.coords:
            nan_data_pred = np.full((len(covariates.y), len(covariates.x)), np.nan)
            nan_data_se = np.full((len(covariates.y), len(covariates.x)), np.nan)
            return xr.Dataset(
                {"pred": (('y', 'x'), nan_data_pred), "se": (('y', 'x'), nan_data_se)},
                coords={'y': covariates.y, 'x': covariates.x}
            )
        return xr.Dataset()

    LOG.info(f"Performing Ordinary Kriging with {len(valid_aq)} points for residuals.")
    
    OK = OrdinaryKriging(
        valid_aq.geometry.x.values,
        valid_aq.geometry.y.values,
        valid_aq["residual"].values,
        variogram_model="spherical", 
        nlags=8, # As used in R script, can be adjusted
    )

    if not ({'x', 'y'} <= set(covariates.coords)):
        LOG.error("Covariates dataset must have 'x' and 'y' coordinates for grid definition.")
        return xr.Dataset() 

    gridx = covariates.x.values
    gridy = covariates.y.values

    try:
        z, ss = OK.execute("grid", gridx, gridy) # z: prediction, ss: variance
    except Exception as e:
        LOG.error(f"Error during PyKrige OK.execute: {e}")
        if 'x' in covariates.coords and 'y' in covariates.coords:
            nan_data_pred = np.full((len(covariates.y), len(covariates.x)), np.nan)
            nan_data_se = np.full((len(covariates.y), len(covariates.x)), np.nan)
            return xr.Dataset(
                {"pred": (('y', 'x'), nan_data_pred), "se": (('y', 'x'), nan_data_se)},
                coords={'y': covariates.y, 'x': covariates.x}
            )
        return xr.Dataset()
        
    kriged_pred_da = xr.DataArray(
        z,
        coords={"y": gridy, "x": gridx},
        dims=("y", "x"),
        name="pred"  # Name for the prediction DataArray
    )
    kriged_se_da = xr.DataArray(
        np.sqrt(ss.clip(min=0)), # Standard error, ensure variance is non-negative
        coords={"y": gridy, "x": gridx},
        dims=("y", "x"),
        name="se"  # Name for the standard error DataArray
    )
    
    return xr.Dataset({"pred": kriged_pred_da, "se": kriged_se_da})


# -----------------------------------------------------------------------------
# Combine & plotting
# -----------------------------------------------------------------------------

def combine_results(covariates: xr.Dataset, kriged_output_ds: xr.Dataset) -> xr.Dataset:
    lm_pred = covariates.get("lm_pred")
    if lm_pred is None:
        LOG.error("lm_pred missing from covariates; run predict_linear_model() first.")
        # Return an empty dataset or raise error, depending on desired strictness
        return xr.Dataset()
        
    kriged_residuals_pred = kriged_output_ds.get("pred")
    kriged_residuals_se = kriged_output_ds.get("se")

    if kriged_residuals_pred is None:
        LOG.error("'pred' (kriged residuals prediction) missing from kriged_output_ds.")
        return xr.Dataset()
    if kriged_residuals_se is None:
        LOG.warning("'se' (kriged residuals standard error) missing from kriged_output_ds. Std.Error plot will be empty.")
        # Create an array of NaNs for pred_se if it's missing, so merge doesn't fail
        kriged_residuals_se = xr.full_like(kriged_residuals_pred, np.nan).rename("pred_se")
    else:
        kriged_residuals_se = kriged_residuals_se.rename("pred_se") # Rename for clarity in final output

    # Ensure lm_pred and kriged_residuals_pred are aligned if they come from different gridding processes
    # This assumes they are already on the same grid, which should be the case by design here.
    try:
        combined_pred = lm_pred + kriged_residuals_pred
    except ValueError as e:
        LOG.error(f"Error combining lm_pred and kriged_residuals_pred (likely dimension mismatch): {e}")
        LOG.info(f"lm_pred dims: {lm_pred.dims}, shape: {lm_pred.shape}")
        LOG.info(f"kriged_residuals_pred dims: {kriged_residuals_pred.dims}, shape: {kriged_residuals_pred.shape}")
        return xr.Dataset()

    # Merge all relevant layers
    # The final dataset will contain: 'aq_pred', 'pred_se', and potentially others from covariates if desired.
    # For now, just the prediction and its SE related to kriging.
    # Also include the original lm_pred and kriged_residuals_pred for inspection if needed.
    ds_out = xr.Dataset()
    ds_out["aq_pred"] = combined_pred.rename("aq_pred")
    ds_out["pred_se"] = kriged_residuals_se # Already named pred_se
    ds_out["lm_pred_component"] = lm_pred # For diagnostics
    ds_out["kriged_residual_component"] = kriged_residuals_pred # For diagnostics
    
    ds_out.attrs.update(covariates.attrs)
    if hasattr(kriged_output_ds, 'attrs'):
        ds_out.attrs.update(kriged_output_ds.attrs)
        
    return ds_out


def predict_linear_model(model: LinearRegression, covariates: xr.Dataset) -> xr.DataArray:
    """Predict fitted linear model over raster grid. Handles NaNs in covariates by returning NaN predictions."""
    
    if not covariates.data_vars:
        LOG.warning("predict_linear_model: Input 'covariates' dataset has no data variables. Returning empty DataArray.")
        return xr.DataArray(name="lm_pred")

    df_full = covariates.to_dataframe().reset_index()

    if not hasattr(model, 'feature_names_in_') or model.feature_names_in_ is None:
        LOG.error("predict_linear_model: Model does not have 'feature_names_in_'. Cannot proceed.")
        # Fallback attempt (risky)
        if hasattr(model, 'coef_') and model.coef_ is not None:
            num_expected_features = model.coef_.shape[-1]
            available_cov_vars = [name for name in covariates.data_vars if name in df_full.columns]
            if len(available_cov_vars) >= num_expected_features:
                feature_names = available_cov_vars[:num_expected_features]
                LOG.warning(f"Using inferred feature names: {feature_names}. This might be incorrect.")
            else:
                LOG.error(f"Cannot infer feature names. Expected {num_expected_features}, found {len(available_cov_vars)}.")
                return xr.DataArray(name="lm_pred")
        else:
            return xr.DataArray(name="lm_pred")
    else:
        feature_names = list(model.feature_names_in_)

    missing_features = [name for name in feature_names if name not in df_full.columns]
    if missing_features:
        LOG.error(f"predict_linear_model: Missing feature columns in DataFrame: {missing_features}. Required: {feature_names}.")
        return xr.DataArray(name="lm_pred")

    X_for_prediction_df = df_full[feature_names]
    predictions = np.full(len(df_full), np.nan)
    mask_complete_rows = ~np.isnan(X_for_prediction_df.values).any(axis=1)
    
    if np.any(mask_complete_rows):
        X_complete_df = X_for_prediction_df[mask_complete_rows]
        try:
            predictions[mask_complete_rows] = model.predict(X_complete_df)
        except Exception as e:
            LOG.error(f"predict_linear_model: Error during model.predict: {e}")
            return xr.DataArray(name="lm_pred")

    df_full["lm_pred"] = predictions

    first_var_key = next(iter(covariates.data_vars))
    ordered_dim_names = list(covariates[first_var_key].dims)
    index_cols = [dim for dim in ordered_dim_names if dim in df_full.columns]

    if len(index_cols) != len(ordered_dim_names):
        LOG.warning(
            f"predict_linear_model: Mismatch between original dims {ordered_dim_names} "
            f"and found indexable columns {index_cols}. Result might be unexpected."
        )
        if 'y' in df_full.columns and 'x' in df_full.columns:
            index_cols_fallback = ['y', 'x']
            for dim in ordered_dim_names:
                if dim not in index_cols_fallback and dim in df_full.columns:
                    index_cols_fallback.append(dim)
            LOG.info(f"Attempting to use fallback index_cols: {index_cols_fallback}")
            index_cols = index_cols_fallback
        else:
            LOG.error("predict_linear_model: Cannot determine suitable index columns ('y', 'x' not found or ambiguous).")
            return xr.DataArray(name="lm_pred")

    if not index_cols:
        LOG.error("predict_linear_model: No valid index columns identified.")
        return xr.DataArray(name="lm_pred")
        
    try:
        df_indexed = df_full.set_index(index_cols)
        ds_out = df_indexed.to_xarray()
    except Exception as e:
        LOG.error(f"predict_linear_model: Error during set_index({index_cols}) or to_xarray(): {e}")
        LOG.error(f"DataFrame columns: {list(df_full.columns)}")
        LOG.error(f"Attempted index_cols: {index_cols}")
        LOG.error(f"DataFrame head (sample):\n{df_full.sample(min(5, len(df_full)))}")
        return xr.DataArray(name="lm_pred")

    if "lm_pred" not in ds_out:
        LOG.error("predict_linear_model: 'lm_pred' not found in output xarray Dataset.")
        return xr.DataArray(name="lm_pred")
        
    return ds_out["lm_pred"]


def plot_aq_prediction(ds: xr.Dataset, ax: Optional[plt.Axes] = None, cmap="viridis"):
    ax = ax or plt.subplots(figsize=(6, 6))[1]
    
    data_var = ds["aq_pred"]
    
    # Try to get units from data variable attributes, then dataset attributes, then default
    units = data_var.attrs.get("units", ds.attrs.get("units", "µg/m³"))
    pollutant_name = ds.attrs.get("pollutant", "AQ") # Get pollutant name from ds attributes
    
    # Use cbar_kwargs to set the colorbar label directly in the plot() call
    data_var.plot(ax=ax, cmap=cmap, robust=True, cbar_kwargs={'label': f"{pollutant_name} ({units})"})
    ax.set_title(f"Predicted {pollutant_name}")
    return ax


def plot_aq_se(ds: xr.Dataset, ax: Optional[plt.Axes] = None, cmap="magma"):
    ax = ax or plt.subplots(figsize=(6, 6))[1]
    if "pred_se" not in ds:
        # If pred_se is missing, display a message on the plot
        ax.text(0.5, 0.5, "'pred_se' layer missing", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
        ax.set_title("Prediction Std. Error (Missing)")
        ax.set_xticks([])
        ax.set_yticks([])
        return ax

    data_var_se = ds["pred_se"]
    # Try to get units from dataset attributes (should be same as prediction)
    units = ds.attrs.get("units", "µg/m³") 

    # Use cbar_kwargs to set the colorbar label directly in the plot() call
    data_var_se.plot(ax=ax, cmap=cmap, robust=True, cbar_kwargs={'label': f"σ ({units})"})
    ax.set_title("Prediction Std. Error")
    return ax


# -----------------------------------------------------------------------------
# House-keeping helpers (placeholders)
# -----------------------------------------------------------------------------

def check_map_progress(parent_dir: os.PathLike | str):
    """Stub: replicate R `check_map_progress()` – implement as needed."""
    LOG.info("check_map_progress() not implemented in Python version.")


if __name__ == "__main__":
    print("Module imported correctly. See docstring for usage.")
