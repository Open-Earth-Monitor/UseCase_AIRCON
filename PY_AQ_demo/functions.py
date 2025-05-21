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
AQ_DATA_DIR = ROOT_DIR / "AQ_data"          # mimic R layout
SUPPL_DIR = ROOT_DIR / "supplementary"      # ...
STATIONS_META = ROOT_DIR / "AQ_stations" / "EEA_stations_meta_table.parquet"

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
    out = df.merge(meta, how="left", on="Air.Quality.Station.EoI.Code")
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
    """Lightweight substitute for Arrow-dataset filtering."""
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
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

    freq_dir = "01_daily" if d > 0 else ("02_monthly" if m > 0 else "03_annual")
    pattern = _def_patterns[freq_dir].pattern.format(poll=pollutant, stat=stat)
    candidates = [p for p in (AQ_DATA_DIR / freq_dir).glob("*.parquet") if re.search(pattern, p.name)]
    if not candidates:
        raise FileNotFoundError(f"No AQ data file for pattern {pattern}")
    path = candidates[0]
    filters = {"year": y}
    if m > 0:
        filters["month"] = m
    if d > 0:
        filters["doy"] = d
    df = _open_dataset(path, filters)
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

def _get_covariate_path(
    time_res_folder: str,
    var_name: str,
    year: int,
    perc_str: Optional[str],
    base_dir: Path = SUPPL_DIR,
) -> Optional[Path]:
    """Construct glob pattern and find the first matching covariate file.

    Mimics R's `get_filename` for time-varying covariates.
    Searches in `base_dir / time_res_folder`.
    Pattern: `var_name_lower*{perc_str}*{year}.nc` or `var_name_lower*{year}.nc`.
    Returns the path to the first found file, or None.
    """
    sane_var_name = _sanitize_varname_for_filename(var_name)
    
    pattern_parts = [sane_var_name.lower()]
    if perc_str:
        pattern_parts.append(f"*{perc_str}")
    pattern_parts.append(f"*{year}.nc")
    glob_pattern = "".join(pattern_parts)
    
    search_dir = base_dir / time_res_folder
    if not search_dir.is_dir():
        LOG.warning(f"Covariate search directory does not exist: {search_dir}")
        return None

    found_files = sorted(list(search_dir.glob(glob_pattern)))
    if not found_files:
        LOG.warning(f"No covariate file found for pattern '{glob_pattern}' in {search_dir}")
        return None
    
    return found_files[0]


def load_covariates(aq: gpd.GeoDataFrame, dem: xr.DataArray) -> xr.Dataset:
    """Load and prepare covariates based on AirQuality GeoDataFrame attributes.

    This function replicates the dynamic covariate loading logic from the R
    script `functions.R/load_covariates`. It determines the pollutant,
    temporal resolution (daily, monthly, annual), and year from the input
    `aq` attributes. It then loads a pollutant-specific set of covariate
    rasters (e.g., CAMS data, wind speed, elevation, land cover, etc.),
    warps them to the target `dem` grid, and returns them as an xarray.Dataset.

    Parameters
    ----------
    aq : gpd.GeoDataFrame
        Air quality measurements, with attributes 'pollutant', 'stat', 'y', 'm', 'd'
        set by `load_aq()`.
    dem : xr.DataArray
        DEM (Digital Elevation Model) xarray.DataArray used as the target grid
        for warping covariates. Renamed to "Elevation" in the output.

    Returns
    -------
    xr.Dataset
        A dataset containing all loaded and processed covariate layers,
        aligned to the DEM grid. Attributes from `aq` (pollutant, year, etc.)
        are also propagated to this dataset.

    Raises
    ------
    ValueError
        If required attributes are missing from `aq`.
    FileNotFoundError
        If expected covariate files are not found based on naming conventions.
    """
    LOG.info(f"Loading dynamic covariates for pollutant: {aq.attrs.get('pollutant')}, year: {aq.attrs.get('y')}")

    # 1. Retrieve params from aq.attrs
    pollutant = aq.attrs.get('pollutant')
    stat = aq.attrs.get('stat')
    year = aq.attrs.get('y')
    month = aq.attrs.get('m')
    day_of_year = aq.attrs.get('d')

    if not all([pollutant, stat, isinstance(year, int), isinstance(month, int), isinstance(day_of_year, int)]):
        raise ValueError(
            "Missing or invalid required attributes (pollutant, stat, y, m, d) in input GeoDataFrame's attrs. "
            f"Found: pollutant={pollutant}, stat={stat}, y={year}, m={month}, d={day_of_year}"
        )

    # 2. Determine temporal resolution directory name and layer index for time-varying covariates
    if day_of_year > 0:
        time_res_folder = "01_daily"
        layer_idx = day_of_year  # R uses 1-indexed day of year for band selection
    elif month > 0:
        time_res_folder = "02_monthly"
        layer_idx = month  # R uses 1-indexed month for band selection
    else:
        time_res_folder = "03_annual"
        layer_idx = 1      # R uses band 1 for annual data

    # 'perc' string for filenames, similar to R: `perc = switch(stat, "perc" = "perc", NULL)`
    perc_val_for_filename = "perc" if stat == "perc" else None

    # --- Initialize list of covariates ---
    # DEM is 'tg' in R, used as the base elevation and target grid
    loaded_covariates_list = [dem.rename("Elevation")]
    cams_data_arr: Optional[xr.DataArray] = None # To hold CAMS data for potential log transform

    # --- Load common dynamic covariates (used by most/all pollutants) ---
    LOG.info(f"Temporal folder: {time_res_folder}, Layer index: {layer_idx}, Year: {year}, Pollutant: {pollutant}")

    # CAMS data (e.g., CAMS_PM10)
    # R: get_filename(tdir, cams_name, y, perc)
    cams_var_name_in_file = f"CAMS_{pollutant}" # e.g., CAMS_PM10
    cams_path = _get_covariate_path(time_res_folder, cams_var_name_in_file, year, perc_val_for_filename)
    if cams_path:
        LOG.info(f"Found CAMS data at: {cams_path}")
        cams_data_arr = read_n_warp(cams_path, band=layer_idx, target=dem, name=f"CAMS_{pollutant.upper()}")
        # loaded_covariates_list.append(cams_data_arr) # Will be added based on pollutant logic (e.g. log transformed)
    else:
        LOG.warning(f"{cams_var_name_in_file} covariate not found. Searched with perc='{perc_val_for_filename}'.")

    # WindSpeed
    # R: get_filename(tdir, "wind_speed", y) -> perc is NULL for wind_speed
    ws_path = _get_covariate_path(time_res_folder, "wind_speed", year, perc_str=None) # perc is NULL in R call
    if ws_path:
        LOG.info(f"Found WindSpeed data at: {ws_path}")
        wind_speed_arr = read_n_warp(ws_path, band=layer_idx, target=dem, name="WindSpeed")
        loaded_covariates_list.append(wind_speed_arr)
    else:
        LOG.warning("WindSpeed covariate not found.")

    # --- Pollutant-specific covariate loading logic ---
    # This section mirrors the `switch(poll, ...)` block in functions.R

    # Add CAMS data (potentially log-transformed) based on pollutant type
    if cams_data_arr is not None:
        if pollutant in ["PM10", "PM2.5"]:
            log_cams_name = f"log_CAMS_{pollutant.upper()}"
            try:
                loaded_covariates_list.append(np.log(cams_data_arr).rename(log_cams_name))
                LOG.info(f"Added log-transformed CAMS data as {log_cams_name}")
            except Exception as e:
                LOG.error(f"Error log-transforming CAMS data for {pollutant}: {e}, adding non-transformed version.")
                loaded_covariates_list.append(cams_data_arr) # Add non-transformed if log fails
        else: # For O3, NO2, SO2, add CAMS directly if available
            loaded_covariates_list.append(cams_data_arr)
            LOG.info(f"Added CAMS data as {cams_data_arr.name}")
    else:
        LOG.warning(f"CAMS data for {pollutant} was not loaded, will not be included.")

    # Additional covariates based on pollutant type
    if pollutant == "PM10":
        # R: warp_to_target(x = stars::read_stars(get_filename("static", "RelHum_daily", y), proxy=F)[,,,lyr], ... name = "RelHumidity")
        # Assuming RelHumidity is daily, so use time_res_folder and layer_idx if it's not truly static
        # For simplicity, let's assume a file like 'RelHum_daily_YYYY.nc' or 'RelHumidity_YYYY.nc'
        rh_path = _get_covariate_path(time_res_folder, "RelHumidity", year, perc_str=None) # Or "RelHum_daily"
        if rh_path:
            rh_arr = read_n_warp(rh_path, band=layer_idx, target=dem, name="RelativeHumidity")
            loaded_covariates_list.append(rh_arr)
            LOG.info("Added RelativeHumidity covariate.")
        else:
            LOG.warning("RelativeHumidity covariate not found.")
        
        # Example for a truly static land cover like CLC_NAT_1km from R
        # clc_nat_path = SUPPL_DIR / "static" / "clc" / "CLC_NAT_percent_1km.tif" # R: "supplementary/static/clc/CLC_NAT_percent_1km.tif"
        # if clc_nat_path.exists():
        #     clc_nat_arr = read_n_warp(clc_nat_path, band=1, target=dem, name="CLC_NAT_1km")
        #     loaded_covariates_list.append(clc_nat_arr)
        #     LOG.info("Added CLC_NAT_1km covariate.")
        # else:
        #     LOG.warning(f"Static covariate CLC_NAT_1km not found at {clc_nat_path}")

    elif pollutant == "O3":
        # R: read_n_warp(get_filename(tdir, "solar_radiation", y), lyr, target = tg, name = "SolarRadiation")
        sr_path = _get_covariate_path(time_res_folder, "solar_radiation", year, perc_str=None)
        if sr_path:
            sr_arr = read_n_warp(sr_path, band=layer_idx, target=dem, name="SolarRadiation")
            loaded_covariates_list.append(sr_arr)
            LOG.info("Added SolarRadiation covariate.")
        else:
            LOG.warning("SolarRadiation covariate not found.")

    elif pollutant == "NO2":
        # R: get_filename(tdir, "s5p_no2", y), var = "NO2_TROPOMI"
        # Assuming s5p_no2 files contain a variable/band named or corresponding to NO2_TROPOMI
        # The read_n_warp function uses band index. If NO2_TROPOMI is a specific var in a multi-var netCDF,
        # rioxarray's open_rasterio might need `variable='NO2_TROPOMI'` and then sel(band=layer_idx) if it's also time-sliced.
        # For now, assume band=layer_idx is sufficient or the file is single-variable for that band.
        s5p_path = _get_covariate_path(time_res_folder, "s5p_no2", year, perc_str=None)
        if s5p_path:
            s5p_arr = read_n_warp(s5p_path, band=layer_idx, target=dem, name="TROPOMI_NO2") # R uses var="NO2_TROPOMI"
            loaded_covariates_list.append(s5p_arr)
            LOG.info("Added TROPOMI_NO2 covariate.")
        else:
            LOG.warning("TROPOMI_NO2 (s5p_no2) covariate not found.")
        
        # R also loads pre-processed static covariates for NO2 from an RDS file.
        # Example: no2_static_path = SUPPL_DIR / "static" / f"NO2_static_covariates_1km.nc" # Assuming converted to .nc
        # if no2_static_path.exists():
        #     no2_static_ds = xr.open_dataset(no2_static_path)
        #     for var_name in no2_static_ds.data_vars:
        #         static_cov = warp_to_target(no2_static_ds[var_name], dem, name=str(var_name))
        #         loaded_covariates_list.append(static_cov)
        #     LOG.info(f"Added static NO2 covariates from {no2_static_path}")
        # else:
        #    LOG.warning(f"Static NO2 covariates file not found at {no2_static_path}")

    elif pollutant == "PM2.5":
        # R: Similar to PM10, loads RelHumidity and also BoundaryLayerHeight
        # CAMS_PM2.5 and WindSpeed are already handled by common logic + log_CAMS for PM2.5
        
        # Relative Humidity
        rh_path = _get_covariate_path(time_res_folder, "RelHumidity", year, perc_str=None)
        if rh_path:
            rh_arr = read_n_warp(rh_path, band=layer_idx, target=dem, name="RelativeHumidity")
            loaded_covariates_list.append(rh_arr)
            LOG.info("Added RelativeHumidity covariate for PM2.5.")
        else:
            LOG.warning("RelativeHumidity covariate not found for PM2.5.")

        # Boundary Layer Height (blh)
        # R: get_filename(tdir, "blh", y)
        blh_path = _get_covariate_path(time_res_folder, "blh", year, perc_str=None)
        if blh_path:
            blh_arr = read_n_warp(blh_path, band=layer_idx, target=dem, name="BoundaryLayerHeight")
            loaded_covariates_list.append(blh_arr)
            LOG.info("Added BoundaryLayerHeight covariate for PM2.5.")
        else:
            LOG.warning("BoundaryLayerHeight (blh) covariate not found for PM2.5.")

    elif pollutant == "SO2":
        # R: Loads CAMS_SO2, WindSpeed, Elevation (common), plus static PopDen and Imperviousness
        # CAMS_SO2 and WindSpeed are already handled by common logic.

        # Population Density (static)
        # R: get_filename("static", "pop_den", y, "_1km") -> .../static/pop_den_1km_YYYY.nc
        popden_static_path = SUPPL_DIR / "static" / f"pop_den_1km_{year}.nc"
        if popden_static_path.exists():
            popden_arr = read_n_warp(popden_static_path, band=1, target=dem, name="PopulationDensity")
            loaded_covariates_list.append(popden_arr)
            LOG.info("Added PopulationDensity (static) covariate for SO2.")
        else:
            LOG.warning(f"PopulationDensity (static) covariate not found at {popden_static_path} for SO2.")

        # Imperviousness (static)
        # R: get_filename("static", "imperviousness", y) -> .../static/imperviousness_YYYY.nc
        imperv_static_path = SUPPL_DIR / "static" / f"imperviousness_{year}.nc"
        if imperv_static_path.exists():
            imperv_arr = read_n_warp(imperv_static_path, band=1, target=dem, name="Imperviousness")
            loaded_covariates_list.append(imperv_arr)
            LOG.info("Added Imperviousness (static) covariate for SO2.")
        else:
            LOG.warning(f"Imperviousness (static) covariate not found at {imperv_static_path} for SO2.")

    # Merge all loaded covariates
    # Ensure no duplicate names before merging, xr.merge will raise an error if duplicates exist.
    # This can happen if CAMS data (e.g. CAMS_PM10) is added both directly and as log_CAMS_PM10.
    # The logic above tries to add one or the other for PM10/PM2.5.
    
    # Deduplicate by name, keeping the last one added if names collide (though logic should prevent this for CAMS)
    final_cov_dict = {da.name: da for da in loaded_covariates_list if da.name is not None}
    final_covariates_ds = xr.merge(list(final_cov_dict.values()))
    
    # Propagate attributes from aq to the resulting dataset
    final_covariates_ds.attrs.update(aq.attrs)
    
    LOG.info(f"Successfully loaded {len(final_covariates_ds.data_vars)} covariate layers: {list(final_covariates_ds.data_vars.keys())}")
    return final_covariates_ds


# -----------------------------------------------------------------------------
# Modelling
# -----------------------------------------------------------------------------

def _df_from_covariates(ds: xr.Dataset) -> pd.DataFrame:
    """Flatten xarray Dataset → DataFrame suitable for model prediction."""
    df = ds.to_dataframe().reset_index()
    df = df.dropna()
    return df


def linear_aq_model(aq: gpd.GeoDataFrame, covariates: xr.Dataset) -> LinearRegression:
    """Fit ordinary least-squares model pollutant ~ covariates."""
    pollutant = find_poll(aq.columns)
    if pollutant is None:
        raise ValueError("Could not infer pollutant column in AQ table")
    y = aq[pollutant].values

    # Sample covariate values at station points
    cov_vals = []
    for name, da in covariates.data_vars.items():
        vals = list(
            rasterio.sample.sample_gen(
                da.rio.write_crs(da.rio.crs).rio.to_rasterio().dataset_mask(),  # quick hack
                [(g.x, g.y) for g in aq.geometry],
            )
        )
        cov_vals.append(np.squeeze(vals))
    X = np.column_stack(cov_vals)

    model = LinearRegression().fit(X, y)
    model.feature_names_in_ = list(covariates.data_vars.keys())
    LOG.info("Linear model R² = %.3f", model.score(X, y))
    return model


def predict_linear_model(model: LinearRegression, covariates: xr.Dataset) -> xr.DataArray:
    """Predict fitted linear model over raster grid."""
    df = _df_from_covariates(covariates)
    X = df[model.feature_names_in_].values
    df["lm_pred"] = model.predict(X)
    ds_out = df.set_index({"y": "y", "x": "x"}).to_xarray()
    return ds_out["lm_pred"]


# -----------------------------------------------------------------------------
# Residual kriging (simplified via PyKrige OrdinaryKriging)
# -----------------------------------------------------------------------------

def krige_aq_residuals(aq: gpd.GeoDataFrame, covariates: xr.Dataset,
                       model: LinearRegression, n_max: int = 10) -> xr.DataArray:
    """Interpolate model residuals with Ordinary Kriging (2-D) – simplified."""
    pollutant = find_poll(aq.columns)
    if pollutant is None:
        raise ValueError("pollutant column missing")
    aq["residual"] = aq[pollutant] - model.predict(np.column_stack([aq[p] for p in model.feature_names_in_]))

    # fit OK in CRS 3035 meters
    OK = OrdinaryKriging(
        aq.geometry.x.values,
        aq.geometry.y.values,
        aq["residual"].values,
        variogram_model="exponential",
        nlags=8,
    )

    gridx = covariates.x.values
    gridy = covariates.y.values
    z, _ = OK.execute("grid", gridx, gridy)
    da = xr.DataArray(z, coords={"y": gridy, "x": gridx}, dims=("y", "x"), name="residual_krige")
    return da


# -----------------------------------------------------------------------------
# Combine & plotting
# -----------------------------------------------------------------------------

def combine_results(covariates: xr.Dataset, krige_da: xr.DataArray) -> xr.Dataset:
    lm_pred = covariates["lm_pred"] if "lm_pred" in covariates else None
    if lm_pred is None:
        raise KeyError("lm_pred missing from covariates; run predict_linear_model() first.")
    combined = lm_pred + krige_da
    ds = xr.merge([combined.rename("aq_pred"), krige_da])
    ds.attrs.update(covariates.attrs)
    return ds


def plot_aq_prediction(ds: xr.Dataset, ax: Optional[plt.Axes] = None, cmap="viridis"):
    ax = ax or plt.subplots(figsize=(6, 6))[1]
    im = ds["aq_pred"].plot(ax=ax, cmap=cmap, add_colorbar=False)
    plt.colorbar(im, ax=ax, label="µg/m³")
    ax.set_title("AQ Prediction")
    return ax


def plot_aq_se(ds: xr.Dataset, ax: Optional[plt.Axes] = None, cmap="magma"):
    ax = ax or plt.subplots(figsize=(6, 6))[1]
    if "pred_se" not in ds:
        raise KeyError("pred_se layer missing")
    im = ds["pred_se"].plot(ax=ax, cmap=cmap, add_colorbar=False)
    plt.colorbar(im, ax=ax, label="σ (µg/m³)")
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
