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
>>> from functions_py import load_aq, load_covariates, linear_aq_model
>>> aq = load_aq("PM10", "perc", y=2015, m=1)
>>> dem = rioxarray.open_rasterio("supplementary/static/COP-DEM/... .tif")
>>> cov = load_covariates(aq, dem)
>>> model = linear_aq_model(aq, cov)
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

    gdf.attrs.update({"stat": stat, "y": y, "m": m, "d": d})
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


def load_covariates(aq: gpd.GeoDataFrame, dem: xr.DataArray) -> xr.Dataset:
    """Replicate R `load_covariates()` (simplified).

    The R function contains a large amount of pollutant-specific logic.  Here we
    demonstrate core mechanics – reading DEM, CAMS, WS, etc. – and return an
    xarray.Dataset whose grid matches *dem*.
    """
    cfg = CovariateConfig(
        pollutant=find_poll(aq.columns) or "PM10",
        stat=aq.attrs.get("stat", "perc"),
        y=aq.attrs.get("y", 2015),
        m=aq.attrs.get("m", 0),
        d=aq.attrs.get("d", 0),
    )

    # Example: load CAMS and wind-speed netCDFs following the naming convention.
    cams_nc = SUPPL_DIR / "cams" / f"cams_{cfg.pollutant.lower()}_{cfg.y}.nc"
    ws_nc = SUPPL_DIR / "wind" / f"wind_speed_{cfg.y}.nc"

    covs: List[xr.DataArray] = []
    if cams_nc.exists():
        cams = rxr.open_rasterio(cams_nc, masked=True).sel(band=cfg.m or 1)
        covs.append(warp_to_target(cams, dem, name=f"CAMS_{cfg.pollutant}"))
    if ws_nc.exists():
        ws = rxr.open_rasterio(ws_nc, masked=True).sel(band=cfg.m or 1)
        covs.append(warp_to_target(ws, dem, name="WindSpeed"))

    covs.append(dem.rename("Elevation"))

    ds = xr.merge(covs)
    ds.attrs.update(cfg.__dict__)
    LOG.info("Loaded %d covariate layers", len(ds.data_vars))
    return ds


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
