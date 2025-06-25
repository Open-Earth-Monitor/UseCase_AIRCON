# /// script
# dependencies = [
#   "pandas==1.5.3",
#   "xarray==0.16.2",
#   "geopandas==0.13.2",
#   "scikit-learn==1.3.2",
#   "numpy==1.22.4",
#   "pykrige",
#   "netcdf4",
#   "joblib",
#   "pyproj",
#   "shapely"
# ]
# ///

import geopandas
import pandas as pd
import logging
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube
from openeo_udf.api.feature_collection import FeatureCollection
from sklearn.linear_model import LinearRegression
from pykrige.ok import OrdinaryKriging
from typing import Optional
from shapely.geometry import box

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

# --- Helper Functions (copied from functions.py) ---

def linear_aq_model(aq: geopandas.GeoDataFrame, covariates: xr.Dataset) -> Optional[LinearRegression]:
    pollutant = aq.attrs.get("pollutant")
    if not pollutant:
        LOG.error("Attribute 'pollutant' missing from input GeoDataFrame 'aq'.")
        return None
    if pollutant not in aq.columns:
        LOG.error(f"Target pollutant column '{pollutant}' not found in GeoDataFrame 'aq'.")
        return None

    LOG.info(f"Preparing data for linear model for pollutant: {pollutant}")
    LOG.info(f"Number of stations received: {len(aq)}")
    LOG.info(f"Station data head:\n{aq.head().to_string()}")
    LOG.info(f"Covariate metadata: {covariates}")

    valid_feature_names = list(covariates.data_vars)
    cov_vals_list = []

    # Point-wise sampling of covariates
    for idx, station in aq.iterrows():
        try:
            selection = covariates.sel(
                x=station.geometry.x,
                y=station.geometry.y,
                method="nearest"
            )
            cov_vals = {var: selection[var].item() for var in valid_feature_names}
            if idx < 5:  # Log first 5 samples
                LOG.info(f"Sampled covariates for station {idx} at ({station.geometry.x}, {station.geometry.y}): {cov_vals}")
        except Exception as e:
            LOG.warning(f"Could not sample covariates for station {idx}: {e}")
            cov_vals = {var: np.nan for var in valid_feature_names}
        
        cov_vals_list.append(cov_vals)

    if not cov_vals_list:
        LOG.error("Covariate sampling resulted in an empty list.")
        return None

    # Convert to DataFrame for model fitting
    df = pd.DataFrame(cov_vals_list, columns=valid_feature_names)
    df[pollutant] = aq[pollutant].values

    LOG.info(f"DataFrame before NaN removal (head):\n{df.head().to_string()}")
    LOG.info(f"NaN count per column:\n{df.isnull().sum().to_string()}")

    # Drop rows with NaN in target or features
    df_clean = df.dropna()
    LOG.info(f"Number of stations for model fitting after NaN removal: {len(df_clean)}")

    if df_clean.empty:
        LOG.error("No valid data remains after NaN removal.")
        return None

    X = df_clean[valid_feature_names]
    y = df_clean[pollutant]

    model = LinearRegression()
    model.fit(X, y)
    LOG.info(f"Fitted linear model with coefficients: {model.coef_}")

    # Add residuals to the original GeoDataFrame
    predictions = model.predict(X)
    residuals = y - predictions
    
    # Create a new column 'residual' in the original GeoDataFrame, filled with NaNs
    aq['residual'] = np.nan
    # Place the calculated residuals at the correct indices (of the non-NaN rows)
    aq.loc[df_clean.index, 'residual'] = residuals
    
    return model

def predict_linear_model(model: LinearRegression, covariates: xr.Dataset) -> xr.DataArray:
    """
    Predicts values over a grid using a trained linear model.
    This function is designed to work entirely with xarray to avoid pandas version conflicts.
    """
    LOG.info("Starting grid prediction with linear model.")
    
    # Ensure the covariates are in the same order as the model's features
    feature_names = list(model.feature_names_in_)
    covariates_ordered = covariates[feature_names]

    # Stack all spatial and temporal dimensions to create a single "sample" dimension
    sample_dims = list(covariates_ordered.dims)
    stacked_covariates = covariates_ordered.to_array(dim="variable").stack(sample=sample_dims)
    
    # Transpose to get the shape (n_samples, n_features) that scikit-learn expects
    X_for_prediction = stacked_covariates.transpose("sample", "variable")

    # Drop samples (pixels) with any NaN values
    X_complete = X_for_prediction.dropna(dim="sample", how="any")
    LOG.info(f"Predicting on {len(X_complete.sample)} valid data points out of {len(X_for_prediction.sample)} total.")

    if len(X_complete.sample) == 0:
        LOG.warning("No valid data points for prediction after dropping NaNs.")
        # Return an array of NaNs with the correct shape
        return xr.full_like(covariates.isel(drop=True, **{list(covariates.data_vars)[0]: 0}), np.nan)

    # Predict on the valid data points
    predictions_values = model.predict(X_complete.values)

    # Create a new DataArray to hold the predictions
    predictions_da = xr.DataArray(
        predictions_values,
        coords={"sample": X_complete.coords["sample"]},
        dims=["sample"]
    )

    # Create a full-sized DataArray with NaNs, then fill in the predicted values
    lm_pred_stacked = predictions_da.reindex_like(X_for_prediction.isel(variable=0, drop=True))
    
    # Unstack the "sample" dimension to restore the original spatial grid
    lm_pred = lm_pred_stacked.unstack("sample")
    lm_pred.name = "lm_pred"
    
    LOG.info("Finished grid prediction.")
    return lm_pred

def krige_aq_residuals(aq: geopandas.GeoDataFrame, covariates: xr.Dataset) -> xr.Dataset:
    if "residual" not in aq.columns:
        LOG.error("Input 'aq' must contain a 'residual' column.")
        return xr.Dataset()

    valid_aq = aq[aq["residual"].notna() & aq.geometry.is_valid].copy()
    if valid_aq.empty:
        LOG.warning("No valid data points for kriging.")
        return xr.Dataset()

    LOG.info(f"Performing Ordinary Kriging with {len(valid_aq)} points.")
    OK = OrdinaryKriging(
        valid_aq.geometry.x.values, valid_aq.geometry.y.values, valid_aq["residual"].values,
        variogram_model="spherical", nlags=8
    )
    
    gridx = covariates.x.values
    gridy = covariates.y.values
    z, ss = OK.execute("grid", gridx, gridy)
    
    kriged_ds = xr.Dataset(
        {"pred": (( 'y', 'x'), z), "se": (('y', 'x'), np.sqrt(ss.clip(min=0)))},
        coords={'y': gridy, 'x': gridx}
    )
    return kriged_ds

# --- Main UDF Function ---

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    # The UDF receives a DataArray where different bands are stacked in one dimension.
    # The helper functions, however, expect an xarray.Dataset where each band is a variable.
    # Here, we convert the DataArray to a Dataset to ensure compatibility.
    covariates_da = cube.get_array()
    covariates_ds = covariates_da.to_dataset(dim='bands')
    LOG.info(f"Converted datacube to dataset with variables: {list(covariates_ds.data_vars)}")

    # The station data is passed in the context as a FeatureCollection
    # The backend passes the vector cube directly as the context object, not in a dict.
    station_fc = context
    if not station_fc:
        raise ValueError("Vector geometries (station data) not found in context.")

    # Convert the vector cube object to a GeoDataFrame.
    # The backend provides an object that can be directly consumed by from_features.
    aq_gdf = geopandas.GeoDataFrame.from_features(context.to_geojson())
    
    # Assuming the pollutant is passed in context or can be inferred
    pollutant = 'O3'  # Hard-coded for now, as context is a DriverVectorCube
    aq_gdf.attrs['pollutant'] = pollutant

    # --- Spatial Filtering within UDF ---
    # Since backend filtering of vector data is unreliable, we filter the station
    # data here to match the extent of the raster data cube.

    # Get the bounding box from the raster data cube's coordinates.
    min_x, max_x = covariates_ds.x.min().item(), covariates_ds.x.max().item()
    min_y, max_y = covariates_ds.y.min().item(), covariates_ds.y.max().item()

    # Create a bounding box geometry from the raster's extent.
    raster_bbox = box(min_x, min_y, max_x, max_y)
    LOG.info(f"Clipping station data to raster extent: {raster_bbox.bounds}")

    # The GeoDataFrame created from GeoJSON should have its CRS set.
    # We assume the raster data and vector data are in the same CRS,
    # as is expected from the openEO backend.
    
    # Clip the GeoDataFrame using the raster's bounding box.
    original_count = len(aq_gdf)
    aq_gdf = geopandas.clip(aq_gdf, raster_bbox)
    LOG.info(f"Clipped stations: {original_count} -> {len(aq_gdf)}")

    if aq_gdf.empty:
        raise ValueError("No station data remains after clipping to the raster extent. Check data alignment and CRS.")
    # --- End Spatial Filtering ---


    # 1. Fit linear model and get residuals
    LOG.info("Fitting linear model...")
    lm = linear_aq_model(aq_gdf, covariates_ds)
    if lm is None:
        raise RuntimeError("Failed to fit linear model.")

    # 2. Predict linear model over the grid
    LOG.info("Predicting with linear model...")
    lm_pred = predict_linear_model(lm, covariates_ds)

    # 3. Krige the residuals
    LOG.info("Kriging residuals...")
    kriged_residuals = krige_aq_residuals(aq_gdf, covariates_ds) # aq_gdf now has 'residual' column
    
    if 'pred' not in kriged_residuals:
        raise RuntimeError("Failed to krige residuals.")

    # 4. Combine predictions
    LOG.info("Combining linear model prediction and kriged residuals...")
    final_prediction = lm_pred + kriged_residuals['pred']
    final_prediction.name = "aq_prediction_final"

    return XarrayDataCube(final_prediction)
