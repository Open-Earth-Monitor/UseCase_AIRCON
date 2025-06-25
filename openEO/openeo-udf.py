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
#   "pyproj"
# ]
# ///

import geopandas
import logging
import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube
from openeo_udf.api.feature_collection import FeatureCollection
from sklearn.linear_model import LinearRegression
from pykrige.ok import OrdinaryKriging
from typing import Optional

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
    cov_vals_list = []
    valid_feature_names = []

    if aq.geometry.empty or aq.geometry.is_empty.all():
        LOG.error("Input GeoDataFrame 'aq' has no valid geometries for sampling.")
        return None

    station_x = xr.DataArray([g.x for g in aq.geometry], dims="station_id")
    station_y = xr.DataArray([g.y for g in aq.geometry], dims="station_id")

    for cov_name, da in covariates.data_vars.items():
        try:
            sampled_points = da.sel(x=station_x, y=station_y, method="nearest")
            cov_vals_list.append(sampled_points.to_numpy())
            valid_feature_names.append(cov_name)
        except Exception as e:
            LOG.error(f"Failed to sample covariate '{cov_name}': {e}. Appending NaNs.")
            cov_vals_list.append(np.full(station_x.size, np.nan))

    X_candidate = np.column_stack(cov_vals_list)
    y_candidate = aq[pollutant].values

    nan_mask = np.isnan(X_candidate).any(axis=1) | np.isnan(y_candidate)
    X_final = X_candidate[~nan_mask]
    y_final = y_candidate[~nan_mask]

    if X_final.shape[0] == 0:
        LOG.error("No valid data remains after NaN removal.")
        return None

    model = LinearRegression()
    model.fit(X_final, y_final)
    model.feature_names_in_ = valid_feature_names

    # Calculate residuals and add to GeoDataFrame
    predictions_at_stations = model.predict(X_final)
    residuals = y_final - predictions_at_stations
    aq_filtered = aq[~nan_mask].copy()
    aq_filtered['residual'] = residuals
    aq.loc[aq_filtered.index, 'residual'] = residuals

    return model

def predict_linear_model(model: LinearRegression, covariates: xr.Dataset) -> xr.DataArray:
    df_full = covariates.to_dataframe().reset_index()
    feature_names = list(model.feature_names_in_)
    X_for_prediction = df_full[feature_names]
    
    predictions = np.full(len(df_full), np.nan)
    mask_complete_rows = ~np.isnan(X_for_prediction.values).any(axis=1)
    
    if np.any(mask_complete_rows):
        predictions[mask_complete_rows] = model.predict(X_for_prediction[mask_complete_rows])

    df_full["lm_pred"] = predictions
    df_indexed = df_full.set_index([dim for dim in covariates.dims if dim in df_full.columns])
    return df_indexed.to_xarray()["lm_pred"]

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
    covariates_array = cube.get_array()
    # The .openeo accessor seems to be unavailable in the backend environment.
    # We assume the band dimension is named 'bands' by convention.
    band_dim = "bands"
    covariates_dataset = covariates_array.to_dataset(dim=band_dim)

    # The station data is passed in the context as a FeatureCollection
    # The backend passes the vector cube directly as the context object, not in a dict.
    station_fc = context
    if not station_fc:
        raise ValueError("Vector geometries (station data) not found in context.")

    # Convert FeatureCollection to GeoDataFrame
    features = station_fc.to_dict()['features']
    aq_gdf = geopandas.GeoDataFrame.from_features(features)
    
    # Assuming the pollutant is passed in context or can be inferred
    pollutant = context.get('pollutant', 'O3') # Default to O3 if not specified
    aq_gdf.attrs['pollutant'] = pollutant

    # 1. Fit linear model and get residuals
    LOG.info("Fitting linear model...")
    lm = linear_aq_model(aq_gdf, covariates_dataset)
    if lm is None:
        raise RuntimeError("Failed to fit linear model.")

    # 2. Predict linear model over the grid
    LOG.info("Predicting with linear model...")
    lm_pred = predict_linear_model(lm, covariates_dataset)

    # 3. Krige the residuals
    LOG.info("Kriging residuals...")
    kriged_residuals = krige_aq_residuals(aq_gdf, covariates_dataset) # aq_gdf now has 'residual' column
    
    if 'pred' not in kriged_residuals:
        raise RuntimeError("Failed to krige residuals.")

    # 4. Combine predictions
    LOG.info("Combining linear model prediction and kriged residuals...")
    final_prediction = lm_pred + kriged_residuals['pred']
    final_prediction.name = "aq_prediction_final"

    return XarrayDataCube(final_prediction)
