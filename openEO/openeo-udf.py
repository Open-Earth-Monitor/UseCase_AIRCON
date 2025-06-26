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
from pyproj import CRS

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

    # --- Robust Covariate Sampling and NaN Handling ---
    LOG.info("Starting robust covariate sampling.")
    
    cov_vals_list = []
    valid_feature_names = []

    if aq.geometry.empty or aq.geometry.is_empty.all():
        raise ValueError("Input GeoDataFrame 'aq' has no valid geometries for sampling.")
        
    station_x = xr.DataArray(aq.geometry.x, dims="station_id")
    station_y = xr.DataArray(aq.geometry.y, dims="station_id")

    # 1. Sample each covariate, handling potential errors
    for cov_name, da in covariates.data_vars.items():
        try:
            # Ensure da is 2D for sampling
            if 'band' in da.dims: da = da.isel(band=0)
            if not (da.ndim == 2 and 'x' in da.dims and 'y' in da.dims):
                 raise ValueError(f"Covariate '{cov_name}' not 2D (y,x), dims: {da.dims}")

            sampled_points = da.sel(x=station_x, y=station_y, method="nearest")
            cov_vals_list.append(sampled_points.to_numpy())
            valid_feature_names.append(cov_name)
        except Exception as e:
            LOG.warning(f"Failed to sample covariate '{cov_name}': {e}. Appending NaNs.")
            cov_vals_list.append(np.full(station_x.size, np.nan))
            valid_feature_names.append(f"{cov_name}_SKIPPED") # Mark as skipped

    if not cov_vals_list:
        raise ValueError("No covariate values could be extracted.")

    # 2. Two-stage NaN removal (mimicking R's na.omit more robustly)
    X_candidate = np.column_stack(cov_vals_list)
    y_candidate = aq[pollutant].values

    # Stage 1: Filter out columns that are ALL NaN (from failed covariates)
    all_nan_cols_mask = np.isnan(X_candidate).all(axis=0)
    if all_nan_cols_mask.all():
        raise ValueError("All covariate columns are entirely NaN after sampling.")
    
    X_filtered_cols = X_candidate[:, ~all_nan_cols_mask]
    final_feature_names = [name for i, name in enumerate(valid_feature_names) if not all_nan_cols_mask[i]]
    LOG.info(f"Using features for model: {final_feature_names}")

    # Stage 2: Filter out rows with ANY NaN (in remaining features or target variable)
    nan_mask_X_rows = np.isnan(X_filtered_cols).any(axis=1)
    nan_mask_y_rows = np.isnan(y_candidate)
    combined_nan_mask_rows = nan_mask_X_rows | nan_mask_y_rows
    
    X_final = X_filtered_cols[~combined_nan_mask_rows]
    y_final = y_candidate[~combined_nan_mask_rows]

    # Also filter the original GeoDataFrame to keep indices aligned for residual calculation
    aq_filtered = aq[~combined_nan_mask_rows]

    if X_final.shape[0] == 0:
        raise ValueError("After NaN removal, no valid data remains.")

    # 3. Fit the linear regression model
    model = LinearRegression()
    model.fit(X_final, y_final)
    model.feature_names_in_ = final_feature_names # Store feature names

    # 4. Calculate and store residuals in the filtered GeoDataFrame
    # Initialize residual column with NaNs
    aq['residual'] = np.nan
    predictions = model.predict(X_final)
    residuals = y_final - predictions
    # Assign back to original df using index of the filtered (non-NaN) rows
    aq.loc[aq_filtered.index, 'residual'] = residuals 

    LOG.info(f"Successfully fitted linear model. Residuals calculated for {len(aq_filtered)} stations.")
    
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

def _harmonize_covariate_grids(ds: xr.Dataset) -> xr.Dataset:
    """
    Ensures all DataArrays in an xarray.Dataset share the same spatial grid.

    Selects the grid of the first variable as the 'master' and reindexes all other
    variables to match it using nearest-neighbor sampling.

    Args:
        ds: The input Dataset with potentially misaligned grids.

    Returns:
        A new Dataset where all data variables are aligned to the same grid.
    """
    data_vars = list(ds.data_vars)
    if not data_vars:
        LOG.info("Dataset is empty, no harmonization needed.")
        return ds

    master_grid = ds[data_vars[0]]
    LOG.info(f"Harmonizing all covariates to the grid of '{data_vars[0]}'.")

    harmonized_vars = {}
    for var_name in data_vars:
        if var_name == data_vars[0]:
            harmonized_vars[var_name] = ds[var_name]
            continue
        
        LOG.info(f"Reindexing '{var_name}'...")
        # Use reindex_like for robust, pure-xarray grid alignment
        harmonized_vars[var_name] = ds[var_name].reindex_like(master_grid, method="nearest")

    # Create a new dataset from the harmonized variables
    harmonized_ds = xr.Dataset(harmonized_vars)
    harmonized_ds.attrs.update(ds.attrs)
    
    LOG.info("Grid harmonization complete.")
    return harmonized_ds


# --- Main UDF Function ---

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    # The UDF receives a DataArray where different bands are stacked in one dimension.
    # The helper functions, however, expect an xarray.Dataset where each band is a variable.
    # Here, we convert the DataArray to a Dataset to ensure compatibility.
    covariates_da = cube.get_array()
    covariates_ds = covariates_da.to_dataset(dim='bands')
    LOG.info(f"Converted datacube to dataset with variables: {list(covariates_ds.data_vars)}")

    # --- Grid Harmonization ---
    # Ensure all raster layers are on the same grid before any processing
    covariates_ds = _harmonize_covariate_grids(covariates_ds)

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

    # --- CRS and Spatial Alignment ---
    LOG.info("Starting CRS and spatial alignment.")
    LOG.info(f"Initial station data count: {len(aq_gdf)}")
    LOG.info(f"Initial station CRS: {aq_gdf.crs}")
    LOG.info(f"Covariate coords: {covariates_ds.coords}")
    LOG.info(f"Covariate attrs: {covariates_ds.attrs}")

    try:
        # OpenEO typically adds CRS info in a 'spatial_ref' coordinate.
        raster_crs_info = covariates_ds.coords.get('spatial_ref')
        if raster_crs_info is not None:
            raster_crs = raster_crs_info.attrs.get('crs_wkt')
            if not raster_crs:
                raise ValueError("Found 'spatial_ref' coordinate but it lacks 'crs_wkt' attribute.")
            LOG.info(f"Raster CRS detected from 'spatial_ref' coordinate.")

            vector_crs = aq_gdf.crs
            if not vector_crs:
                LOG.warning("Vector data has no CRS information. Assuming WGS84 (EPSG:4326).")
                aq_gdf.set_crs("EPSG:4326", inplace=True)
                vector_crs = aq_gdf.crs
            LOG.info(f"Vector CRS from GeoDataFrame: {vector_crs}")
            
            if not CRS(vector_crs).equals(CRS(raster_crs)):
                LOG.info(f"Reprojecting vector data from {vector_crs} to match raster CRS.")
                aq_gdf = aq_gdf.to_crs(raster_crs)
                LOG.info(f"Vector data reprojected. New CRS: {aq_gdf.crs}")
            else:
                LOG.info("Vector and Raster CRSs already match.")
        else:
            LOG.warning("Could not find 'spatial_ref' coordinate in raster data. Assuming WGS84 (EPSG:4326) as a fallback, as this is common for AGERA5.")
            raster_crs = "EPSG:4326"
            vector_crs = aq_gdf.crs
            if not vector_crs:
                LOG.warning("Vector data has no CRS information. Assuming WGS84 (EPSG:4326).")
                aq_gdf.set_crs("EPSG:4326", inplace=True)
                vector_crs = aq_gdf.crs

            if not CRS(vector_crs).equals(CRS(raster_crs)):
                LOG.info(f"Reprojecting vector data from {vector_crs} to match assumed raster CRS (EPSG:3035).")
                aq_gdf = aq_gdf.to_crs(raster_crs)
                LOG.info(f"Vector data reprojected. New CRS: {aq_gdf.crs}")
            else:
                LOG.info("Vector CRS is already the assumed raster CRS (EPSG:3035).")

    except Exception as e:
        LOG.error(f"An error occurred during CRS alignment: {e}. Proceeding without reprojection, but results may be incorrect.")

    # --- Spatial Filtering within UDF ---
    LOG.info(f"Preparing for spatial clipping. Vector CRS: {aq_gdf.crs}, Vector bounds: {aq_gdf.total_bounds}")
    LOG.info(f"Raster CRS used for clipping: {raster_crs}")
    min_x, max_x = covariates_ds.x.min().item(), covariates_ds.x.max().item()
    min_y, max_y = covariates_ds.y.min().item(), covariates_ds.y.max().item()
    raster_bbox = box(min_x, min_y, max_x, max_y)
    LOG.info(f"Clipping station data to raster extent: {raster_bbox.bounds}")

    original_count = len(aq_gdf)
    aq_gdf = geopandas.clip(aq_gdf, raster_bbox)
    LOG.info(f"Clipped stations: {original_count} -> {len(aq_gdf)}")

    if aq_gdf.empty:
        raise ValueError("No station data remains after clipping to the raster extent. Check data alignment and CRS.")
    # --- End Spatial Alignment and Filtering ---

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
