import os
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import xarray as xr
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from pathlib import Path
import warnings

def find_poll(x):
    """Find pollutant from the column names."""
    n = x.columns if hasattr(x, 'columns') else list(x.keys())
    poll = None
    for p in ["PM10", "PM2.5", "O3", "NO2", "SO2"]:
        if p in n:
            poll = p
    return poll

def add_meta(aq):
    """Add metadata to the air quality dataframe."""
    if not any(col in aq.columns for col in ["Countrycode", "Station.Type", "Station.Area", 
                                           "Longitude", "Latitude"]):
        meta_data = pq.read_table("AQ_stations/EEA_stations_meta_table.parquet").to_pandas()
        by = "Air.Quality.Station.EoI.Code"
        aq = pd.merge(aq, meta_data, on=by, how="left")
    return aq

def create_dir_if(paths, recursive=True):
    """Create directories if they don't exist."""
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

# Interpolation functions
def load_aq(poll, stat, y, m=0, d=0, sf=True, verbose=True):
    """Load air quality data for given pollutant, statistic, and time."""
    if not (m == 0 or d == 0):
        raise ValueError("Supply only either m (month, 1-12) or d (day of year; 1-365).")
    if poll not in ["PM10", "PM2.5", "O3", "NO2", "SO2"]:
        raise ValueError("Select one of PM10, PM2.5, O3, NO2, SO2.")
    if stat not in ["perc", "mean", "max"]:
        raise ValueError("Select one of mean, max or perc.")
    
    tdir = os.path.join("AQ_data", "03_annual" if (m == 0 and d == 0) else "02_monthly" if d == 0 else "01_daily")
    
    info = f"{os.path.basename(tdir)[3:].upper()} {poll} data. Year = {y}"
    
    # Pattern for glob matching
    pattern = f"{poll}*{stat}*"
    
    # daily
    if d > 0:
        tdir_pattern = os.path.join(tdir, pattern)
        tdir_matches = glob.glob(tdir_pattern)
        info = f"{info}; Day of Year = {d}; Variable = {os.path.basename(tdir_matches[0])}"
        dataset = ds.dataset(tdir_matches)
        x = dataset.to_table(filter=(ds.field("year") == y) & (ds.field("doy") == d)).to_pandas()
    # monthly
    elif m > 0:
        tdir_pattern = os.path.join(tdir, pattern)
        tdir_matches = glob.glob(tdir_pattern)
        assert len(tdir_matches) == 1, f"Expected exactly one match for {tdir_pattern}, found {len(tdir_matches)}"
        info = f"{info}; Month = {m}; Variable = {os.path.basename(tdir_matches[0])}"
        dataset = ds.dataset(tdir_matches[0])
        x = dataset.to_table(filter=(ds.field("year") == y) & (ds.field("month") == m)).to_pandas()
    # annual
    else:
        tdir_pattern = os.path.join(tdir, pattern)
        tdir_matches = glob.glob(tdir_pattern)
        assert len(tdir_matches) == 1, f"Expected exactly one match for {tdir_pattern}, found {len(tdir_matches)}"
        info = f"{info}; Variable = {os.path.basename(tdir_matches[0])}"
        dataset = ds.dataset(tdir_matches[0])
        x = dataset.to_table(filter=(ds.field("year") == y)).to_pandas()
    
    x = add_meta(x)
    
    if sf:
        x = gpd.GeoDataFrame(x, geometry=gpd.points_from_xy(x.Longitude, x.Latitude), crs="EPSG:4326")
        x = x.to_crs("EPSG:3035")
    
    # Store attributes as metadata in the DataFrame
    x.attrs = {
        "stat": stat,
        "y": y,
        "m": m,
        "d": d
    }
    
    if verbose:
        print(f"{info}; n = {len(x)}")
    
    return x

def get_filename(tdir, varname, y, perc=None, base_dir="supplementary"):
    """Get filename for a variable."""
    varname = varname.replace(".", "p")
    pattern = f"{varname.lower()}*{perc or ''}*{y}.nc"
    matching_files = glob.glob(os.path.join(base_dir, tdir, pattern))
    if matching_files:
        return matching_files[0]
    return None

def warp_to_target_rasterio(x, target, name, method="bilinear", mask=False, to_xarray=False):
    """Warp raster to target using rasterio."""
    # Use rasterio to reproject raster
    with rasterio.open(x) if isinstance(x, str) else x as src:
        if isinstance(target, str):
            with rasterio.open(target) as target_src:
                target_transform = target_src.transform
                target_crs = target_src.crs
                target_width = target_src.width
                target_height = target_src.height
                target_data = target_src.read(1) if mask else None
        else:
            target_transform = target.transform
            target_crs = target.crs
            target_width = target.width
            target_height = target.height
            target_data = target.read(1) if mask else None
        
        # Calculate transform
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, 
            *src.bounds, dst_width=target_width, dst_height=target_height
        )
        
        # Create destination raster
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': target_crs,
            'transform': target_transform,
            'width': target_width,
            'height': target_height
        })
        
        # Create output array
        dst_data = np.zeros((height, width), dtype=kwargs['dtype'])
        
        # Reproject
        reproject(
            source=rasterio.band(src, 1),
            destination=dst_data,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=target_transform,
            dst_crs=target_crs,
            resampling=getattr(Resampling, method)
        )
        
        # Apply mask if needed
        if mask and target_data is not None:
            dst_data = np.where(np.isnan(target_data), np.nan, dst_data)
    
    # Convert to xarray if requested
    if to_xarray:
        # Create xarray DataArray with correct dimensions
        result = xr.DataArray(
            dst_data,
            dims=('y', 'x'),
            coords={
                'y': np.linspace(target_transform[5], target_transform[5] + target_height * target_transform[4], target_height),
                'x': np.linspace(target_transform[0], target_transform[0] + target_width * target_transform[0], target_width)
            },
            name=name
        )
        return result
    
    return dst_data

def read_n_warp_rasterio(x, lyr, target, name, mask=True, to_xarray=False):
    """Read and warp raster data to target."""
    # Open source raster
    with rasterio.open(x) as src:
        # For multi-band/multi-layer rasters, select the specific layer
        band_idx = lyr if isinstance(lyr, int) else 1
        
    # Now warp using the warp_to_target_rasterio function
    return warp_to_target_rasterio(x, target, name, mask=mask, to_xarray=to_xarray)

def load_covariates_rasterio(x, spatial_ext=None):
    """Load covariates for a given pollutant using rasterio."""
    poll = find_poll(x)
    reso = 10 if poll == "O3" else 1
    stat = x.attrs.get("stat") if hasattr(x, "attrs") else None
    perc = "perc" if stat == "perc" else None
    y = x.attrs.get("y") if hasattr(x, "attrs") else None
    m = x.attrs.get("m") if hasattr(x, "attrs") else 0
    d = x.attrs.get("d") if hasattr(x, "attrs") else 0
    
    tdir = "01_daily" if d > 0 else "02_monthly" if m > 0 else "03_annual"
    lyr = d if d > 0 else m if m > 0 else y
    dtime = f"year = {y}" + (f", month = {m}" if m > 0 else f", doy = {d}" if d > 0 else "")
    
    # static (target grid)
    dtm_file = f"supplementary/static/COP-DEM/COP_DEM_Europe_{reso}km_mask_epsg3035.tif"
    dtm = rasterio.open(dtm_file)
    
    # Set spatial extent if provided
    if spatial_ext is not None:
        # Crop dtm to spatial extent
        from rasterio.windows import from_bounds
        window = from_bounds(*spatial_ext, dtm.transform)
        dtm = dtm.read(1, window=window)
    
    # Create a list to store all covariates
    covariates = {}
    
    # measurement-level (needed by all pollutants)
    cams_name = f"CAMS_{poll}"
    cams_file = get_filename(tdir, poll, y, perc)
    cams = read_n_warp_rasterio(cams_file, lyr, dtm, cams_name, mask=True, to_xarray=True)
    covariates[cams_name] = cams
    
    ws_file = get_filename(tdir, "wind_speed", y)
    ws = read_n_warp_rasterio(ws_file, lyr, dtm, "WindSpeed", mask=True, to_xarray=True)
    covariates["WindSpeed"] = ws
    
    # Read DEM as xarray
    with rasterio.open(dtm_file) as src:
        dtm_data = src.read(1)
        dtm_xr = xr.DataArray(
            dtm_data,
            dims=('y', 'x'),
            coords={
                'y': np.linspace(src.bounds.bottom, src.bounds.top, src.height),
                'x': np.linspace(src.bounds.left, src.bounds.right, src.width)
            },
            name="Elevation"
        )
    covariates["Elevation"] = dtm_xr
    
    # measurement-level (individual for each pollutant)
    if poll == "PM10":
        # Add log of CAMS
        covariates[f"log_{cams_name}"] = xr.apply_ufunc(np.log, cams)
        
        # Add CLC Natural land cover
        clc_nat_file = "supplementary/static/clc/CLC_NAT_percent_1km.tif"
        clc_nat = read_n_warp_rasterio(clc_nat_file, 1, dtm, "CLC_NAT_1km", mask=True, to_xarray=True)
        covariates["CLC_NAT_1km"] = clc_nat
        
        # Add relative humidity
        rh_file = get_filename(tdir, "rel_humidity", y)
        rh = read_n_warp_rasterio(rh_file, lyr, dtm, "RelHumidity", mask=True, to_xarray=True)
        covariates["RelHumidity"] = rh
        
    elif poll == "PM2.5":
        # Add log of CAMS
        covariates[f"log_{cams_name}"] = xr.apply_ufunc(np.log, cams)
        
        # Add CLC Natural land cover
        clc_nat_file = "supplementary/static/clc/CLC_NAT_percent_1km.tif"
        clc_nat = read_n_warp_rasterio(clc_nat_file, 1, dtm, "CLC_NAT_1km", mask=True, to_xarray=True)
        covariates["CLC_NAT_1km"] = clc_nat
        
    elif poll == "O3":
        # Add solar radiation
        sr_file = get_filename(tdir, "solar_radiation", y)
        sr = read_n_warp_rasterio(sr_file, lyr, dtm, "SolarRadiation", mask=True, to_xarray=True)
        covariates["SolarRadiation"] = sr
        
    elif poll == "NO2":
        # Add elevation 5km radius
        elev_5km_file = "supplementary/static/COP-DEM/COP_DEM_Europe_5km_radius_mean_1km_epsg3035.tif"
        elev_5km = read_n_warp_rasterio(elev_5km_file, 1, dtm, "Elevation_5km_radius", mask=True, to_xarray=True)
        covariates["Elevation_5km_radius"] = elev_5km
        
        # Add TROPOMI NO2
        tropomi_file = get_filename(tdir, "s5p_no2", y)
        tropomi = read_n_warp_rasterio(tropomi_file, lyr, dtm, "TROPOMI_NO2", mask=True, to_xarray=True)
        covariates["TROPOMI_NO2"] = tropomi
        
        # Add population density
        pop_file = "supplementary/static/pop_density_1km_epsg3035.tif"
        pop = read_n_warp_rasterio(pop_file, 1, dtm, "PopulationDensity", mask=True, to_xarray=True)
        covariates["PopulationDensity"] = pop
        
        # Add various CLC layers
        clc_layers = {
            "CLC_NAT_1km": "supplementary/static/clc/CLC_NAT_percent_1km.tif",
            "CLC_AGR_1km": "supplementary/static/clc/CLC_AGR_percent_1km.tif",
            "CLC_TRAF_1km": "supplementary/static/clc/CLC_TRAF_percent_1km.tif",
            "CLC_NAT_5km_radius": "supplementary/static/clc/CLC_NAT_percent_5km_radius_1km.tif",
            "CLC_LDR_5km_radius": "supplementary/static/clc/CLC_LDR_percent_5km_radius_1km.tif",
            "CLC_HDR_5km_radius": "supplementary/static/clc/CLC_HDR_percent_5km_radius_1km.tif"
        }
        
        for name, file in clc_layers.items():
            clc = read_n_warp_rasterio(file, 1, dtm, name, mask=True, to_xarray=True)
            covariates[name] = clc
    
    # Convert dictionary to Dataset
    covariates_ds = xr.Dataset(covariates)
    
    # Add metadata
    covariates_ds.attrs = {
        "pollutant": poll,
        "stat": stat,
        "dtime": dtime,
        "y": y,
        "m": m,
        "d": d
    }
    
    return covariates_ds

def filter_area_type(x, area_type="RB", mainland_europe=True):
    """Filter area by type."""
    if area_type not in ["RB", "UB", "UT", "JB"]:
        raise ValueError("area_type should be one of ['RB', 'UB', 'UT', 'JB']!")
    
    if isinstance(x, pd.DataFrame):
        if area_type == "RB":
            x = x[(x["Station.Type"] == "background") & (x["Station.Area"] == "rural")]
        elif area_type == "UB":
            x = x[(x["Station.Type"] == "background") & (x["Station.Area"] == "urban")]
        elif area_type == "UT":
            x = x[(x["Station.Type"] == "traffic") & (x["Station.Area"] == "urban")]
        elif area_type == "JB":
            x = x[(x["Station.Type"] == "background")]
        
        # Add area_type attribute
        if hasattr(x, "attrs"):
            x.attrs["area_type"] = area_type
    
    return x

def linear_aq_model_scale(aq, covariates):
    """Create a linear model for air quality prediction."""
    from sklearn.linear_model import LinearRegression
    
    if not (isinstance(aq, gpd.GeoDataFrame) and isinstance(covariates, xr.Dataset)):
        raise ValueError("Please supply a GeoDataFrame (aq) and the corresponding covariates as xarray Dataset.")
    
    poll = find_poll(aq)
    
    # Extract coordinates from aq
    coords = pd.DataFrame({
        'x': aq.geometry.x,
        'y': aq.geometry.y
    })
    
    # Extract values from covariates at the coordinates
    covar_values = {}
    for var in covariates.data_vars:
        # Extract values at coordinates
        # This is a simplification - proper extraction would require more complex spatial indexing
        values = []
        for _, row in coords.iterrows():
            # Find nearest pixel
            x_idx = np.abs(covariates.x.values - row['x']).argmin()
            y_idx = np.abs(covariates.y.values - row['y']).argmin()
            
            # Extract value
            values.append(covariates[var].values[y_idx, x_idx])
        
        covar_values[var] = values
    
    # Create DataFrame with covariate values
    covar_df = pd.DataFrame(covar_values)
    
    # Fit linear model
    lm = LinearRegression()
    X = covar_df.fillna(0)  # Replace NAs with 0 for simplicity
    y = aq[poll]
    
    lm.fit(X, y)
    
    return lm

def define_mask(aq):
    """Define a mask based on countries not in the aq dataset."""
    # This is a simplified version as giscoR is R-specific
    # You would need to download country boundaries from an appropriate source
    import geopandas as gpd
    
    # Load countries from a shapefile or other source
    # This is a placeholder - you need to provide the actual data source
    countries = gpd.read_file("path/to/countries_shapefile.shp")
    
    # Filter countries not in aq
    cnt_mask = countries[~countries["CNTR_ID"].isin(aq["Countrycode"])]
    
    # Transform to same CRS
    cnt_mask = cnt_mask.to_crs(aq.crs)
    
    return cnt_mask

def get_country_mask(aq, mainland_europe=True):
    """Get country mask for a given air quality dataset."""
    # This is a simplified version as giscoR is R-specific
    # You would need to download country boundaries from an appropriate source
    import geopandas as gpd
    
    # Load countries from a shapefile or other source
    # This is a placeholder - you need to provide the actual data source
    countries = gpd.read_file("path/to/countries_shapefile.shp")
    
    # Filter countries in aq
    cnt_mask = countries[countries["CNTR_ID"].isin(aq["Countrycode"])]
    
    # Transform to same CRS
    cnt_mask = cnt_mask.to_crs(aq.crs)
    
    return cnt_mask

def get_country_boundaries(aq, mainland_europe=True):
    """Get country boundaries for a given air quality dataset."""
    # This is a simplified version as giscoR is R-specific
    # You would need to download country boundaries from an appropriate source
    import geopandas as gpd
    
    # Load countries from a shapefile or other source
    # This is a placeholder - you need to provide the actual data source
    countries = gpd.read_file("path/to/countries_shapefile.shp")
    
    # Filter countries in aq
    cnt_bound = countries[countries["CNTR_ID"].isin(aq["Countrycode"])]
    
    # Transform to same CRS
    cnt_bound = cnt_bound.to_crs(aq.crs)
    
    return cnt_bound

def get_grid_cells(aq, mainland_europe=True):
    """Get grid cells for a given air quality dataset."""
    # This is a simplified version as giscoR is R-specific
    # You would need to download grid cells from an appropriate source
    import geopandas as gpd
    
    # Load grid cells from a shapefile or other source
    # This is a placeholder - you need to provide the actual data source
    grid_cells = gpd.read_file("path/to/grid_cells_shapefile.shp")
    
    # Filter grid cells in aq
    grid_cells = grid_cells[grid_cells.intersects(aq.unary_union)]
    
    # Transform to same CRS
    grid_cells = grid_cells.to_crs(aq.crs)
    
    return grid_cells

def get_grid_cells_elev(aq, mainland_europe=True):
    """Get grid cells elevation for a given air quality dataset."""
    # This is a simplified version as giscoR is R-specific
    # You would need to download grid cells elevation from an appropriate source
    import geopandas as gpd
    
    # Load grid cells elevation from a shapefile or other source
    # This is a placeholder - you need to provide the actual data source
    grid_cells_elev = gpd.read_file("path/to/grid_cells_elev_shapefile.shp")
    
    # Filter grid cells elevation in aq
    grid_cells_elev = grid_cells_elev[grid_cells_elev.intersects(aq.unary_union)]
    
    # Transform to same CRS
    grid_cells_elev = grid_cells_elev.to_crs(aq.crs)
    
    return grid_cells_elev

def krige_aq_residuals(x, covariates, lm, n_min=0, n_max=float('inf'), cluster=None, 
                       cv=True, show_vario=False, verbose=False):
    """Perform kriging on air quality residuals.
    
    Args:
        x: Air quality data (GeoDataFrame)
        covariates: Covariates (xarray Dataset)
        lm: Linear model
        n_min: Minimum number of points
        n_max: Maximum number of points
        cluster: Cluster for parallel processing
        cv: Whether to perform cross-validation
        show_vario: Whether to show variogram
        verbose: Whether to show verbose output
    
    Returns:
        Kriged residuals
    """
    import time
    from sklearn.metrics import mean_squared_error
    import pykrige
    from pykrige.ok import OrdinaryKriging
    import numpy as np
    
    start_time = time.time()
    
    # Get pollutant and area type
    pollutant = covariates.attrs.get("pollutant")
    area_type = x.attrs.get("area_type") if hasattr(x, "attrs") else None
    
    if verbose:
        print(f"Kriging {pollutant} {area_type} residuals...")
    
    # Get coordinates
    coords = pd.DataFrame({
        'x': x.geometry.x,
        'y': x.geometry.y
    })
    
    # Extract covariate values at point locations
    X_point = {}
    for var in covariates.data_vars:
        values = []
        for _, row in coords.iterrows():
            # Find nearest pixel
            x_idx = np.abs(covariates.x.values - row['x']).argmin()
            y_idx = np.abs(covariates.y.values - row['y']).argmin()
            
            # Extract value
            values.append(covariates[var].values[y_idx, x_idx])
        
        X_point[var] = values
    
    # Create DataFrame with covariate values
    X_point = pd.DataFrame(X_point).fillna(0)
    
    # Predict using linear model
    y_pred = lm.predict(X_point)
    
    # Calculate residuals
    residuals = x[pollutant] - y_pred
    
    # Prepare data for kriging
    x_coords = coords['x'].values
    y_coords = coords['y'].values
    z_values = residuals.values
    
    # Create and fit ordinary kriging model
    OK = OrdinaryKriging(
        x_coords, 
        y_coords, 
        z_values, 
        variogram_model='spherical', 
        verbose=verbose,
        enable_plotting=show_vario,
    )
    
    # Create grid for kriging
    grid_x = np.linspace(min(x_coords), max(x_coords), 100)
    grid_y = np.linspace(min(y_coords), max(y_coords), 100)
    
    # Perform kriging
    z_krig, ss_krig = OK.execute('grid', grid_x, grid_y)
    
    # Convert to xarray
    krig_result = xr.Dataset({
        'kriged_residuals': (['y', 'x'], z_krig),
        'kriging_variance': (['y', 'x'], ss_krig)
    }, coords={
        'x': grid_x,
        'y': grid_y
    })
    
    # Add metadata
    krig_result.attrs = {
        'pollutant': pollutant,
        'area_type': area_type,
        'processing_time': time.time() - start_time
    }
    
    # Cross-validation if requested
    if cv:
        from sklearn.model_selection import KFold
        
        # Prepare cross-validation
        kf = KFold(n_splits=10, shuffle=True, random_state=42)
        mse_values = []
        
        for train_idx, test_idx in kf.split(x_coords):
            # Train kriging model
            OK_cv = OrdinaryKriging(
                x_coords[train_idx], 
                y_coords[train_idx], 
                z_values[train_idx], 
                variogram_model='spherical', 
                verbose=False
            )
            
            # Predict for test set
            z_pred, _ = OK_cv.execute('points', x_coords[test_idx], y_coords[test_idx])
            
            # Calculate MSE
            mse = mean_squared_error(z_values[test_idx], z_pred)
            mse_values.append(mse)
        
        # Add cross-validation results to metadata
        krig_result.attrs['cv_mse_mean'] = np.mean(mse_values)
        krig_result.attrs['cv_rmse_mean'] = np.sqrt(np.mean(mse_values))
        
        if verbose:
            print(f"Cross-validation RMSE: {np.sqrt(np.mean(mse_values)):.4f}")
    
    return krig_result

def merge_aq_maps(paths, pse_paths=None, weights=None, cluster=None):
    """Merge air quality maps from different paths.
    
    Args:
        paths: Paths to air quality maps
        pse_paths: Paths to PSE maps
        weights: Weights for merging
        cluster: Cluster for parallel processing
    
    Returns:
        Merged air quality map
    """
    import re
    import numpy as np
    import xarray as xr
    import time
    
    # Extract info from path
    basename = os.path.basename(paths[0])
    info = basename.split('_')
    pollutant = info[0]
    stat = info[1]
    area_type = ""
    
    # Extract time information
    time_match = re.search(r'(\d{8})_(\d{8})', basename)
    if time_match:
        start_time = time_match.group(1)
        end_time = time_match.group(2)
        dtime = f"from {start_time} to {end_time}"
    else:
        dtime = "unknown time period"
    
    # Set default weights if not provided
    if weights is None:
        weights = np.ones(len(paths)) / len(paths)
    
    # Normalise weights
    weights = np.array(weights) / sum(weights)
    
    # Load and merge maps
    merged = None
    
    for i, path in enumerate(paths):
        # Load map
        ds = xr.open_dataset(path)
        
        # Scale by weight
        ds = ds * weights[i]
        
        # Merge
        if merged is None:
            merged = ds
        else:
            # Add variables
            for var in ds.data_vars:
                if var in merged:
                    merged[var] += ds[var]
                else:
                    merged[var] = ds[var]
    
    # Process PSE maps if provided
    pse_merged = None
    
    if pse_paths is not None:
        for i, path in enumerate(pse_paths):
            # Load map
            ds = xr.open_dataset(path)
            
            # Scale by weight
            ds = ds * weights[i]
            
            # Merge
            if pse_merged is None:
                pse_merged = ds
            else:
                # Add variables
                for var in ds.data_vars:
                    if var in pse_merged:
                        pse_merged[var] += ds[var]
                    else:
                        pse_merged[var] = ds[var]
        
        # Merge with main result
        if pse_merged is not None:
            for var in pse_merged.data_vars:
                merged[f"pse_{var}"] = pse_merged[var]
    
    # Add metadata
    merged.attrs = {
        'pollutant': pollutant,
        'stat': stat,
        'dtime': dtime,
        'area_type': area_type
    }
    
    return merged

def merge_aq_maps_list(compo_list, weights=None, cluster=None, verbose=True):
    """Merge a list of air quality maps.
    
    Args:
        compo_list: List of air quality maps
        weights: Weights for merging
        cluster: Cluster for parallel processing
        verbose: Whether to show verbose output
    
    Returns:
        Merged air quality map
    """
    import numpy as np
    import xarray as xr
    import time
    
    # Sort list by name
    compo_list = {k: compo_list[k] for k in sorted(compo_list.keys())}
    
    # Set default weights if not provided
    if weights is None:
        weights = np.ones(len(compo_list)) / len(compo_list)
    
    # Normalise weights
    weights = np.array(weights) / sum(weights)
    
    # Extract info from first component
    first_key = list(compo_list.keys())[0]
    first_comp = compo_list[first_key]
    
    pollutant = first_comp.attrs.get('pollutant', '')
    stat = first_comp.attrs.get('stat', '')
    dtime = first_comp.attrs.get('dtime', '')
    area_type = first_comp.attrs.get('area_type', '')
    
    # Merge components
    merged = None
    
    for i, (key, comp) in enumerate(compo_list.items()):
        # Scale by weight
        scaled = comp * weights[i]
        
        # Merge
        if merged is None:
            merged = scaled
        else:
            # Add variables
            for var in scaled.data_vars:
                if var in merged:
                    merged[var] += scaled[var]
                else:
                    merged[var] = scaled[var]
    
    # Add metadata
    merged.attrs = {
        'pollutant': pollutant,
        'stat': stat,
        'dtime': dtime,
        'area_type': area_type
    }
    
    return merged

def check_map_progress(parent_dir):
    """Check progress of map creation.
    
    Args:
        parent_dir: Parent directory containing maps
    
    Returns:
        DataFrame with map progress information
    """
    import glob
    import os
    import re
    import pandas as pd
    
    # Find COG directories
    cog_dirs = []
    for root, dirs, files in os.walk(parent_dir):
        for dir in dirs:
            if 'cog' in dir:
                cog_dirs.append(os.path.join(root, dir))
    
    # Extract pollutants
    polls = [path.split(os.path.sep)[3] for path in cog_dirs]
    
    # Find COG files (excluding PSE files)
    cog_files = []
    for cog_dir in cog_dirs:
        for root, dirs, files in os.walk(cog_dir):
            for file in files:
                if 'pse' not in file:
                    cog_files.append(os.path.join(root, file))
    
    # Create DataFrame
    df = pd.DataFrame({'path': cog_files})
    
    # Extract information from paths
    df['poll'] = df['path'].apply(lambda x: x.split(os.path.sep)[3])
    df['freq'] = df['path'].apply(lambda x: x.split(os.path.sep)[2])
    
    # Extract time information
    def extract_time(path):
        # Find pattern like 20150101_20231231
        time_match = re.search(r'(\d{8})_(\d{8})', path)
        if time_match:
            return time_match.group(1), time_match.group(2)
        return None, None
    
    df['start'], df['stop'] = zip(*df['path'].apply(extract_time))
    
    # Create pivot table
    pivot = pd.pivot_table(df, index='start', columns='poll', values='path', aggfunc='count')
    
    # Add year, month, day columns
    pivot['y'] = pivot.index.str[:4].astype(int)
    pivot['m'] = pivot.index.str[4:6].astype(int)
    pivot['d'] = pivot.index.str[6:8].astype(int)
    
    # Group by year and pollutant
    result = pivot.groupby(['poll', 'y']).sum().reset_index()
    
    # Pivot to wide format
    result = result.pivot_table(index='y', columns='poll', values='n')
    
    return result
