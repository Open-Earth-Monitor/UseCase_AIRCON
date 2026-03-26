
# OEMC Use Case 23: Air Quality Monitoring at Continental Scale

The objectives of this use case are

- to implement a reproducible method to predict air quality indicators
  using open source software
- to create annual, monthly, and daily maps of NO2, O3, PM10, PM2.5

This repository stores code for the entire processing chain, including -
the retrieval and preprocessing of measurement station data and raster
covariates - the prediction of air quality indicators using the
Regression Interpolation Merging Mapping (RIMM) method, introduced by
Horalek et al. (2023)

For more details on features and progress, visit the [task
list](tasks.md).

## Data and Results

[**Hourly data from about 7,000 EEA measurement stations throughout the
EU**](https://zenodo.org/records/17085064) serves as basis for
interpolation. Supplementary variables such as population density, land
cover, climatology are further inputs for the prediction method. The
**final maps** can viewed and explored in the [**OEMC
app**](https://app.earthmonitor.org/map/m11/datasets?sidebar-open=true&bbox=%5B-1903163.1041197143,4228333.5219,4129552.920019715,7985363.0743%5D&layers=%5B%7B%22id%22:%22l50%22,%22opacity%22:1,%22date%22:%2220150101_20151231%22%7D%5D)
or downloaded directly via [**Zenodo**]().

## Codeflow

| Step | File                                                                                                                                                                                                                          | Description                                                                                                                                                                                                                                          |
|-----:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|    1 | `ACS_CAMS_access.R` `CCS_ERA5_access.R` `download_nc_job.R` `rename_nc.R`                                                                                                                                                     | Request hourly ERA5 weather and CAMS pollution data. Copy to URLs from the web interface and store in .txt file to iterate over for downloading. Rename files according to metadata.                                                                 |
|    2 | [`EEA_stations.qmd`](R/EEA_stations.md)                                                                                                                                                                                       | Create a spatial dataset with all AQ stations and supplement them with static covariates (elevation, population, land cover).                                                                                                                        |
|    3 | [`EEA_AQ_data_access.qmd`](R/EEA_AQ_data_access.md) `EEA_AQ_data_access_all_countries.R`                                                                                                                                      | Download & pre-process hourly AQ measurements. This includes reading, filtering, and joining up to 5 pollutant time series per station for 2015-2023.                                                                                                |
|    4 | [`xarray_extract_station_SSR.ipynb`](R/xarray_extract_station_SSR.ipynb) [`EEA_PM25_gapfilling_all_countries.qmd`](R/EEA_PM25_gapfilling_all_countries.md)                                                                    | Extract hourly Surface Solar Radiation before gapfilling PM2.5 (only where PM10 is measured) using linear regression.                                                                                                                                |
|    5 | [`xarray_dask_rel_humidity.ipynb`](R/xarray_dask_rel_humidity.ipynb) [`xarray_dask_ws_wd.ipynb`](R/xarray_dask_ws_wd.ipynb)                                                                                                   | Process ERA5 wind vectors and temperature data to wind speed & direction and relative humidity.                                                                                                                                                      |
|    6 | [`EEA_AQ_data_temporal_aggregation.qmd`](R/EEA_AQ_data_temporal_aggregation.md) [`xarray_temp_aggregate.ipynb`](R/xarray_temp_aggregate.ipynb) [`xarray_temp_aggregate_rolling.ipynb`](R/xarray_temp_aggregate_rolling.ipynb) | Aggregate AQ measurements and CAMS/ERA5 hourly data to annual, monthly, daily means/quantiles.                                                                                                                                                       |
|    7 | [`s5p_l3_access.ipynb`](R/s5p_l3_access.ipynb)                                                                                                                                                                                | Access Sentinel-5P TROPOMI Level 3 NO2 data and aggregate to coarser temporal resolutions.                                                                                                                                                           |
|    8 | [`AQ_demo.qmd`](R/AQ_demo.md) `AQ_interpolation_loop_palma_[daily/monthly/annual].R`                                                                                                                                          | Interpolate AQ data using environmental and socio-economic covariates. Weight and combine individual outputs for rural background, urban background, and urban traffic stations. Return cloud-optimized GeoTiff with prediction and error variables. |
|    9 | [`AQ_maps_as_Zarr.ipynb`](R/AQ_maps_as_Zarr.ipynb)                                                                                                                                                                            | Convert map outputs to Zarr stores.                                                                                                                                                                                                                  |

## References

- Horálek, J., Vlasáková, L., Schreiberová, M., Marková, J., Schneider,
  P., Kurfürst, P., Tognet, F., Schovánková, J., Vlček, O., Damašková,
  D., 2023. European air quality maps for 2020. PM10, PM2.5, Ozone, NO2,
  NOx and Benzo(a)pyrene spatial estimates and their uncertainties.
  ([No. ETC HE Report
  2022/12](https://www.eionet.europa.eu/etcs/etc-he/products/etc-he-report-2022-12-european-air-quality-maps-for-2020-pm10-pm2-5-ozone-no2-nox-and-benzo-a-pyrene-spatial-estimates-and-their-uncertainties)).
