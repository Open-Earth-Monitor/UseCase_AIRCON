
# OEMC Use Case 23: Air Quality Monitoring at Continental Scale

The goal is to create annual, monthly, daily, and potentially hourly
maps of four key air quality indicators (NO2, O3, PM10, PM2.5). Hourly
data from about 6,000 EEA measurement stations throughout the EU serves
as basis for interpolation. Supplementary variables such as population
density, land cover, climatology are further inputs to a universal
Kriging method.

# ToDo

## Preprocess AQ

- [x] write download functions
- [x] write function to filter by quality flags
- [x] write function to filter for rural/urban/suburban &
  background/traffic
  - *include traffic or not? different approaches in the reports*
- [x] write function to join pollutant tables and station meta data
- [x] download station data for all countries (2015-2023)
- [x] preprocess all station data

## Gapfill PM2.5 using PM10 observations

predictors:

- [x] gapfill all PM2.5 with predictors according to EEA
- coordinates
- population density
- sun shine duration
  - ERA5 alternative:
    [`surface_net_solar_radiation`](https://codes.ecmwf.int/grib/param-db/?id=176)

## Temporal aggregates (annual, monthly, daily)

- [x] NO2 (mean)
- [x] PM 2.5 (mean)
- [x] PM10
  - [x] mean
  - [x] 90.4 percentile of daily mean
- [ ] O3
  - [x] mean
  - [ ] 93.2 percentile of max. daily 8h rolling mean

## Supplementary data

### Station-level (static)

- [x] gather station data
- [x] add elevation
- [x] add CLC
  - [x] reclassify to 8 classes (Horalek 2019, section 3.4)
- [x] add population density
- [ ] add road type information
  - [x] buffer and rasterize GRIP vector data for road classes 1-3
  - [x] distance to nearest road (by type)

### Measurement-level (hourly)

- CAMS data (atmospheric transport model outputs for each pollutant,
  hourly)
  - [reanalysis](https://ads.atmosphere.copernicus.eu/cdsapp#!/dataset/cams-europe-air-quality-reanalyses?tab=overview)
    for 2015-2022
    - validated reanalysis: 2015-2020
    - interim reanalysis: 2021-2022
  - [forecasts](https://ads.atmosphere.copernicus.eu/cdsapp#!/dataset/cams-europe-air-quality-forecasts?tab=overview)
    for 2023 (3-year rolling archive)
  - download
    - [x] 2015-2022
    - [ ] 2023
- ECWMF data (hourly):
  - wind speed (from u and v)
  - surface net solar radiation
  - temperature
  - relative humidity (from temp. and dew point temp.)
  - [x] download
  - [x] calculate wind speed/direction & humidity
  - temporal aggregates
    - [x] annual
    - [x] monthly
    - [x] daily
- Sentinel-5P TROPOMI
  - temporal aggregates
    - [ ] annual
    - [ ] monthly
    - [ ] daily

## Interpolation

- [x] function to read aq data
- [x] function to read and warp covariates to a common grid
- [ ] function wrapping lm
- [ ] function for residual kriging in parallel

create 3 separate map layers based on…

- rural background stations
- urban/suburban background stations
- urban/suburban traffic station
- [ ] run interpolation test for one annual dataset

## Merging

- [x] rasterize GRIP road data (area impacted by traffic)
- [ ] function to merge the 3 map layers

# Codeflow

| Step | File                                                                       | Description                                                                                                                                                                          |
|-----:|:---------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|    1 | `ACS_CAMS_access.R` `CCS_ERA5_access.R` `download_nc_job.R` `rename_nc.R`  | Request hourly ERA5 weather and CAMS pollution data. Copy to URLs from the web interface and store in .txt file to iterate over for downloading. Rename files according to metadata. |
|    2 | `EEA_stations.qmd`                                                         | Create a spatial dataset with all AQ stations and supplement them with static covariates (elevation, population, land cover).                                                        |
|    3 | `EEA_AQ_data_access.qmd` `EEA_AQ_data_access_all_countries.R`              | Download & pre-process hourly AQ measurements. This includes reading, filtering, and joining up to 4 pollutant time series per station for 2015-2023.                                |
|    4 | `EEA_PM25_gapfilling.qmd`                                                  | Infer PM2.5 where missing AND where PM10 is available using linear regression.                                                                                                       |
|    5 | `xarray_extract_station_SSR.ipynb` `EEA_PM25_gapfilling_all_countries.qmd` | Extract hourly Surface Solar Radiation before gapfilling PM2.5 (only where PM10 is measured) using linear regression.                                                                |
|    6 | `xarray_dask_rel_humidity.ipynb` `xarray_dask_ws_wd.ipynb`                 | Process ERA5 wind vectors temperature data to wind speed & direction and relative humidity.                                                                                          |
|    7 | `EEA_AQ_data_temporal_aggregation.qmd` `xarray_temp_aggregate.ipynb`       | Aggregate AQ measurements and CAMS/ERA5 hourly data to annual, monthly, daily means.                                                                                                 |
