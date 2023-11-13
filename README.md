
# OEMC Use Case 23: Air Quality Monitoring at Continental Scale

The goal is to create annual, monthly, and potentially hourly maps of
four key air quality indicators (NO2, O3, PM10, PM2.5). Hourly data from
about 6000 EEA measurement stations throughout the EU serves as basis
for interpolation. Supplementary variables such as population density,
land cover, climatology are further inputs to a universal Kriging
method.

# ToDo

## Preprocess AQ

- [x] write download functions
- [x] write function to filter by quality flags
- [x] write function to filter for rural/urban/suburban &
  background/traffic
  - *include traffic or not? different approaches in the reports*
- [x] write function to join pollutant tables and station meta data
- write function to aggregate temporal dimension (year, month, day)
  - [x] simple mean
  - specific percentiles (for O3, NO2)
- [x] download station data for all countries (2015-2023)
- [x] preprocess all station data

## Gapfill PM2.5 using PM10 observations

predictors:

- coordinates
- population density
- sun shine duration
  - ERA5 alternative:
    [`surface_net_solar_radiation`](https://codes.ecmwf.int/grib/param-db/?id=176)
- [x] gapfill all PM2.5

## Supplementary data

### Station-level (static)

- [x] gather station data
- [x] add elevation
- [x] add CLC
  - [x] reclassify to 8 classes (Horalek 2019, section 3.4)
- [x] add population density

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
- [x] create workflow efficient & parallel extraction
- [ ] temporal aggregates for annual, monthly & daily maps

## Interpolation

- create 3 separate map layers based on…
  - rural background stations
  - urban/suburban background stations
  - urban/suburban traffic stations

## Merging

# Codeflow

<table style="width:100%;">
<colgroup>
<col style="width: 4%" />
<col style="width: 24%" />
<col style="width: 70%" />
</colgroup>
<thead>
<tr class="header">
<th>Step</th>
<th>File</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>1</td>
<td><code>ACS_CAMS_access.R</code> <br />
<code>CCS_ERA5_access.R</code><br />
<code>download_nc_job.R</code> <br />
<code>rename_nc.R</code></td>
<td>Request hourly ERA5 weather and CAMS pollution data. Copy to URLs
from the web interface and store in .txt file to iterate over for
downloading. Rename files according to metadata.</td>
</tr>
<tr class="even">
<td>2</td>
<td><code>EEA_stations.qmd</code></td>
<td>Create a spatial dataset with all AQ stations and supplement them
with static covariates (elevation, population, land cover).</td>
</tr>
<tr class="odd">
<td>3</td>
<td><code>EA_AQ_data_access.qmd</code>
<code>EEA_AQ_data_access_all_countries.R</code></td>
<td>Download &amp; pre-process hourly AQ measurements. This includes
reading, filtering, and joining up to 4 pollutant time series per
station for 2015-2023.</td>
</tr>
<tr class="even">
<td>4</td>
<td><code>EEA_PM25_gapfilling.qmd</code></td>
<td>Infer PM2.5 where missing AND where PM10 is available using linear
regression.</td>
</tr>
<tr class="odd">
<td>5</td>
<td><code>EEA_AQ_data_temporal_aggregation.qmd</code></td>
<td>Aggregate measurements to annual, monthly, daily.</td>
</tr>
<tr class="even">
<td>6</td>
<td><p><code>xarray_extract_station_SSR.ipynb</code></p>
<p><code>EEA_PM25_gapfilling_all_countries.qmd</code></p></td>
<td>Extract hourly Surface Solar Radiation before gapfilling PM2.5 (only
where PM10 is measured) using linear regression.</td>
</tr>
<tr class="odd">
<td>7</td>
<td><code>xarray_dask_rel_humidity.ipynb</code><br />
<code>xarray_dask_ws_wd.ipynb</code></td>
<td>Process ERA5 wind vectors temperature data to wind speed &amp;
direction and relative humidity.</td>
</tr>
</tbody>
</table>
