
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
- [ ] preprocess all station data

## Gapfill PM2.5 using PM10 observations

predictors: - coordinates - population density - sun shine duration -
ERA5 alternative:
[`surface_net_solar_radiation`](https://codes.ecmwf.int/grib/param-db/?id=176)

- [ ] add model plot function with metrics
- [ ] gapfill all PM2.5

## Supplementary data

### Station-level

- [x] gather station data
- [x] add elevation
- [x] add CLC
  - [x] reclassify to 8 classes (Horalek 2019, section 3.4)
- [x] add population density

### Measurement-level

- CAMS data (atmospheric transport model outputs for each pollutant,
  hourly)
  - [ ] download
- ECWMF data (hourly):
  - wind speed (from u and v)
  - surface net solar radiation
  - temperature
  - relative humidity (from temp. and dew point temp.)
  - [x] download
  - [ ] calculate wind speed & humidity
- [x] write function for efficient & parallel extraction

## Interpolation

- create 3 separate map layers based on…
  - rural background stations
  - urban/suburban background stations
  - urban/suburban traffic stations

## Merging