# Temporal Aggregates for EEA AQ Station Data
Johannes Heisig
2024-02-06

## Setup

``` r
library(pbmcapply) # for parallel processing
```

    Loading required package: parallel

``` r
library(tictoc)    # for timing

if (! basename(getwd()) == "UseCase_AIRCON") setwd("UseCase_AIRCON")
source("R/functions.R") 
```

## Overview

Interpolated air quality maps for Europe will be processed at a temporal
resolution of days, months, and years. For this reason hourly air
quality (AQ) data needs to be aggregated to coarse temporal steps.
Hourly PM2.5 time series were previously gap-filled. In the aggregation
process the temporal coverage of each pollutant and measurement station
is assessed at daily, monthly and annual level. Observations exceeding a
threshold (e.g. 75%) are flagged.

The function `process_temp_agg()` takes the path to a gap-filled
(country-level) AQ data file, as well as the name of a pollutant of
interest and a threshold for flagging sufficient temporal data coverage.
To calculate a certain percentile instead of the mean, one can supply a
value to the `perc` argument. The function returns nothing but writes
daily, monthly and annual aggregates directly to file.

Aggregating ozone measurements requires special attention and is
described in more detail below.

## Temporal Means

PM10, PM2.5 and NO2 are aggregated using the `mean()` function for all
temporal resolutions.

``` r
gapfilled = list.files("AQ_data/03_hourly_gapfilled", full.names = T)
pollutants = c("PM10", "PM2.5", "NO2")

for (p in pollutants){
  message(p)
  tic()
  m = pbmclapply(gapfilled, process_temp_agg, p, cov_threshold = 0.75, 
                 overwrite = T, mc.cores = 8) 
  toc()
}
```

    PM10

    Warning in mclapply(X, FUN, ..., mc.cores = mc.cores, mc.preschedule =
    mc.preschedule, : all scheduled cores encountered errors in user code

    19.581 sec elapsed

    PM2.5

    Warning in mclapply(X, FUN, ..., mc.cores = mc.cores, mc.preschedule =
    mc.preschedule, : all scheduled cores encountered errors in user code

    19.897 sec elapsed

    NO2

    Warning in mclapply(X, FUN, ..., mc.cores = mc.cores, mc.preschedule =
    mc.preschedule, : all scheduled cores encountered errors in user code

    20.625 sec elapsed

## PM10: 90.41th percentile of daily means

Beside the mean, PM10 is aggregated using the 90.41th percentile
(formerly 36th highest value) for monthly and annual aggregates.

``` r
tic()
pm = pbmclapply(gapfilled, process_temp_agg, "PM10", perc = 0.9041, 
           cov_threshold = 0.75, overwrite = T, mc.cores = 8)
```

    Warning in mclapply(X, FUN, ..., mc.cores = mc.cores, mc.preschedule =
    mc.preschedule, : all scheduled cores encountered errors in user code

``` r
toc()
```

    22.49 sec elapsed

## Ozone: 93.15th percentile of daily maxima of the 8 hour running mean

Ozone data requires a different temporal aggregation method than other
pollutants where averages span over a moving window of 8 hours. Each
station’s O3 time series is filled with missing values for hours where
no observation was recorded. The 8 hour running mean is then calculated
with the requirement of 6 valid (non-missing) values among the 8 values
of interest. Daily aggregates represent the maximum of the 8 hour
running mean within 24 hours. Monthly and annual aggregates represent
the 93.15th percentile of the daily aggregates. A threshold is applied
to flag aggregates which fulfill a certain coverage requirement.

This more complex approach is implemented in `process_temp_agg_8hm()`

``` r
tic()
o3 = pbmclapply(gapfilled, process_O3_temp_agg_8hm, perc = 0.9315, 
           cov_threshold = 0.75, mc.cores = 8)
toc()
```

    6.377 sec elapsed
