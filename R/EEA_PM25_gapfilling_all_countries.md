# Gapfilling of hourly PM2.5 data for all countries
Johannes Heisig
2024-01-25

``` r
suppressPackageStartupMessages({
library(dplyr)
library(arrow)
library(tictoc)
library(stars)
})
source("R/functions.R")
station_meta = read_parquet("AQ_stations/EEA_stations_meta.parquet")
```

# Read Data

Open hourly data for all 40 countries, but filter while reading to limit
memory needs (using the arrow library).

``` r
tic()
aq = open_dataset("AQ_data/02_hourly_SSR") |> 
  filter(!is.na(PM10), !is.na(PM2.5), PM2.5 > 0, PM10 > 0) |> 
  collect()
pm2.5_99.9 = quantile(aq$PM2.5, 0.999)
pm10_99.9 = quantile(aq$PM10, 0.999)
aq = filter(aq, PM2.5 <= pm2.5_99.9, PM10 <= pm10_99.9)
toc()
```

    10.309 sec elapsed

``` r
aq
```

    # A tibble: 41,801,144 × 13
       AirQualityStationEoICode StationArea StationType Longitude Latitude
       <fct>                    <fct>       <fct>           <dbl>    <dbl>
     1 AD0942A                  urban       background       1.54     42.5
     2 AD0942A                  urban       background       1.54     42.5
     3 AD0942A                  urban       background       1.54     42.5
     4 AD0942A                  urban       background       1.54     42.5
     5 AD0942A                  urban       background       1.54     42.5
     6 AD0942A                  urban       background       1.54     42.5
     7 AD0942A                  urban       background       1.54     42.5
     8 AD0942A                  urban       background       1.54     42.5
     9 AD0942A                  urban       background       1.54     42.5
    10 AD0942A                  urban       background       1.54     42.5
    # ℹ 41,801,134 more rows
    # ℹ 8 more variables: DatetimeBegin <dttm>, NO2 <dbl>, O3 <dbl>, PM2.5 <dbl>,
    #   PM10 <dbl>, Countrycode <fct>, SSR <dbl>, points <int>

``` r
nrow(aq) 
```

    [1] 41801144

``` r
table(aq$StationType, aq$StationArea)
```

                
                    rural rural-nearcity rural-regional rural-remote suburban
      background  3312987              0              0            0  7191431
      industrial        0              0              0            0        0
      traffic       83563              0              0            0  2512848
                
                    urban
      background 18226317
      industrial        0
      traffic    10473998

# Add Population Density Information

Extract population density values from SEDAC raster at each station
location.

``` r
pop = read_stars("supplementary/static/pop_density_original_epsg3035.tif")
pop_ex = st_extract(pop[1], 
                    station_meta |> 
                      st_as_sf(coords = c('Longitude', 'Latitude'), crs=st_crs(4326)) |> 
                      st_transform(st_crs(3035))) |> 
  st_drop_geometry() |> 
  setNames("Population")

pop_stations = cbind(AirQualityStationEoICode = station_meta$AirQualityStationEoICode, pop_ex) 
```

Join based on station id.

``` r
tic()
aq = left_join(aq, pop_stations, by = "AirQualityStationEoICode") |> 
  filter(!is.na(Population))
toc()
```

    3.956 sec elapsed

# Gapfilling Model (as proposed by Denby 2011 and Horalek et al. 2019)

Simple linear model to predict PM2.5 concentrations from PM10,
population density, surface solar radiation and coordinates. Model
includes interactions for station type (background vs traffic) but not
for station area.

``` r
lm_pm2.5 = lm(PM2.5 ~ (PM10 + Population + SSR + Longitude + Latitude) * StationType , data = aq)

summary(lm_pm2.5)
```


    Call:
    lm(formula = PM2.5 ~ (PM10 + Population + SSR + Longitude + Latitude) * 
        StationType, data = aq)

    Residuals:
         Min       1Q   Median       3Q      Max 
    -131.740   -2.884   -0.263    2.673  135.636 

    Coefficients:
                                    Estimate Std. Error  t value Pr(>|t|)    
    (Intercept)                   -6.294e+00  1.410e-02  -446.53   <2e-16 ***
    PM10                           6.082e-01  8.259e-05  7363.72   <2e-16 ***
    Population                    -6.506e-05  3.684e-07  -176.58   <2e-16 ***
    SSR                           -1.607e-07  1.941e-10  -828.12   <2e-16 ***
    Longitude                      3.284e-03  1.430e-04    22.96   <2e-16 ***
    Latitude                       1.653e-01  2.793e-04   591.88   <2e-16 ***
    StationTypetraffic             1.709e+01  2.182e-02   783.20   <2e-16 ***
    PM10:StationTypetraffic       -1.571e-01  1.384e-04 -1134.88   <2e-16 ***
    Population:StationTypetraffic  3.239e-05  6.728e-07    48.13   <2e-16 ***
    SSR:StationTypetraffic         1.930e-08  3.529e-10    54.70   <2e-16 ***
    Longitude:StationTypetraffic   1.896e-02  2.565e-04    73.90   <2e-16 ***
    Latitude:StationTypetraffic   -3.205e-01  4.131e-04  -775.86   <2e-16 ***
    ---
    Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

    Residual standard error: 7.152 on 38450504 degrees of freedom
      (2850872 observations deleted due to missingness)
    Multiple R-squared:  0.6783,    Adjusted R-squared:  0.6783 
    F-statistic: 7.37e+06 on 11 and 38450504 DF,  p-value: < 2.2e-16

``` r
caret::RMSE(predict(lm_pm2.5), lm_pm2.5$model$PM2.5)
```

    [1] 7.152414

# Prediction

Map over each country and predict PM2.5 for gaps where PM10 is available
by applying the above model. The function `fill_pm2.5_gaps()` further
adds a column named “pseudo” that indicates whether a value was actually
measured (0) or inferred through gapfilling (1), before writing to
(parquet) file.

``` r
countries = as.character(unique(station_meta$Countrycode)) |> sort()
filled = purrr::map_vec(countries, fill_pm2.5_gaps, 
                        model = lm_pm2.5, 
                        population_table = pop_stations,
                        gap_data = "AQ_data/02_hourly_SSR",
                        out_dir = "AQ_data/03_hourly_gapfilled")
```

    AD - AL - AT - BA - BE - BG - CH - CY - CZ - DE - DK - EE - ES - FI - FR - GB - GE - GI - GR - HR - HU - IE - IS - IT - LT - LU - LV - ME - MK - MT - NL - NO - PL - PT - RO - RS - SE - SI - SK - TR - UA - XK - 

``` r
filled |> basename()
```

     [1] "AD_hourly_gapfilled.parquet" "AL_hourly_gapfilled.parquet"
     [3] "AT_hourly_gapfilled.parquet" "BA_hourly_gapfilled.parquet"
     [5] "BE_hourly_gapfilled.parquet" "BG_hourly_gapfilled.parquet"
     [7] "CH_hourly_gapfilled.parquet" "CY_hourly_gapfilled.parquet"
     [9] "CZ_hourly_gapfilled.parquet" "DE_hourly_gapfilled.parquet"
    [11] "DK_hourly_gapfilled.parquet" "EE_hourly_gapfilled.parquet"
    [13] "ES_hourly_gapfilled.parquet" "FI_hourly_gapfilled.parquet"
    [15] "FR_hourly_gapfilled.parquet" "GB_hourly_gapfilled.parquet"
    [17] "GE_hourly_gapfilled.parquet" "GI_hourly_gapfilled.parquet"
    [19] "GR_hourly_gapfilled.parquet" "HR_hourly_gapfilled.parquet"
    [21] "HU_hourly_gapfilled.parquet" "IE_hourly_gapfilled.parquet"
    [23] "IS_hourly_gapfilled.parquet" "IT_hourly_gapfilled.parquet"
    [25] "LT_hourly_gapfilled.parquet" "LU_hourly_gapfilled.parquet"
    [27] "LV_hourly_gapfilled.parquet" "ME_hourly_gapfilled.parquet"
    [29] "MK_hourly_gapfilled.parquet" "MT_hourly_gapfilled.parquet"
    [31] "NL_hourly_gapfilled.parquet" "NO_hourly_gapfilled.parquet"
    [33] "PL_hourly_gapfilled.parquet" "PT_hourly_gapfilled.parquet"
    [35] "RO_hourly_gapfilled.parquet" "RS_hourly_gapfilled.parquet"
    [37] "SE_hourly_gapfilled.parquet" "SI_hourly_gapfilled.parquet"
    [39] "SK_hourly_gapfilled.parquet" "TR_hourly_gapfilled.parquet"
    [41] "UA_hourly_gapfilled.parquet" "XK_hourly_gapfilled.parquet"
