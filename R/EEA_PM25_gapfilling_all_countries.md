# Gapfilling of hourly PM2.5 data for all countries
Johannes Heisig
2024-07-17

``` r
suppressPackageStartupMessages({
library(dplyr)
library(arrow)
library(tictoc)
library(stars)
})
source("R/functions.R")
station_meta = geoarrow::read_geoparquet_sf("AQ_stations/EEA_stations_meta_sf.parquet")
```

# Read Data

Open hourly data for all 40 countries, but filter while reading to limit
memory needs (using the arrow library). Here we are reading only data
relevant for building a linear gap-filling model (so
`PM2.5 > 0 & PM10 > 0`).

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

    22.904 sec elapsed

``` r
aq
```

    # A tibble: 73,481,499 × 21
       Air.Quality.Station.EoI…¹ Countrycode Start                PM10 Validity_PM10
       <fct>                     <fct>       <dttm>              <dbl>         <dbl>
     1 AD0942A                   AD          2021-01-01 02:00:00  13.5             1
     2 AD0942A                   AD          2021-01-01 03:00:00  14.6             1
     3 AD0942A                   AD          2021-01-01 04:00:00  12.8             1
     4 AD0942A                   AD          2021-01-01 05:00:00   9.4             1
     5 AD0942A                   AD          2021-01-01 06:00:00   6               1
     6 AD0942A                   AD          2021-01-01 07:00:00   5.7             1
     7 AD0942A                   AD          2021-01-01 08:00:00   5.3             1
     8 AD0942A                   AD          2021-01-01 09:00:00   5               1
     9 AD0942A                   AD          2021-01-01 10:00:00   4.8             1
    10 AD0942A                   AD          2021-01-01 11:00:00   6.5             1
    # ℹ 73,481,489 more rows
    # ℹ abbreviated name: ¹​Air.Quality.Station.EoI.Code
    # ℹ 16 more variables: Verification_PM10 <dbl>, PM2.5 <dbl>,
    #   Validity_PM2.5 <dbl>, Verification_PM2.5 <dbl>, O3 <dbl>,
    #   Validity_O3 <int>, Verification_O3 <int>, NO2 <dbl>, Validity_NO2 <dbl>,
    #   Verification_NO2 <dbl>, SO2 <dbl>, Validity_SO2 <dbl>,
    #   Verification_SO2 <dbl>, Longitude <dbl>, Latitude <dbl>, SSR <dbl>

``` r
nrow(aq) 
```

    [1] 73481499

# Add Population Density Information

Extract population density values from SEDAC raster at each station
location.

``` r
pop = read_stars("supplementary/static/pop_density_original_epsg3035.tif")
pop_ex = st_extract(pop[1], 
                    station_meta |> 
                      st_transform(st_crs(3035))) |> 
  st_drop_geometry() |> 
  setNames("Population")

pop_stations = cbind(Air.Quality.Station.EoI.Code = station_meta$Air.Quality.Station.EoI.Code,
                     Station.Type = station_meta$Station.Type,
                     pop_ex) 
```

Join based on station id.

``` r
tic()
aq = left_join(aq, pop_stations, by = "Air.Quality.Station.EoI.Code") |> 
  filter(!is.na(Population))
toc()
```

    12.977 sec elapsed

# Gapfilling Model (as proposed by Denby 2011 and Horalek et al. 2019)

Simple linear model to predict PM2.5 concentrations from PM10,
population density, surface solar radiation and coordinates. Model
includes interactions for station type (background vs traffic) but not
for station area.

``` r
lm_pm2.5 = lm(PM2.5 ~ (PM10 + Population + SSR + Longitude + Latitude) * Station.Type , data = aq)

summary(lm_pm2.5)
```


    Call:
    lm(formula = PM2.5 ~ (PM10 + Population + SSR + Longitude + Latitude) * 
        Station.Type, data = aq)

    Residuals:
         Min       1Q   Median       3Q      Max 
    -152.657   -2.744   -0.398    2.416  151.400 

    Coefficients:
                                        Estimate Std. Error  t value Pr(>|t|)    
    (Intercept)                       -6.763e+00  1.120e-02  -604.09   <2e-16 ***
    PM10                               5.765e-01  6.509e-05  8857.51   <2e-16 ***
    Population                        -6.978e-05  3.026e-07  -230.56   <2e-16 ***
    SSR                               -1.385e-07  1.398e-10  -990.34   <2e-16 ***
    Longitude                          6.558e-03  1.161e-04    56.49   <2e-16 ***
    Latitude                           1.803e-01  2.197e-04   821.03   <2e-16 ***
    Station.Typeindustrial             3.143e+00  2.248e-02   139.80   <2e-16 ***
    Station.Typetraffic                1.731e+01  1.797e-02   963.57   <2e-16 ***
    PM10:Station.Typeindustrial       -1.022e-01  1.576e-04  -648.30   <2e-16 ***
    PM10:Station.Typetraffic          -1.403e-01  1.102e-04 -1273.28   <2e-16 ***
    Population:Station.Typeindustrial  1.164e-04  1.860e-06    62.55   <2e-16 ***
    Population:Station.Typetraffic    -7.443e-06  5.583e-07   -13.33   <2e-16 ***
    SSR:Station.Typeindustrial         2.082e-08  3.529e-10    58.98   <2e-16 ***
    SSR:Station.Typetraffic            1.185e-08  2.529e-10    46.85   <2e-16 ***
    Longitude:Station.Typeindustrial   6.495e-02  2.886e-04   225.04   <2e-16 ***
    Longitude:Station.Typetraffic      1.775e-02  2.140e-04    82.95   <2e-16 ***
    Latitude:Station.Typeindustrial   -5.584e-02  4.529e-04  -123.30   <2e-16 ***
    Latitude:Station.Typetraffic      -3.251e-01  3.417e-04  -951.45   <2e-16 ***
    ---
    Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

    Residual standard error: 6.973 on 67273274 degrees of freedom
      (5545925 observations deleted due to missingness)
    Multiple R-squared:  0.6605,    Adjusted R-squared:  0.6605 
    F-statistic: 7.7e+06 on 17 and 67273274 DF,  p-value: < 2.2e-16

``` r
caret::RMSE(predict(lm_pm2.5), lm_pm2.5$model$PM2.5)
```

    [1] 6.973138

# Prediction

Map over each country and predict PM2.5 for gaps where PM10 is available
by applying the above model. The function `fill_pm2.5_gaps()` further
adds a column named “pseudo” that indicates whether a value was actually
measured (0) or inferred through gap-filling (1), before writing to
(parquet) file.

``` r
countries = as.character(unique(station_meta$Countrycode)) |> sort()
out_dir = "AQ_data/03_hourly_gapfilled"
filled = purrr::map_vec(countries, 
                        fill_pm2.5_gaps, 
                        model = lm_pm2.5, 
                        population_table = pop_stations,
                        gap_data = "AQ_data/02_hourly_SSR",
                        out_dir = out_dir, 
                        overwrite = T,
                        .progress = T)
```

    AD - 

     ■■                                 2% |  ETA:  8m

    AL - 

     ■■                                 5% |  ETA:  8m

    AT - 

     ■■■                                7% |  ETA: 10m

    BA - 

     ■■■■                              10% |  ETA:  9m

    BE - 

     ■■■■■                             12% |  ETA:  9m

    BG - 

     ■■■■■                             15% |  ETA:  9m

    CH - 

     ■■■■■■                            17% |  ETA:  8m

    CY - 

     ■■■■■■■                           20% |  ETA:  8m

    CZ - 

     ■■■■■■■■                          22% |  ETA:  8m

    DE - 

     ■■■■■■■■                          24% |  ETA:  9m

    DK - 

     ■■■■■■■■■                         27% |  ETA:  8m

    EE - 

     ■■■■■■■■■■                        29% |  ETA:  8m

    ES - 

     ■■■■■■■■■■■                       32% |  ETA:  9m

    FI - 

     ■■■■■■■■■■■                       34% |  ETA:  9m

    FR - 

     ■■■■■■■■■■■■                      37% |  ETA:  9m

    GB - 

     ■■■■■■■■■■■■■                     39% |  ETA:  9m

    GE - 

     ■■■■■■■■■■■■■                     41% |  ETA:  8m

    GR - 

     ■■■■■■■■■■■■■■                    44% |  ETA:  8m

    HR - 

     ■■■■■■■■■■■■■■■                   46% |  ETA:  7m

    HU - 

     ■■■■■■■■■■■■■■■■                  49% |  ETA:  7m

    IE - 

     ■■■■■■■■■■■■■■■■                  51% |  ETA:  6m

    IS - 

     ■■■■■■■■■■■■■■■■■                 54% |  ETA:  6m

    IT - 

     ■■■■■■■■■■■■■■■■■■                56% |  ETA:  6m

    LT - 

     ■■■■■■■■■■■■■■■■■■■               59% |  ETA:  6m

    LU - 

     ■■■■■■■■■■■■■■■■■■■               61% |  ETA:  5m

    LV - 

     ■■■■■■■■■■■■■■■■■■■■              63% |  ETA:  5m

    ME - 

     ■■■■■■■■■■■■■■■■■■■■■             66% |  ETA:  4m

    MK - 

     ■■■■■■■■■■■■■■■■■■■■■             68% |  ETA:  4m

    MT - 

     ■■■■■■■■■■■■■■■■■■■■■■            71% |  ETA:  4m

    NL - 

     ■■■■■■■■■■■■■■■■■■■■■■■           73% |  ETA:  3m

    NO - 

     ■■■■■■■■■■■■■■■■■■■■■■■■          76% |  ETA:  3m

    PL - 

     ■■■■■■■■■■■■■■■■■■■■■■■■          78% |  ETA:  3m

    PT - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■         80% |  ETA:  2m

    RO - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■        83% |  ETA:  2m

    RS - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■       85% |  ETA:  2m

    SE - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■       88% |  ETA:  2m

    SI - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■      90% |  ETA:  1m

    SK - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■     93% |  ETA:  1m

    TR - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■    95% |  ETA: 36s

    UA - 

     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■    98% |  ETA: 18s

    XK - 

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
    [17] "GE_hourly_gapfilled.parquet" "GR_hourly_gapfilled.parquet"
    [19] "HR_hourly_gapfilled.parquet" "HU_hourly_gapfilled.parquet"
    [21] "IE_hourly_gapfilled.parquet" "IS_hourly_gapfilled.parquet"
    [23] "IT_hourly_gapfilled.parquet" "LT_hourly_gapfilled.parquet"
    [25] "LU_hourly_gapfilled.parquet" "LV_hourly_gapfilled.parquet"
    [27] "ME_hourly_gapfilled.parquet" "MK_hourly_gapfilled.parquet"
    [29] "MT_hourly_gapfilled.parquet" "NL_hourly_gapfilled.parquet"
    [31] "NO_hourly_gapfilled.parquet" "PL_hourly_gapfilled.parquet"
    [33] "PT_hourly_gapfilled.parquet" "RO_hourly_gapfilled.parquet"
    [35] "RS_hourly_gapfilled.parquet" "SE_hourly_gapfilled.parquet"
    [37] "SI_hourly_gapfilled.parquet" "SK_hourly_gapfilled.parquet"
    [39] "TR_hourly_gapfilled.parquet" "UA_hourly_gapfilled.parquet"
    [41] "XK_hourly_gapfilled.parquet"

# Results

Structure of the gap-filled data:

``` r
(res = open_dataset(out_dir))
```

    FileSystemDataset with 41 Parquet files
    19 columns
    Air.Quality.Station.EoI.Code: dictionary<values=string, indices=int32>
    Countrycode: dictionary<values=string, indices=int32>
    Start: timestamp[us]
    PM10: double
    Validity_PM10: double
    Verification_PM10: double
    PM2.5: double
    Validity_PM2.5: double
    Verification_PM2.5: double
    filled_PM2.5: bool
    O3: double
    Validity_O3: int32
    Verification_O3: int32
    NO2: double
    Validity_NO2: double
    Verification_NO2: double
    SO2: double
    Validity_SO2: double
    Verification_SO2: double

How many missing PM2.5 values were filled? (`filled_PM2.5==1`)

``` r
res |> 
  count(filled_PM2.5) |> 
  collect()
```

    # A tibble: 2 × 2
      filled_PM2.5         n
      <lgl>            <int>
    1 TRUE          85006232
    2 FALSE        218020367
