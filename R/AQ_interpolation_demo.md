# AQ Gapfilling
Johannes Heisig
2024-02-09

``` r
library(dplyr)
library(tictoc)

source("R/functions.R")
```

## Read AQ Data

Daily, monthly, or annual aggregates of AQ measurement data are imported
according using `load_aq()`. Required inputs are the `pollutant`, the
aggregation statistic (‘mean’ or ‘perc’) and a temporal selection.
Specifying only the year (`y`) returns annual data. Additionally,
specifying **either** the month (`m`) **or** the day of the year (`d`)
returns monthly or daily data, respectively.

``` r
y = 2020
m = 4
pollutant = "PM10"
stat = "perc"

aq = load_aq(pollutant, stat, y, m) 
```

    Loading monthly PM10 data. Year = 2020 ; Month = 4

## Add Covariates

Covariates are then loaded based using `load_covariates_EEA()`. The
temporal extent is extracted from the AQ measurement data object.
Thereby each covariate raster layer is warped to match the spatial grid
of the 1 x 1 km Corine Land Cover (CLC) dataset. Warping is faster when
setting `parallel=TRUE`.

By default our data covers the CAMS grid, which ranges from -25° to 45°
longitude and 30° to 72° latitude. We can limit the extent by supplying
a bounding box in the CRS of the data (*ETRS89-extended / LAEA Europe
(EPSG: 3035)*). For this example we limit the covariates to mainland
Europe and exclude e.g. Azores and Canary Islands.

Note: EEA uses different combinations of covariates for each pollutant,
which the function accounts for.

- PM10: log_cams, elevation, ws, rh, clc
- PM2.5: log_cams, elevation, ws, clc
- O3: cams, elevtion, ws, ssr
- NO2: cams, elevation, elevation 5km, ws, ssr, S5P, pop, clc, clc 5km

``` r
xmin = 2500000
ymin = 1400000
xmax = 7400000 
ymax = 5500000 
sp_ext = terra::ext(xmin, xmax, ymin, ymax)

tic()
aq_cov = load_covariates_EEA(aq, sp_ext, parallel = T)
toc()
```

    19.339 sec elapsed

``` r
aq_cov
```

    stars object with 2 dimensions and 5 attributes
    attribute(s), summary of first 1e+05 cells:
     log_CAMS_PM10     WindSpeed          CLC           Elevation      
     Min.   :1.19    Min.   :1.91    HDR    :     0   Min.   :  -5.22  
     1st Qu.:2.32    1st Qu.:2.04    LDR    :     0   1st Qu.:   0.00  
     Median :2.77    Median :2.23    IND    :     0   Median :   0.00  
     Mean   :2.59    Mean   :2.37    TRAF   :     0   Mean   : 117.38  
     3rd Qu.:2.92    3rd Qu.:2.72    UGR    :     0   3rd Qu.:  78.98  
     Max.   :3.03    Max.   :3.51    (Other):     0   Max.   :2298.20  
     NA's   :68166   NA's   :98318   NA's   :100000                    
      RelHumidity   
     Min.   :47.31  
     1st Qu.:52.28  
     Median :57.13  
     Mean   :57.18  
     3rd Qu.:61.53  
     Max.   :72.49  
     NA's   :98318  
    dimension(s):
      from   to  offset delta                       refsys x/y
    x    1 4900 2500000  1000 ETRS89-extended / LAEA Eu... [x]
    y    1 4100 5500000 -1000 ETRS89-extended / LAEA Eu... [y]

## Linear Models per Station-Area/-Type Group

Next, we need to specify the desired combination of station area and
type

- rural background (RB)
- urban background (UB)
- urban traffic (UT)

For each group, linear models are (a) trained and (b) predicted and
finally added to (c) kriging residuals. A post-processing procedure is
applied to merge the three outputs to a single map. In this example we
only use rural background stations as input.

``` r
station_area_type = "RB"
aq = filter_area_type(aq, area_type = station_area_type)              

linmod = linear_aq_model(aq, aq_cov)
summary(linmod)
```


    Call:
    lm(formula = frm, data = aq_join)

    Residuals:
         Min       1Q   Median       3Q      Max 
    -1.35544 -0.11738  0.00716  0.10891  0.72762 

    Coefficients:
                    Estimate Std. Error t value Pr(>|t|)    
    (Intercept)    5.430e-01  2.820e-01   1.926   0.0556 .  
    log_CAMS_PM10  9.269e-01  5.490e-02  16.884   <2e-16 ***
    WindSpeed      3.624e-02  2.204e-02   1.644   0.1018    
    CLCUGR         3.996e-02  1.476e-01   0.271   0.7868    
    CLCAGR         5.656e-03  7.951e-02   0.071   0.9434    
    CLCNAT        -6.328e-02  8.348e-02  -0.758   0.4494    
    CLCOTH         1.086e-01  1.255e-01   0.866   0.3876    
    Elevation      8.648e-05  6.089e-05   1.420   0.1572    
    RelHumidity   -5.121e-03  2.016e-03  -2.540   0.0119 *  
    ---
    Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

    Residual standard error: 0.2174 on 191 degrees of freedom
      (16 observations deleted due to missingness)
    Multiple R-squared:  0.7739,    Adjusted R-squared:  0.7644 
    F-statistic: 81.71 on 8 and 191 DF,  p-value: < 2.2e-16

## Predict Linear Model and Post-Process

Before predicting the linear model we check for missing classes of CLC
data in the training data. The model is not able to make predictions for
land cover classes it has not been trained for. Covariates are thus
masked accordingly. A reason for a CLC class to be missing can be that a
`station_area_type` such as ‘rural background’ by design excludes
e.g. traffic areas.

After prediction, a back-transformation is applied to target variables
which where log-transformed during model building.

``` r
# check for missing land cover classes 
aq_cov = mask_missing_CLC(aq_cov, linmod)
```

    AQ data has no observations with CLC class label(s) HDR, IND, TRAF.
    Corresponding pixels are masked in the covariate data.

``` r
# predict
aq_cov$lm_pred = predict(linmod, newdata = aq_cov)

# transform back
if (attr(linmod, "log_transformed") == TRUE){
  aq_cov["lm_pred"] = exp(aq_cov["lm_pred"])
}
```

## Residual Kriging

The residuals of the linear model can now be interpolated using
covariates. `krige_aq_residuals()` requires three objects:

- measurements (to retrieve locations)
- covariates (including a post-processed linear model prediction)
- linear model (to retrieve residuals and the formula)

The number of nearest neighbors to consider can be specified with
`n.max`. Kriging can be parallelized with `n.cores` \> 1.

``` r
k = krige_aq_residuals(aq, aq_cov, linmod, n.max = 10, 
                       show.vario = F, n.cores = 8)
```

    The legacy packages maptools, rgdal, and rgeos, underpinning the sp package,
    which was just loaded, were retired in October 2023.
    Please refer to R-spatial evolution reports for details, especially
    https://r-spatial.org/r/2023/05/15/evolution4.html.
    It may be desirable to make the sf package available;
    package maintainers should consider adding sf to Suggests:.

    Kriging residuals in parallel using 8 cores.

    1/3: Peparing cluster.

    2/3: Splitting new data.

    3/3: Kriging interpolation.

    Completed.  177.565 sec elapsed

## Result

Model prediction and Kriging output are merged using
`combine_results()`. To limit the linear model prediction to certain
value range, pass minimum and maximum to `trim_range`.

``` r
result = combine_results(aq_cov, k, trim_range = c(0,100))

plot_aq_interpolation(result)
```

    downsample set to 5

![](AQ_interpolation_demo_files/figure-commonmark/plot-1.png)

The example shows interpolated PM10 during April 2020, the time of the
first COVID-19-related lockdowns in Europe. Pollution was relatively low
compared to e.g April 2019.
