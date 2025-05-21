## --------------------------------------------------------------------------------------------------
#| output: false
library(dplyr)
library(tictoc)

source("/palma/home/j/jheisig/oemc_aq/functions.R")


## ----read-aq, warning=FALSE------------------------------------------------------------------------
y = 2015
m = 1
pollutant = "PM10"
stat = "perc"

aq = load_aq(pollutant, stat, y, m) 


## ----load-cov--------------------------------------------------------------------------------------
dem = readRDS("supplementary/static/COP-DEM/COP_DEM_Europe_mainland_10km_mask_epsg3035.rds")

aq_cov = load_covariates(aq, dem)

aq_cov


## ----linmod----------------------------------------------------------------------------------------
station_area_type = "RB"
aq = filter_area_type(aq, area_type = station_area_type)              

linmod = linear_aq_model(aq, aq_cov)
lm_measures(linmod)


## ----predict---------------------------------------------------------------------------------------
# predict
aq_cov$lm_pred = predict(linmod, newdata = aq_cov)
aq_cov$se = predict(linmod, newdata = aq_cov, se.fit = T)$se.fit
        
# transform back
if (attr(linmod, "log_transformed") == TRUE){
  aq_cov["lm_pred"] = exp(aq_cov["lm_pred"])
  aq_cov["se"] = exp(aq_cov["se"])
}


## --------------------------------------------------------------------------------------------------
library(doParallel)
n.cores = 8
cl = makeCluster(n.cores)
registerDoParallel(cl)
clusterEvalQ(cl, {
  library(gstat);
  library(stars)
})



## ----krige-----------------------------------------------------------------------------------------
tictoc::tic()
k = krige_aq_residuals(aq, aq_cov, linmod, n.max = 10, cv = T,
                       show.vario = F, cluster = cl, verbose = T)
tictoc::toc()

print(attr(k, "loo_cv"))


## ----plot, fig.height=7----------------------------------------------------------------------------
result = combine_results(aq_cov, k)

plot_aq_prediction(result)


## --------------------------------------------------------------------------------------------------
plot_aq_se(result)


## --------------------------------------------------------------------------------------------------
stopCluster(cl)

