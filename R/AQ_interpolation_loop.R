

# Process Air Quality Maps for Europe to be displayed in app.earthmonitor.org


library(dplyr)
library(stars)
library(tictoc)

source("R/functions.R")

pollutant = "PM10"
stat = "perc"
years = c(2019, 2020)
months = 1:12
area_types = c("RB", "UB", "UT")
outdir = "AQ_maps/PM10_monthly_Covid"

xmin = 2500000
ymin = 1400000
xmax = 7400000 
ymax = 5500000 
sp_ext = terra::ext(xmin, xmax, ymin, ymax)

weights = read_stars(c(
  "supplementary/static/merge_weights/traffic_weights_1km.tif",
  "supplementary/static/merge_weights/urban_weights_1km.tif"),
  along = "bands") |> 
  st_set_dimensions("bands", c("w_traffic","w_urban")) 


clus = c(rep("localhost", 8))
cl = parallel::makeCluster(clus, type = "SOCK")
parallel::clusterEvalQ(cl, library(gstat))


msrs_path = file.path(outdir, "lm_measures.csv")
#lm_msrs = read.csv(msrs_path)[,-1]
lm_msrs = matrix(nrow=0, ncol=5) |> as.data.frame()

for (y in years){
  for (m in months){
    
    # if any area_type is missing for this year-month combo, read covariates just once
    compo_paths = file.path(
      outdir, paste0(paste(pollutant, stat, y, m, area_types, sep = "_"), ".tif")) |> 
      setNames(area_types)
    
    if (any(!file.exists(compo_paths))){
      
      aq = load_aq(pollutant, stat, y, m) 
      aq_cov = load_covariates_EEA(aq, sp_ext, parallel = T)
      
      for (t in area_types){
        if (!file.exists(compo_paths[t])){
          message(t, "  ", Sys.time()|> round())
          
          # subset aq by the desired station area type
          aq_t = filter_area_type(aq, area_type = t) |> 
            filter_st_coverage(aq_cov)
          
          # linear model
          linmod = linear_aq_model(aq_t, aq_cov)
          msrs = lm_measures(linmod)
          message(paste(names(msrs), msrs, collapse = " "))
          lm_msrs = rbind(lm_msrs, c(y=y,m=m,t=t,msrs))
          write.csv(lm_msrs, msrs_path)
          #print(linmod$model$CLC |> summary())

          # handle missing land cover classes
          aq_cov_mask = mask_missing_CLC(aq_cov, linmod)

          # predict lm
          aq_cov_mask$lm_pred = predict(linmod, newdata = aq_cov_mask)

          # transform back
          if (attr(linmod, "log_transformed") == TRUE){
            aq_cov_mask["lm_pred"] = exp(aq_cov_mask["lm_pred"])
          }

          # residual kriging
          k = krige_aq_residuals(aq_t, aq_cov_mask, linmod, n.max = 10,
                                 show.vario = F, cluster = cl)

          result = combine_results(aq_cov_mask, k)#, trim_range = c(0,100))
          write_stars(result, layer = "aq_interpolated", compo_paths[t])

          #plot_aq_interpolation(result, downsample = 5)
        }
      }
    }
    # merge and write final map
    st = as.Date(paste(y,m,"01",sep = "-"))
    en = (lubridate::ceiling_date(st, "month") - 1 ) |> format("%Y%m%d")
    
    cog_path = paste0("air_quality.pm10_p90.41_1km_s_",  format(st, "%Y%m%d"), "_", en, "_eu_epsg.3035_v20240225.tif")
    cog_path = file.path("AQ_maps/PM10_monthly_Covid", "cog", cog_path)
    if (!dir.exists(dirname(cog_path))) dir.create(dirname(cog_path), recursive = T)
    
    if (!file.exists(cog_path)){
      aq_merge = merge_aq_maps(compo_paths, weights, cluster = cl)
      
      tif_path = sub("RB.tif","merged.tif", compo_paths[1])
      write_stars(aq_merge, tif_path)
      
      gdalUtilities::gdal_translate(tif_path, cog_path, of = "COG",
                     co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
      
      plot_aq_interpolation(aq_merge, pollutant, stat, y, m, layer = 1)
      message("AQ map completed.\n======================================")
    }
  }
  gc()
}

parallel::stopCluster(cluster)

#===============================================================================

library(ggplot2)

lm_msrs$dt = as.Date(paste(lm_msrs$y, lm_msrs$m, "15", sep = "-"))
ggplot(lm_msrs, aes(x=dt,y=RMSE, color=t, size = R2)) + 
  geom_point()

ggplot(lm_msrs, aes(x=t,y=RMSE)) + 
  geom_boxplot()
ggplot(lm_msrs, aes(x=t,y=R2)) + 
  geom_boxplot()


# gdal_translate filename.TIF newfileCOG.TIF -of COG -co COMPRESS=LZW

