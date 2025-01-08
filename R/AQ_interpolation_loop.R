

# Process Air Quality Maps for Europe to be displayed in app.earthmonitor.org


library(dplyr)
library(stars)
library(tictoc)

source("R/functions.R")

pollutant = "NO2"
stat = "mean"
#stat = "perc"
years = c(2021)
months = 4:6
vrsn = "v20240731"

area_types = c("RB", "UB", "JB")
if (pollutant != "O3") area_types = c(area_types, "UT")

outdir = paste0("AQ_maps/test2/monthly/", pollutant, "_", stat)
pdfdir = file.path(outdir, "pdf")
if (!dir.exists(pdfdir)) dir.create(pdfdir, recursive = T)

xmin = 2500000
ymin = 1400000
xmax = 7400000
ymax = 5500000
sp_ext = terra::ext(xmin, xmax, ymin, ymax)

test_subset = T

weights = read_stars(c(
  "supplementary/static/merge_weights/traffic_weights_1km.tif",
  "supplementary/static/merge_weights/urban_weights_1km.tif"),
  along = "band") |> 
  st_set_dimensions("band", c("w_traffic","w_urban")) 


clus = c(rep("localhost", 8))
cl = parallel::makeCluster(clus, type = "SOCK")
doParallel::registerDoParallel(cl)
parallel::clusterEvalQ(cl, c(library(gstat), library(stars)))

e = new.env()

perc = switch(stat,
              "perc" = ifelse(pollutant=="PM10", "_90.41", "_93.15"),
              NULL)

msrs_path = file.path(outdir, paste0(pollutant, "_", stat, "_measures.csv"))
model_msrs = matrix(nrow=0, ncol=17) |> as.data.frame()
if (file.exists(msrs_path)) model_msrs = read.csv(msrs_path)[,-1]
                  

 y = 2021
 m = 4
 n_max = 5
 t = "UB"

for (y in years){
  for (m in months){
    
    # if any area_type is missing for this year-month combo, read covariates just once
    compo_paths = file.path(
      outdir, paste0(paste(pollutant, stat, y, m, area_types, sep = "_"), ".tif")) |> 
      setNames(area_types)
    pse_paths = paste0(dirname(compo_paths[1]), "/PSE_", basename(compo_paths)) |> 
      setNames(area_types)
    
    aq = load_aq(pollutant, stat, y, m) 
    
    if (any(!file.exists(compo_paths))){
      
      aq_cov = load_covariates_EEA(aq, sp_ext)
      
      for (t in area_types){
      #t = "UB"
      if (!file.exists(compo_paths[t])){
        message(t, "  ", Sys.time()|> round())
        
        # subset aq by the desired station area type
        aq_t = filter_area_type(aq, area_type = t) 
        aq_cov_t = filter_area_type(aq_cov, area_type = t)
        
        # linear model
        linmod = linear_aq_model(aq_t, aq_cov_t)
        msrs = lm_measures(linmod)
        message("1. linear model: ", paste(names(msrs), msrs, collapse = ", "), ", N = ", nrow(aq_t))
        
        # predict lm
        message("2. predicting...")
        aq_cov_t$lm_pred = predict(linmod, newdata = aq_cov_t)
        aq_cov_t$se = predict(linmod, newdata = aq_cov_t, se.fit = T)$se.fit
        
        if (test_subset){
          message("-----------> apply test subset.")
          aq_att = attributes(aq_cov_t)
          aq_cov_t = aq_cov_t[,1000:1500, 2000:2500]
          attributes(aq_cov_t) = c(attributes(aq_cov_t) , aq_att[4:length(aq_att)])
        } 

        # residual kriging
        message("3. kriging...")
        k = krige_aq_residuals_2(aq_t, aq_cov_t, linmod, 
                                 n.max = n_max, cv = T,
                                 show.vario = F, verbose=T, cluster = cl)

        
        message("4. combining...")
        result = combine_results(aq_cov_t, k)#, trim_range = c(0,100))
        
        
        # transform back if needed
        if (attr(linmod, "log_transformed") == TRUE){
          message("5. transforming back...")
          result$aq_pred = exp(result$aq_pred)
          result$pred_se = exp(result$pred_se)
        }
        
        write_stars(result, layer = "aq_pred", compo_paths[t])
        write_stars(result, layer = "pred_se", pse_paths[t])
        
        model_msrs = rbind(
          model_msrs, c(poll=pollutant, stat=stat, year=y,month=m, type=t, 
                        msrs, attr(k, "loo_cv"), attr(k, "vario_metrics")))
        write.csv(model_msrs, msrs_path)
        
        pdf_paths = file.path(
          dirname(compo_paths[1]), "pdf",
          sub(".tif", ".pdf",
              c(basename(compo_paths[t]),
                paste0("PSE_",basename(compo_paths[t])))))
        #pdf(pdf_paths[1])
        plot_aq_prediction(result)
        #dev.off()
        #pdf(pdf_paths[2])
        plot_aq_se(result)
        #dev.off()

        
        message(t, " complete. ---------------")
        gc()
      }
    }
  }
  
  # merge and write final map
  st = as.Date(paste(y,m,"01",sep = "-"))
  en = (lubridate::ceiling_date(st, "month") - 1 ) |> format("%Y%m%d")
  
  cog_path_aq = file.path(dirname(compo_paths[1]), "cog",
                          paste0("air_quality.", tolower(pollutant), perc,"_1km_s_", 
                                 format(st, "%Y%m%d"), "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
  
  cog_path_pse = file.path(dirname(compo_paths[1]), "cog",
                           paste0("air_quality.pse_", tolower(pollutant), perc,"_1km_s_", 
                                  format(st, "%Y%m%d"), "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
  
  if (!dir.exists(dirname(cog_path_aq))) dir.create(dirname(cog_path_aq), recursive = T)
  
  if (!file.exists(cog_path_aq)){
    
    aq_merge = merge_aq_maps(compo_paths, pse_paths, weights = weights, cluster = cl)
    
    plot_aq_prediction(aq_merge)
    
    tif_path = sub("RB.tif","merged.tif", compo_paths[1])
    write_stars(aq_merge, layer = "aq_pred", tif_path)
    
    pse_path = paste0(dirname(tif_path), "/PSE_", basename(tif_path)) 
    write_stars(aq_merge, layer = "pred_se", pse_path)
    
    gdalUtilities::gdal_translate(tif_path, cog_path_aq, of = "COG",
                                  co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
    gdalUtilities::gdal_translate(pse_path, cog_path_pse, of = "COG",
                                  co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
    
    # simple CV: measurement vs combined map (already backtransformed!!)
    simple_msrs = cv_measures("simple", 
                              stars::st_extract(aq_merge, aq) |> 
                                sf::st_drop_geometry() |> 
                                dplyr::pull("aq_pred"), 
                              dplyr::pull(aq, pollutant))
    
    model_msrs = rbind(model_msrs, 
                       c(poll=pollutant, stat=stat, year=y,month=m, type="all", 
                         c("-","-"), simple_msrs, vario_metrics = c("-","-","-")))
    
    message(paste("AQ map (year =", y, ", month =", m,
                  ") completed.\n======================================"))
  }
}
}
parallel::stopCluster(cl)
    
    # 
    # #===============================================================================
    # 
    # library(ggplot2)
    # 
    # lm_msrs$dt = as.Date(paste(lm_msrs$y, lm_msrs$m, "15", sep = "-"))
    # ggplot(lm_msrs, aes(x=dt,y=RMSE, color=t, size = R2)) + 
    #   geom_point()
    # 
    # ggplot(lm_msrs, aes(x=t,y=RMSE)) + 
    #   geom_boxplot()
    # ggplot(lm_msrs, aes(x=t,y=R2)) + 
    #   geom_boxplot()
    
    
    # gdal_translate filename.TIF newfileCOG.TIF -of COG -co COMPRESS=LZW
    
    