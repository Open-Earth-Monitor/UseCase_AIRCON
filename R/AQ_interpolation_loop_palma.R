args = commandArgs(trailingOnly=TRUE) # 1. variable of interest | 2. percentile or mean | 3. k NN Kriging

pollutant = as.character(args[[1]])
stat = as.character(args[[2]])
n_max = as.numeric(args[[3]])
if (n_max == 0) n_max = Inf

n.cores = 8

.libPaths('~/R/library_new/') # PALMA

for (p in c("proj4", "lwgeom", "stars", "gstat", "dplyr", "units", "giscoR", "doParallel",
            "terra", "caret", "parallel", "tictoc", "lubridate", "gdalUtilities", "arrow")){
  if (!requireNamespace(p, quietly = T)) {
    message("Installing ", p)
    install.packages(p, repos = "https://cran.uni-muenster.de/", dependencies = T, quiet = T)
  }
}
# Process Air Quality Maps for Europe 
source("~/oemc_aq/functions.R")
setwd("/scratch/tmp/jheisig/aq")

suppressPackageStartupMessages({
  library(stars)
  library(dplyr)
  library(doParallel)
  library(gstat)
  library(tictoc)
})

suppressMessages({
  cl = NULL
  if (n.cores > 1){
    cl = parallel::makeCluster(n.cores, outfile="~/cluster_output.dat")
    registerDoParallel(cl)
    parallel::clusterEvalQ(cl, {
      .libPaths('~/R/library/');
      library(gstat);
      library(stars);
    })
  }
  e = new.env()
})

vrsn = "v20240813"

years = c(2015:2023)
months = 1:12

area_types = c("RB", "UB", "JB")
if (pollutant != "O3") area_types = c(area_types, "UT")

outdir = paste0("AQ_maps/test/monthly/", pollutant, "_", stat)
tifdir = file.path(outdir, "tif")
pdfdir = file.path(outdir, "pdf")
cogdir = file.path(outdir, "cog")
create_dir_if(c(pdfdir, cogdir, tifdir), recursive=T)

xmin = 2500000; ymin = 1400000; xmax = 7400000; ymax = 5500000 
sp_ext = terra::ext(xmin, xmax, ymin, ymax)

weights = read_stars(c(
  "supplementary/static/merge_weights/traffic_weights_1km.tif",
  "supplementary/static/merge_weights/urban_weights_1km.tif"),
  along = "band") |> 
  st_set_dimensions("band", c("w_traffic","w_urban"))

perc = switch(stat,
              "perc" = ifelse(pollutant=="PM10", "_90.41", "_93.15"),
              NULL)

msrs_path = file.path(outdir, paste0(pollutant, "_", stat, "_measures.csv"))
model_msrs = matrix(nrow=0, ncol=17) |> as.data.frame()
if (file.exists(msrs_path)) model_msrs = read.csv(msrs_path)

message("Start loop...")

for (y in years){
  for (m in months){
    compo_paths = file.path(tifdir, 
                            paste0(paste(pollutant, stat, y, m, area_types, sep = "_"), ".tif")) |> 
      setNames(area_types)
    pse_paths = paste0(tifdir, "/PSE_", basename(compo_paths)) |> 
      setNames(area_types)
    aq = load_aq(pollutant, stat, y, m) 
    
    # if any area_type is missing for this year-month combo, read covariates just once
    if (any(!file.exists(compo_paths))){
      
      message(pollutant, ": year = ", y, " - month = ", m)
      aq_cov = load_covariates_EEA(aq, sp_ext)
      
      for (t in area_types){
        if (!file.exists(compo_paths[t])){
          message("mapping ", t, "                    ", Sys.time()|> round())
          
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
          
          # residual kriging
          message("3. kriging...")
          k = krige_aq_residuals(aq_t, aq_cov_t, linmod, 
                                 n.max = n_max, cv = T,
                                 show.vario = F, verbose=F, cluster = cl)
          
          message("4. combining...")
          result = combine_results(aq_cov_t, k)
          
          # transform back
          if (attr(linmod, "log_transformed") == TRUE){
            message("5. transforming back...")
            result$aq_pred = exp(result$aq_pred)
            result$pred_se = exp(result$pred_se)
          }
          
          write_stars(result, layer = "aq_pred", compo_paths[t])
          write_stars(result, layer = "pred_se", pse_paths[t])
          
          message("6. write - measure - plot...")
          model_msrs = rbind(model_msrs, 
                             c(poll=pollutant, stat=stat, year=y,month=m, type=t, 
                               msrs, attr(k, "loo_cv"), attr(k, "vario_metrics")))
          write.csv(model_msrs, msrs_path, row.names = F)
          
          pdf_paths = file.path(pdfdir, sub(".tif", ".pdf", c(basename(compo_paths[t]), 
                                                              paste0("PSE_", basename(compo_paths[t])))))           
          pdf(pdf_paths[1]); plot_aq_prediction(result); dev.off()
          pdf(pdf_paths[2]); plot_aq_se(result); dev.off()
          
          message(t, " complete. ---------------")
          rm(aq_cov_t, linmod, result)
          parallel::clusterEvalQ(cl, {gc(verbose=F)})
          gc(verbose=F)
        }
      }
    }
    # merge and write final map
    st = as.Date(paste(y,m,"01",sep = "-"))
    en = (lubridate::ceiling_date(st, "month") - 1 ) |> format("%Y%m%d")
    
    cog_path_aq = file.path(cogdir, paste0("air_quality.", tolower(pollutant), perc,"_1km_s_", 
                                           format(st, "%Y%m%d"), "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
    
    if (!file.exists(cog_path_aq)){
      # merge
      aq_merge = merge_aq_maps(compo_paths, pse_paths, weights, cluster = cl)
      
      # aq tif and cog
      merged_tif_path = sub("RB.tif","merged.tif", compo_paths[1])
      write_stars(aq_merge, layer = "aq_pred", merged_tif_path)
      gdalUtilities::gdal_translate(merged_tif_path, cog_path_aq, of = "COG",
                                    co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
      merged_pdf_paths = file.path(pdfdir, 
                                   sub(".tif", ".pdf", c(basename(merged_tif_path), paste0("PSE_", basename(merged_tif_path)))))
      
      pdf(merged_pdf_paths[1]); plot_aq_prediction(aq_merge); dev.off()
      
      # pse tif and cog
      if ("pred_se" %in% names(aq_merge)){
        cog_path_pse = file.path(cogdir, paste0("air_quality.pse_", tolower(pollutant), perc,"_1km_s_", 
                                                format(st, "%Y%m%d"), "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
        pse_path = paste0(cogdir, "/PSE_", basename(merged_tif_path)) 
        write_stars(aq_merge, layer = "pred_se", pse_path)
        gdalUtilities::gdal_translate(pse_path, cog_path_pse, of = "COG",
                                      co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
        pdf(merged_pdf_paths[2]); plot_aq_se(aq_merge); dev.off()                                    
      }
      
      # simple CV: measurement vs combined map (already backtransformed!!)   
      message("Simple CV...")
      simple_msrs = cv_measures("simple", 
                                stars::st_extract(aq_merge, aq) |> 
                                  sf::st_drop_geometry() |> 
                                  dplyr::pull("aq_pred"), 
                                dplyr::pull(aq, pollutant))
      
      model_msrs = rbind(model_msrs, 
                         c(poll=pollutant, stat=stat, year=y,month=m, type="all", 
                           c("-","-"), simple_msrs, vario_metrics = c("-","-","-")))
      write.csv(model_msrs, msrs_path, row.names = F)
      
      message(paste("AQ maps merged and converted to COG.\n======================================"))
      rm(aq_merge)
      parallel::clusterEvalQ(cl, {gc(verbose=F)})
      gc(verbose=F)
    }
  }
}

parallel::stopCluster(cl)

