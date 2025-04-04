# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#       AIR QUALITY PREDICTION USING THE REGRESSION-INTERPOLATION-MERING-MAPPING (RIMM) METHOD
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#print(sessionInfo())
# LIBRARIES
.libPaths('~/R/library_new/') # R packages location

#for (p in c("proj4", "lwgeom", "stars", "gstat", "dplyr", "units", "giscoR", "doParallel",
#            "terra", "caret", "parallel", "tictoc", "lubridate", "gdalUtilities", "arrow")){
#  if (!requireNamespace(p, quietly = T)) {
#    message("Installing ", p)
#    install.packages(p, repos = "https://cran.uni-muenster.de/", dependencies = T, quiet = T)
#  }
#}

suppressPackageStartupMessages({
  library(stars)
  library(sf)
  library(gstat)
  library(arrow)
})

source("~/oemc_aq/functions.R")     # custom functions
setwd("/scratch/tmp/jheisig/aq")

# PROCESSING PARAMETERS
# collect arguments from the bash command that executes this script
args = commandArgs(trailingOnly=TRUE) # 1. variable of interest | 2. percentile or mean
pollutant = as.character(args[[1]])
stat = as.character(args[[2]])

# Map components, depending on indicator 
# (Rural Background)
area_types = c("RB", "UB", "JB")
if (pollutant != "O3") area_types = c(area_types, "UT")

# Kriging Nearest Neighbor settings according to Jan Horalek
n_min = 20
n_max = 50
n.cores = as.numeric(args[[3]])

# TEMPORAL EXTENT
vrsn = "v20250306"  # version name
re = F
years = c(2015:2023)
months = 1:12
if (re){
  years = rev(years)
  months = rev(months)
}

# CLUSTER
suppressMessages({
  cl = NULL
  if (n.cores > 1){
    cl = parallel::makeCluster(n.cores, outfile=paste0("~/cluster_output_",pollutant, "_", stat ,".dat"))
    doParallel::registerDoParallel(cl)
    parallel::clusterEvalQ(cl, {
      .libPaths('~/R/library_new/');
      library(gstat);
      library(stars);
    })
  }
  e = new.env()
})


# DIRECTORIES
outdir = paste0("AQ_maps/", vrsn, "/monthly/", pollutant, "_", stat)
tifdir = file.path(outdir, "tif")
pdfdir = file.path(outdir, "pdf", "components")
pdfdir_merged = file.path(outdir, "pdf", "merged")
cogdir = file.path(outdir, "cog")
create_dir_if(c(pdfdir, pdfdir_merged, cogdir, tifdir), recursive=T)

grid_resolution = ifelse(pollutant=="O3", 10, 1)
dem = readRDS(paste0(
  "supplementary/static/COP-DEM/COP_DEM_Europe_mainland_",
  grid_resolution, "km_mask_epsg3035.rds"))
#dem[[1]] = dem[[1]]|> as.integer()

weights = readRDS("supplementary/static/merge_weights/merge_weights_1km.rds")

perc = switch(stat,
              "perc" = ifelse(pollutant=="PM10", "_90.41", "_93.15"),
              NULL)

msrs_path = file.path(outdir, paste0(pollutant, "_", stat, "_measures.csv"))
model_msrs = matrix(nrow=0, ncol=17) |> as.data.frame()
if (file.exists(msrs_path)) model_msrs = read.csv(msrs_path)

#print(sessionInfo())

# ============================================================================

message("Start loop...")

for (y in years) {
  cogdir_y = file.path(cogdir, y)
  create_dir_if(cogdir_y)
  
  for (d in days) {
    if (pollutant == "NO2" && (y < 2018 | y == 2018 && d < 120)) {
      #message("NO2: skipping ", y, "-", d)
      cat(".")
      next()
    }
    
    st = as.Date(d, origin = paste(y, "01", "01", sep = "-")) |> format("%Y%m%d")
    en = st
    
    cog_path_aq = file.path(cogdir_y, paste0("air_quality.", tolower(pollutant), perc, "_1km_s_", 
                                             st, "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
    
    if (!file.exists(cog_path_aq)) {    
      # station data
      aq = load_aq(pollutant, stat, y, m) 
      aq = aq[!duplicated(aq$geometry),]  # safety measure to avoid kriging artifacts
      
      # covariates
      aq_cov = load_covariates(aq, dem)
      
      # empty list for storage
      compo_list = as.list(1:length(area_types)) |> setNames(area_types)
      
      for (a_type in area_types){
        message("mapping ", a_type, "                    ", Sys.time()|> round())
        
        # subset aq by the desired station area type
        aq_t = filter_area_type(aq, area_type = a_type) 
        aq_cov_t = filter_area_type(aq_cov, area_type = a_type)
        
        # linear model
        linmod = linear_aq_model(aq_t, aq_cov_t)
        #print(summary(linmod))
        msrs = lm_measures(linmod)
        message("1. linear model: ", paste(names(msrs), msrs, collapse = ", "), ", N = ", nrow(aq_t))
        
        # predict lm
        message("2. predicting...")
        aq_cov_t$lm_pred = predict(linmod, newdata = aq_cov_t)
        aq_cov_t$se = predict(linmod, newdata = aq_cov_t, se.fit = T)$se.fit
        
        #print(aq_cov_t)
        
        # residual kriging
        message("3. kriging...")
        k = krige_aq_residuals(aq_t, aq_cov_t, linmod, 
                               n.max = n_max, n.min = n_min, cv = F,
                               show.vario = F, verbose=F, cluster = cl)
        
        message("4. combining...")
        result = combine_results(aq_cov_t, k)
        
        # transform back
        if (attr(linmod, "log_transformed") == TRUE){
          message("5. transforming back...")
          result$aq_pred = exp(result$aq_pred)
          result$pred_se = exp(result$pred_se)
        }
        
        #write_stars(result, layer = "aq_pred", compo_paths[a_type])
        #write_stars(result, layer = "pred_se", pse_paths[a_type])
        compo_list[[a_type]] = result
        
        message("6. write - measure - plot...")
        model_msrs = rbind(model_msrs, 
                           c(poll=pollutant, stat=stat, year=y,month=m, type=a_type, 
                             msrs, attr(k, "loo_cv"), attr(k, "vario_metrics")))
        write.csv(model_msrs, msrs_path, row.names = F)
        
        #pdf_paths = file.path(pdfdir, sub(".tif", ".pdf", c(basename(compo_paths[a_type]), 
        #                        paste0("PSE_", basename(compo_paths[a_type])))))           
        #pdf(pdf_paths[1]); plot_aq_prediction(result); dev.off()
        #pdf(pdf_paths[2]); plot_aq_se(result); dev.off()
        
        #message(a_type, " complete. ---------------")
        rm(aq_cov_t, linmod, result)
        parallel::clusterEvalQ(cl, {gc(verbose=F)})
        gc(verbose=F)
      }
      
      # merge and write final map
      aq_merge = merge_aq_maps_list(compo_list, weights, cluster = cl)
      
      # aq tif and cog
      merged_tif_path = paste0(dirname(cog_path_aq), "/temp_", basename(cog_path_aq))
      write_stars(aq_merge, layer = "aq_pred", merged_tif_path)
      gdalUtilities::gdal_translate(merged_tif_path, cog_path_aq, of = "COG",
                                    co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
      unlink(merged_tif_path)
      
      
      # pse tif and cog
      cog_path_pse = file.path(cogdir_y, paste0("air_quality.pse_", tolower(pollutant), perc, "_1km_s_", 
                                                st, "_", en, "_eu_epsg.3035_", vrsn, ".tif"))
      pse_path = paste0(dirname(cog_path_pse), "/temp_", basename(cog_path_pse))
      write_stars(aq_merge, layer = "pred_se", pse_path)
      gdalUtilities::gdal_translate(pse_path, cog_path_pse, of = "COG",
                                    co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))
      unlink(pse_path)
      
      merged_pdf_paths = file.path(pdfdir_merged, sub(".tif", ".pdf", c(
        basename(cog_path_aq), basename(cog_path_pse))))
      pdf(merged_pdf_paths[1]); plot_aq_prediction(aq_merge); dev.off()
      pdf(merged_pdf_paths[2]); plot_aq_se(aq_merge); dev.off()                                    
      
      # simple CV: measurement vs combined map (already backtransformed!!)   
      message("Simple CV...")
      simple_msrs = cv_measures("simple", 
                                stars::st_extract(aq_merge, aq) |> 
                                  sf::st_drop_geometry() |> 
                                  dplyr::pull("aq_pred"), 
                                dplyr::pull(aq, pollutant))
      
      model_msrs = rbind(model_msrs, 
                         c(poll=pollutant, stat=stat, year=y, month=m, type="all", 
                           c("-", "-"), simple_msrs, vario_metrics = c("-", "-", "-")))
      write.csv(model_msrs, msrs_path, row.names = F)
      
      message(paste("AQ maps merged and converted to COG.\n======================================"))
      rm(aq_merge)
      parallel::clusterEvalQ(cl, {gc(verbose=F)})
      gc(verbose=F)
      tictoc::toc()
    }
  }
}

parallel::stopCluster(cl)

