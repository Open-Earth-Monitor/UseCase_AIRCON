
find_poll = function(x){
  n = names(x)
  poll = NULL
  for (p in c("PM10","PM2.5","O3","NO2","SO2")){
    if (p %in% n) poll = p
  }
  return(poll)
}

add_meta = function(aq){
  if (!any(c("Countrycode","Station.Type","Station.Area",
             "Longitude","Latitude") %in% names(aq))){
    meta_data = arrow::read_parquet("AQ_stations/EEA_stations_meta_table.parquet")
    by = "Air.Quality.Station.EoI.Code"
    aq = dplyr::left_join(aq, meta_data, by = by)
  }
  return(aq)
}

create_dir_if = function(x, recursive = T){
  suppressMessages({
    new = lapply(x, function(d, r = recursive) if (!dir.exists(d)) dir.create(d, recursive = r))
  })
}

## Interpolation ===============================================================

load_aq = function(poll, stat, y, m=0, d=0, sf = T, verbose = T){
  if (!any(m==0, d==0)) stop("Supply only either m (month, 1-12) or d (day of year; 1-365).")
  if (!poll %in% c("PM10","PM2.5","O3","NO2", "SO2")) stop("Select one of PM10, PM2.5, O3, NO2, SO2.")
  if (!stat %in% c("perc", "mean", "max")) stop("Select one of mean, max or perc.")
  tdir = file.path("AQ_data", ifelse(m == 0 & d == 0, "03_annual", ifelse(d == 0, "02_monthly", "01_daily")))
  
  info = paste(toupper(substr(basename(tdir), 4, nchar(basename(tdir)))), poll, "data. Year =", y)
  
  # daily
  if (d > 0){
    tdir = dir(tdir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    info = paste(info, "; Day of Year =", d, "; Variable =", basename(tdir))
    x = arrow::open_dataset(tdir) |> dplyr::filter(year == y, doy == d) #|> dplyr::collect()
    # monthly
  } else if (m > 0){
    tdir = dir(tdir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(tdir)==1)
    info = paste(info, "; Month =", m, "; Variable =", basename(tdir))
    x = arrow::open_dataset(tdir) |> dplyr::filter(year == y, month == m)
    # annual
  } else {
    tdir = dir(tdir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(tdir)==1)
    info = paste(info, "; Variable =", basename(tdir))
    x = arrow::open_dataset(tdir) |> dplyr::filter(year == y)
  }
  x = dplyr::collect(x) |> add_meta()
  if (sf) x = sf::st_as_sf(x, coords=c("Longitude", "Latitude"), crs = sf::st_crs(4326), remove = F) |> 
    sf::st_transform(sf::st_crs(3035))
  attr(x, "stat") = stat
  attr(x, "y") = y
  attr(x, "m") = m
  attr(x, "d") = d
  if (verbose) message(info, "; n = ", nrow(x))
  return(x)
}

get_filename = function(tdir, varname, y, perc = NULL,
                        base.dir = "supplementary"){
  varname = sub("\\.", "p", varname)
  list.files(file.path(base.dir, tdir), 
             pattern = glob2rx(paste0(tolower(varname), "*",perc, "*", y, ".nc")), 
             full.names = T)[1]
}

# stars versions
warp_to_target = function(x, target, name, method="bilinear", mask = T){
  x = stars::st_warp(x, target, method = method, use_gdal = T, 
                     no_data_value=-9999, options = c("NUM_THREADS=ALL_CPUS"))
  #x = stars::st_warp(x, target, segments=5000)
  if (mask) x[is.na(target)] = NA
  x = sf::st_normalize(x)|> setNames(name)
  gc(verbose = F)
  return(x)
}

read_n_warp = function(x, lyr, target, ...){
  if (class(target)=="character") target = stars::read_stars(target)
  suppressWarnings({
    x = stars::read_stars(x, proxy = F)[,,,lyr, drop=T] 
  })
  # set CRS to WGS84 if missing
  if (sf::st_crs(x)==sf::st_crs(NA)){
    x = sf::st_set_crs(x, sf::st_crs(4326))
  }
  
  warp_to_target(x, target, ...)
}

load_covariates = function(x, tg){
  # stop if tg not elevation layer
  # ...
  poll = find_poll(x)
  stat = attr(x, "stat")
  perc = switch(stat,
                "perc" = "perc",
                NULL)
  
  y = attr(x, "y")
  m = attr(x, "m")   # handle missing month (annual maps) here
  d = attr(x, "d")
  tdir = ifelse(d > 0, "01_daily", ifelse(m > 0, "02_monthly", "03_annual"))
  lyr = ifelse(d > 0, d, ifelse(m > 0, m, 1))
  dtime = paste("year =", y, ifelse(m > 0, paste("month =", m), paste("doy =", d)))
  grid_resolution_km = as.integer(stars::st_dimensions(tg)$x$delta / 1000)
  
  # handle missing S5P data (starts 05/2018)
  lyr_s5p = lyr
  if (poll == "NO2" & y == 2018){
    if (m > 0){
      lyr_s5p = lyr - 4        # shift layer index by 4 missing months
    } else if (d > 0){
      lyr_s5p = lyr - 120      # shift layer index by 120 missing days
    }}
  
  # measurement-level (needed by all pollutants)
  cams_name = paste0("CAMS_", poll)
  cams = read_n_warp(get_filename(tdir, poll, y, perc), lyr,
                     name = cams_name, target = tg)
  ws = read_n_warp(get_filename(tdir, "wind_speed", y), lyr,
                   name = "WindSpeed", target = tg)
  
  # measurement-level (individual for each pollutant)
  l = switch(poll, 
             "PM10" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, tg,                 
                           warp_to_target(
                             x = stars::read_stars("supplementary/static/clc/CLC_NAT_percent_1km.tif"),
                             target = tg, name = "CLC_NAT_1km"),
                           read_n_warp(get_filename(tdir, "rel_humidity", y), lyr, 
                                       target = tg, name = "RelHumidity")), 
             
             "PM2.5" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, tg,
                            warp_to_target(
                              x = stars::read_stars("supplementary/static/clc/CLC_NAT_percent_1km.tif"),
                              target = tg, name = "CLC_NAT_1km")),
             
             "O3" = list(cams, ws, tg,
                         read_n_warp(get_filename(tdir, "solar_radiation", y), lyr,
                                     target = tg, name = "SolarRadiation")),
             
             "NO2" = list(cams, ws, tg,
                          suppressWarnings({
                            warp_to_target(x = stars::read_ncdf(get_filename(tdir, "s5p_no2", y),
                                                                var = "NO2_TROPOMI",
                                                                ncsub = cbind(start = c(1, 1, lyr_s5p), 
                                                                              count=c(6500, 4600, 1)), 
                                                                proxy = F)[,,,drop=T],
                                           target = tg, name = "TROPOMI_NO2")
                          }),
                          # warp_to_target(target = tg, name = "Elevation_5km_radius",
                          #                x = stars::read_stars("supplementary/static/COP-DEM/COP_DEM_Europe_5km_radius_1km_epsg3035.tif")),
                          # warp_to_target(target = tg, name = "PopulationDensity",
                          #                x = stars::read_stars("supplementary/static/pop_density_1km_epsg3035.tif")),
                          # warp_to_target(target = tg, name = "CLC_NAT_1km",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_NAT_percent_1km.tif")),
                          # warp_to_target(target = tg, name = "CLC_AGR_1km",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_AGR_percent_1km.tif")),
                          # warp_to_target(target = tg, name = "CLC_TRAF_1km",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_TRAF_percent_1km.tif")),
                          # warp_to_target(target = tg, name = "CLC_NAT_5km_radius",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_NAT_percent_5km_radius_1km.tif")),
                          # warp_to_target(target = tg, name = "CLC_LDR_5km_radius",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_LDR_percent_5km_radius_1km.tif")),
                          # warp_to_target(target = tg, name = "CLC_HDR_5km_radius",
                          #                x = stars::read_stars("supplementary/static/clc/CLC_HDR_percent_5km_radius_1km.tif"))
                          
                          # Shortcut (for daily maps): pre-processed static covariates at 1 and 10 km
                          readRDS(paste0("supplementary/static/NO2_static_covariates_", grid_resolution_km,"km.rds"))
             )
  )
  
  strs = do.call("c", l)
  attr(strs, "pollutant") <- poll
  attr(strs, "stat") <- stat
  attr(strs, "dtime") <- dtime
  attr(strs, "y") <- y
  attr(strs, "m") <- m
  attr(strs, "d") <- d
  return(strs)
}

# if ("NO2" %in% name){
#      x = stars::read_ncdf("supplementary/01_daily/s5p_no2_daily_2019.nc", ncsub = cbind(start=c(1,1,23), count=c(6500,4600,1)), proxy = F)[,,,drop=T])
#    }

filter_area_type = function(x, area_type = "RB", mainland_europe = T){
  if (!area_type %in% c("RB", "UB", "UT", "JB")) stop("area_type should be one of c(RB, UB, UT, JB)!")
  if ("data.frame" %in% class(x)){
    x = switch(area_type,
               "RB" = dplyr::filter(x, Station.Type == "background", Station.Area == "rural"), 
               "UB" = dplyr::filter(x, Station.Type == "background", Station.Area %in% c("urban", "suburban")), 
               "UT" = dplyr::filter(x, Station.Type == "traffic", Station.Area %in% c("urban", "suburban")),
               "JB" = dplyr::filter(x, Station.Type == "background", Station.Area %in% c("rural", "urban", "suburban"))
    ) |>
      na.omit() |> 
      dplyr::select(-c(Countrycode, Station.Type, Station.Area))
    
    # if desired, restrict coordinates to mainland europe
    if (mainland_europe) x = dplyr::filter(x, dplyr::between(Longitude,-25,45),
                                           dplyr::between(Latitude, 30, 72))
  } else if ("stars" %in% class(x)){
    lookup = list("PM10" = list("RB" = c("log_CAMS_PM10", "Elevation", "WindSpeed", "RelHumidity", "CLC_NAT_1km"),
                                "UB" = c("log_CAMS_PM10"), 
                                "UT" = c("log_CAMS_PM10", "WindSpeed"), 
                                "JB" = c("log_CAMS_PM10", "Elevation", "WindSpeed", "RelHumidity", "CLC_NAT_1km")), 
                  "PM2.5"= list("RB" = c("log_CAMS_PM2.5", "Elevation", "WindSpeed","CLC_NAT_1km"),
                                "UB" = c("log_CAMS_PM2.5"), 
                                "UT" = c("log_CAMS_PM2.5"), 
                                "JB" = c("log_CAMS_PM2.5", "Elevation", "WindSpeed","CLC_NAT_1km")),
                  "O3"= list("RB" = c("CAMS_O3", "Elevation", "SolarRadiation"),
                             "UB" = c("CAMS_O3", "WindSpeed", "SolarRadiation"), 
                             "JB" = c("CAMS_O3", "Elevation", "WindSpeed", "SolarRadiation")), 
                  "NO2"= list("RB" = c("CAMS_NO2", "Elevation", "Elevation_5km_radius", "WindSpeed", "TROPOMI_NO2",
                                       "PopulationDensity", "CLC_NAT_5km_radius","CLC_LDR_5km_radius"),
                              "UB" = c("CAMS_NO2", "WindSpeed","TROPOMI_NO2",
                                       "PopulationDensity", "CLC_NAT_1km","CLC_AGR_1km","CLC_TRAF_1km",
                                       "CLC_LDR_5km_radius","CLC_HDR_5km_radius"), 
                              "UT" = c("CAMS_NO2", "Elevation", "Elevation_5km_radius", "WindSpeed", "TROPOMI_NO2",
                                       "CLC_LDR_5km_radius","CLC_HDR_5km_radius"), 
                              "JB" = c("CAMS_NO2", "Elevation", "Elevation_5km_radius", "WindSpeed", "TROPOMI_NO2",
                                       "PopulationDensity", "CLC_NAT_1km","CLC_AGR_1km","CLC_TRAF_1km",
                                       "CLC_NAT_5km_radius","CLC_LDR_5km_radius","CLC_HDR_5km_radius")))
    aq_attrs = attributes(x)
    poll = attr(x, "pollutant")
    predictor_set = c(lookup[[poll]][[area_type]])
    x = x[predictor_set]
    
    attributes(x)[c("pollutant", "stat", 
                    "dtime", "y", "m", "d")] = aq_attrs[c("pollutant", "stat", 
                                                          "dtime", "y", "m", "d")]
  }
  attr(x, "area_type") <- area_type
  return(x)
}

# spatial AND temporal cov
filter_st_coverage = function(aq, covariates, cov_threshold = 0.75){
  cov_column = ifelse(attr(aq, "d") > 0, 
                      "d_cov", ifelse(attr(aq, "m") > 0, "m_cov", "y_cov"))
  poll = find_poll(aq)
  
  aq = sf::st_filter(aq, sf::st_as_sfc(sf::st_bbox(covariates)))  |> 
    dplyr::filter(!!as.symbol(poll) > 0)
  aq = aq[aq[cov_column] |> sf::st_drop_geometry() > cov_threshold,]
  return(aq)
}

linear_aq_model = function(aq, covariates){
  
  if (!all(any(class(aq)=="sf"), class(covariates)=="stars")) stop("Please supply an sf-object (aq) and the corresponding covariates as stars-object.")
  
  poll = find_poll(aq)
  log_required = poll %in% c("PM2.5", "PM10")
  target = ifelse(log_required, paste0("log(", poll, ")"), poll) # log transform for PM
  
  aq_ext = stars::st_extract(covariates, aq) 
  aq_join = sf::st_join(dplyr::select(aq, dplyr::all_of(poll)), aq_ext) |>   # instead of select(aq, 1:{{poll}})
    unique() 
  
  # check for NAs and record index
  i.na = which(is.na(aq_join), arr.ind = T)
  i.ex = c()
  if (length(i.na) > 0) i.ex = i.na[,1] |> unique()
  if (log_required) {
    i.zero = which(aq_join[[poll]] < 0.00000001, arr.ind = T)
    i.ex = c(i.ex, i.zero)
  }
  if (length(i.ex) > 0) aq_join = aq_join[-i.ex,]
  
  frm = as.formula(paste(target, "~", paste(names(covariates), collapse = "+")))
  l = lm(frm, aq_join)
  attr(l, "exclude") = i.ex |> sort()
  attr(l, "log_transformed") = log_required       # log transform for PM
  attr(l, "formula") = frm
  return(l)
}

linear_aq_model_scale = function(aq, covariates){
  
  if (!all(any(class(aq)=="sf"), class(covariates)=="stars")) stop("Please supply an sf-object (aq) and the corresponding covariates as stars-object.")
  
  poll = find_poll(aq)
  log_required = poll %in% c("PM2.5", "PM10")
  target = ifelse(log_required, paste0("log(", poll, ")"), poll) # log transform for PM
  
  aq_ext = stars::st_extract(covariates, aq) 
  aq_join = sf::st_join(dplyr::select(aq, dplyr::all_of(poll)), aq_ext) |>   # instead of select(aq, 1:{{poll}})
    unique() # |> tidyr::drop_na()
  
  # check for NAs
  i.na = which(is.na(aq_join), arr.ind = T)
  i.ex = c()
  scale_fac = 1
  if (length(i.na) > 0) i.ex = i.na[,1] |> unique()
  if (log_required) {
    #aq_join = dplyr::filter(aq_join, !!rlang::sym(poll) > 0.0000001)  # avoid -Inf caused by log(0)
    scale_fac = 10000
    
    i.zero = which(aq_join[[poll]] < 0.00000001, arr.ind = T)
    i.ex = c(i.ex, i.zero)
  }
  if (length(i.ex) > 0) aq_join = aq_join[-i.ex,]
  
  frm = as.formula(paste(target, "*", scale_fac, "~", paste(names(covariates), collapse = "+")))
  l = lm(frm, aq_join)
  attr(l, "exclude") = i.ex |> sort()
  attr(l, "log_transformed") = log_required       # log transform for PM
  attr(l, "formula") = frm
  attr(l, "scale_fac") = scale_fac
  return(l)
}

lm_measures = function(l){
  rmse = caret::RMSE(l$model[,1] |> as.vector(),l$fitted.values) |> round(3)
  r2 = caret::R2(l$model[,1] |> as.vector(),l$fitted.values) |> round(3)
  return(list(lm_rmse = rmse, lm_r2 = r2))
}

cv_measures = function(cv_type, pred, val){
  cv = cbind(pred, val) |> na.omit()
  p_mean = mean(cv[,2], na.rm = T)
  rmse = caret::RMSE(cv[,1],cv[,2])
  c(
    "cv_type" = cv_type,
    "n" = nrow(cv),
    "poll_mean" = round(p_mean, 3),
    "r2" = round(caret::R2(cv[,1], cv[,2]), 3),
    "rmse" = round(rmse, 3),
    "r_rmse" = round((rmse / p_mean) *100, 3),
    "mpe" = round(sum(cv[,2] - cv[,1]) / nrow(cv), 3))
}

merge_stars_list = function(lst){
  do.call(rbind, lapply(lst, as.data.frame)) |> 
    stars::st_as_stars()
}

krige_aq_residuals = function(x, covariates, lm, n.min = 0, n.max = Inf, cluster = NULL, 
                              cv = T, show.vario = F, verbose = F){
  tictoc::tic()
  pollutant = attr(covariates, "pollutant")
  area_type = attr(x, "area_type")
  if (!is.null(attr(lm$model, "na.action"))){
    if (verbose) message("Exclude NAs")
    x = x[-attr(lm$model, "na.action"),]
  } 
  
  train.dat = lm$model[,2:ncol(lm$model)] |> 
    as.data.frame() |> 
    setNames(names(lm$model)[2:ncol(lm$model)])
  
  # exclude
  i.ex = attr(lm, "exclude")
  if (length(i.ex) > 0) x = x[-i.ex,]
  
  x = cbind(x, train.dat)   # bind geometries for kriging
  x$res = lm$residuals
  
  # CRS
  if (verbose) message("Assure CRS matching")
  x = sf::st_transform(x, sf::st_crs(covariates))
  
  # variogram
  if (verbose) message("Fit variogram")
  #v = gstat::variogram(res~1, x)
  #vr_m = gstat::fit.variogram(v, gstat::vgm(c("Exp", "Sph")))
  v = automap::autofitVariogram(res~1, x, model = c("Exp", "Sph"))
  vr_m = v$var_model
  
  if (show.vario){
    print(vr_m)
    print(plot(vr_m, cutoff = vr_m$range[2] * 1.3))
  } 
  
  vario_metrics = c(nugget = round(vr_m$psill[1],2), 
                    sill = round(vr_m$psill[1] + vr_m$psill[2], 2),
                    range = round(vr_m$range[2]/1000, 1))
  
  cv_msrs = c(cv_type = NA, n = NA, poll_mean = NA, r2 = NA, rmse = NA, r_rmse = NA, mpe = NA)
  
  if (is.null(cluster)){ 
    k = gstat::krige(res~1, x, covariates, vr_m, nmin = n.min, nmax = n.max)
  } else {
    n.cores = length(cluster)
    if (verbose) message("Kriging residuals in parallel using ", n.cores, " cores.")
    
    # export variables to cluster
    frm = Reduce(paste, deparse(attr(lm, "formula")))
    e = new.env()
    e$x = x; e$vr_m = vr_m; e$n.max = n.max; e$n.min = n.min; e$frm = frm
    parallel::clusterExport(cluster, list("x", "vr_m", "n.min", "n.max", "frm"), envir = e)
    
    # split prediction locations into chunks with equal numbers of non-NA values:
    if (verbose) message("Splitting new data.")
    n_cols = dim(covariates)[1]
    n_rows = dim(covariates)[2]
    
    dat.i = which(!is.na(covariates$lm_pred))
    splt = seq(1,length(dat.i), length.out = n.cores+1) |> ceiling()
    row.ids = (dat.i[splt] / n_cols) |> ceiling()
    row.ids[1] = 0; row.ids[length(row.ids)] = n_rows
    
    newdlst = lapply(as.list(1:n.cores), 
                     function(i){
                       from = row.ids[i] + 1
                       to = row.ids[i+1]
                       covariates[,,from:to]})
    # LOO-CV
    if (cv){
      if (verbose) message("LOO cross validation.")
      
      loo_pred = parallel::parSapply(cl, 1:nrow(x), loo_cv, 
                                     pts = x, frm = frm, v = vr_m)
      if (attr(lm, "log_transformed")) loo_pred = exp(loo_pred)
      
      cv_msrs = cv_measures("loo", loo_pred, dplyr::pull(x, pollutant))
    }
    
    # run kriging
    if (verbose) message("Kriging interpolation.")
    krige_lst = parallel::parLapplyLB(
      cluster, newdlst, 
      function(lst) gstat::krige(res~1, x, lst, vr_m, nmin = n.min, nmax = n.max), 
      chunk.size = 1)
    k = merge_stars_list(krige_lst)
    
    # finish up
    t = tictoc::toc(quiet = T)
    if (verbose) message("Completed.  ", t$callback_msg)
    gc(verbose = F)
    attr(k, "loo_cv") <- cv_msrs
    attr(k, "vario_metrics") <- vario_metrics
  }
  attr(k, "area_type") <- area_type
  return(k)
}

loo_cv = function(i, pts, frm, v){
  
  # leave one out
  a = pts[-i,]
  
  # linear prediction part
  l = lm(as.formula(frm), a)
  p = predict(l, pts[i,])
  
  # kriging part
  a$res = l$residuals
  k = gstat::krige(res~1, a, pts[i,], v, debug.level=0) |> as.data.frame()
  return(p + k$var1.pred)
}

combine_results = function(covariates, kriging, trim_range = NULL){
  # check extents/nrow/ncol
  stopifnot("Linear model prediction not found in covariates object. Make sure to first predict the linear model using 'aq_cov$lm_pred = predict(linmod, newdata = aq_cov)' befor combining with kriging results.)" = "lm_pred" %in% names(covariates))
  res = covariates["lm_pred"]
  
  if (!is.null(trim_range)){
    res$lm_pred = ifelse(res$lm_pred < trim_range[1], trim_range[1], res$lm_pred)
    res$lm_pred = ifelse(res$lm_pred > trim_range[2], trim_range[2], res$lm_pred)
  }
  
  # add LM prediction and Kriging-interpolated LM residuals
  res$residuals = kriging$var1.pred
  res$aq_pred = res$lm_pred + res$residuals
  
  # Avoid negative values
  res$aq_pred[res$aq_pred < 0] = 0
  
  # Calculate prediction standard error
  res$pred_se = sqrt(kriging$var1.var + (covariates$se ** 2))
  
  aq_attrs = attributes(covariates)
  attributes(res)[c("pollutant", "stat", "dtime")] = aq_attrs[c("pollutant", "stat", "dtime")]
  attr(res, "area_type") <- attr(kriging, "area_type")
  return(res)
}

define_mask = function(aq){
  cnt_mask = giscoR::gisco_countries[
    !giscoR::gisco_countries$CNTR_ID %in% aq$Countrycode,] |> 
    sf::st_transform(sf::st_crs(aq))
}

plot_aq_prediction = function(x, pollutant = NULL, stat=NULL, dtime=NULL, 
                              area_type=NULL, layer = "aq_pred", 
                              countries = T, pts=NULL, mask = NULL, ...){
  if (is.null(pollutant)) pollutant = attr(x, "pollutant")
  if (is.null(stat)) stat = attr(x, "stat")
  if (is.null(dtime)) dtime = attr(x, "dtime")
  if (is.null(area_type)) area_type = attr(x, "area_type")
  
  if (stat %in% c("perc","max")){
    breaks = switch(pollutant,
                    "PM10" = c(0, 20, 30, 40, 50, 75, 100),
                    "O3" = c(0, 90, 100, 110, 120, 140, 200))
  } else {
    breaks = switch(pollutant,
                    "PM10" = c(0, 15, 20, 30, 40, 50, 100),
                    "PM2.5" = c(0, 5, 10, 15, 20, 25, 40),
                    "NO2" = c(0, 10, 20, 30, 40, 45, 100))
  }
  
  max_val = dplyr::pull(x, layer) |> max(na.rm = T)
  if (max_val > max(breaks)) breaks[length(breaks)] = max_val+.1
  
  cols = c("darkgreen", "forestgreen", "yellow2", "orange", "red", "darkred")
  titl = paste(pollutant, stat, "Prediction", dtime, area_type)
  suppressMessages({
    plot(x[layer], col=cols, breaks = breaks, 
         main = titl, key.pos=1, reset=!countries, ...)
  })
  if (countries) plot(giscoR::gisco_countries$geometry |> 
                        sf::st_transform(sf::st_crs(x)), add=T)
  if (!is.null(mask)) plot(sf::st_geometry(mask), col = alpha("grey80", 0.7), add=T)
  if (!is.null(pts)) plot(pts, add=T, cex=0.4)
}

plot_aq_se = function(x, pollutant = NULL, stat=NULL, dtime=NULL, 
                      area_type=NULL, layer = "pred_se",
                      countries = T, ...){
  if (is.null(pollutant)) pollutant = attr(x, "pollutant")
  if (is.null(stat)) stat = attr(x, "stat")
  if (is.null(dtime)) dtime = attr(x, "dtime")
  if (is.null(area_type)) area_type = attr(x, "area_type")
  
  cols = c("cadetblue1", "cyan3", "darkcyan", "slateblue2", "purple2", "purple4")
  titl = paste(pollutant, stat, "Standard Error", dtime, area_type)
  suppressMessages({
    
    # safety net for when less breaks than colors
    tryCatch({
      plot(x[layer], col=cols, nbreaks=7, 
           main = titl, key.pos=1, reset=!countries, ...)
    }, 
    error = function(e) {
      plot(x[layer], main = titl, key.pos=1, reset=!countries, ...)
    })
    
  })
  if (countries) plot(giscoR::gisco_countries$geometry |> 
                        sf::st_transform(sf::st_crs(x)), add=T)
}

adjust_n_weight = function(x, poll){
  
  # pixel-based merging of individual layers 
  # from Horalek et al. 2019, formula 2.7, 2.8, 2.10, 2.11
  # jb = joint background; rb = rural background; ub = urban background
  # ut= = urban traffic; wt = weight traffic; wu = weight urban
  
  jb = x[1]; rb = x[2]; ub = x[3]
  if (length(x) == 6){
    ut = x[4]; wt = x[5]; wu = x[6]
  } else {
    ut = NULL; wt = x[4]; wu = x[5]
  }
  
  m = any(is.na(jb), is.na(rb), is.na(ub))  # create mask only from predictions
  if (m) {
    r = NA
  } else {
    
    # adjust layers using joint background layer
    # create boolean flags indicating if a value is _smaller_ / _greater_ or should be _replaced_ 
    
    # urban concentration expected to be higher than rural (PM, NO2)
    bool_rb_smaller_ub = rb <= ub
    bool_rb_greater_ub_smaller_jb = jb > rb & rb > ub
    
    bool_jb_replace_rb = !(bool_rb_smaller_ub | bool_rb_greater_ub_smaller_jb)
    bool_jb_replace_ub = !(bool_rb_smaller_ub | !bool_rb_greater_ub_smaller_jb)
    
    # opposite criteria for O3: rural is expected to be higher than urban
    if (poll == "O3"){
      bool_jb_replace_rb = !bool_jb_replace_rb
      bool_jb_replace_ub = !bool_jb_replace_ub
    }
    
    # apply adjustment for RB and UB
    if (bool_jb_replace_rb) rb = jb
    if (bool_jb_replace_ub) ub = jb
    
    # handle urban traffic layer (PM, NO2)
    if (!is.null(ut)){                                 # ---> O3; ignore the rest
      if (!is.na(ut)){                                 # ---> not O3 & ut != NA: proceed with check
        bool_ub_replace_ut = ! ut > ub                 # ---> check if background > traffic
        if (bool_ub_replace_ut) ut = ub                # ---> replace if ut less than ub
      }
    } else {
      ut = 0
    }
    
    # apply weights and merge (sum)
    
    rb_w = (1 - wu) * rb                # weighted RB
    ub_w = (1 - wt) * ub                # weighted UB
    ut_w = wt * ut                      # weighted UT (if applicable; results to 0 if not)
    u_w  = wu * (ub_w + ut_w)           # weighted urban    
    r = sum(rb_w, u_w, na.rm = T)       # sum of rural and urban (excluding NAs if na.rm = T)
  }
  return(r)
}

merge_aq_maps = function(paths, pse_paths = NULL, weights, cluster = NULL){
  
  # grab info from path
  info = strsplit(basename(paths[1]), "_")[[1]]
  pollutant = info[1]; stat = info[2]; area_type = ""
  dtime = paste("year =" , info[3], 
                ifelse(info[4] %in% as.character(1:12), 
                       paste("month =", info[4]), ""))
  
  # read AQ maps from file
  aq_interpolated = paths |> sort() |> 
    stars::read_stars(along="band") |> 
    setNames("value")
  a_types = stars::st_get_dimension_values(aq_interpolated, 3)
  
  # crop weights to map extent
  w = weights |> 
    sf::st_crop(sf::st_bbox(aq_interpolated)) |> 
    sf::st_normalize() |> 
    setNames("value")
  
  # AQ PREDICTIONS -------------------------------------------------------------i
  # adjust spatial resolution in case of O3 (10x10 to 1x1 km, aka 'disaggregate')
  if (dim(aq_interpolated)[1] != dim(w)[1]){
    message("warp to 1km grid...")
    tg = w[,,,1] 
    tg_3d = tg
    for (i in 2:length(a_types)) {
      tg_3d = c(tg_3d, tg, along="band")
    }
    
    # warp to target
    aq_interpolated = stars::st_warp(
      aq_interpolated, tg_3d, use_gdal = TRUE, no_data_value=-9999) |> 
      stars::st_set_dimensions(3, a_types)
  }
  
  # combine maps and weights
  components = c(aq_interpolated, w, along="band")
  
  message("merge AQ maps...")
  # reduce band dimension through pixel-level weighted mean
  aq_merge = stars::st_apply(components, 1:2, adjust_n_weight, 
                             poll = pollutant, .fname = "aq_pred",
                             CLUSTER = cluster, PROGRESS = T) 
  rm(components, aq_interpolated)
  gc(F)
  
  # PREDICTION STANDARD ERROR --------------------------------------------------i
  # same procedure for standard error maps
  if (!is.null(pse_paths)){
    message("merge PSE maps...")
    aq_pse = pse_paths |> sort() |> stars::read_stars(along="band") 
    
    # adjust spatial resolution in case of O3 (10x10 to 1x1 km, aka 'disaggregate')
    if (dim(aq_pse)[1] != dim(w)[1]){
      message("warp to 1km grid...")
      if (!exists("tg_3d")){
        tg = w[,,,1] 
        tg_3d = tg
        for (i in 2:length(a_types)) {
          tg_3d = c(tg_3d, tg, along="band")
        }
      } 
      # warp to target
      aq_pse = stars::st_warp(
        aq_pse, tg_3d, use_gdal = TRUE, no_data_value=-9999) |> 
        stars::st_set_dimensions(3, a_types) |> 
        setNames("value")
    }
    
    parallel::clusterEvalQ(cluster, {gc(verbose=F)})
    components_pse = c(aq_pse, w, along="band")
    aq_pse_merge = stars::st_apply(components_pse, 1:2, adjust_n_weight, 
                                   poll = pollutant, .fname = "pred_se",
                                   CLUSTER = cluster, PROGRESS = T) 
    aq_merge = c(aq_merge, aq_pse_merge)
    
    rm(components_pse, aq_pse, aq_pse_merge)
    gc(F) 
  }
  
  # inherit attributes
  attributes(aq_merge)[c("pollutant", "stat", "dtime", "area_type")] = 
    c(pollutant, stat, dtime, area_type)
  
  return(aq_merge)
}

merge_aq_maps_list = function(compo_list, weights, cluster = NULL, verbose = T){
  
  compo_list = compo_list[sort(compo_list |> names())]
  
  # grab info from path
  pollutant = attr(compo_list[[1]], "pollutant")
  stat = attr(compo_list[[1]], "stat")
  area_type = ""
  dtime = attr(compo_list[[1]], "dtime")
  
  # ======================================================     <<<<--------------------------------------------
  # read AQ maps from list
  aq_interpolated = purrr::map(compo_list, 3) |>
    stars::st_as_stars()
  stars::st_dimensions(aq_interpolated) = stars::st_dimensions(compo_list[[1]])
  aq_interpolated = merge(aq_interpolated, name = "band") |> setNames("value")
  a_types = stars::st_get_dimension_values(aq_interpolated, 3)
  
  # crop weights to map extent
  w = weights |> 
    sf::st_crop(sf::st_bbox(aq_interpolated)) |> 
    sf::st_normalize() |> 
    setNames("value")
  
  # AQ PREDICTIONS -------------------------------------------------------------i
  # adjust spatial resolution in case of O3 (10x10 to 1x1 km, aka 'disaggregate')
  if (dim(aq_interpolated)[1] != dim(w)[1]){
    if (verbose) message("warp to 1km grid...")
    tg = w[,,,1] 
    tg_3d = tg
    for (i in 2:length(a_types)) {
      tg_3d = c(tg_3d, tg, along="band")
    }
    
    # warp to target
    aq_interpolated = stars::st_warp(
      aq_interpolated, tg_3d, use_gdal = TRUE, no_data_value=-9999) |> 
      stars::st_set_dimensions(3, a_types)
  }
  
  # combine maps and weights
  components = c(aq_interpolated, w, along="band")
  
  if (verbose) message("merge AQ maps...")
  # reduce band dimension through pixel-level weighted mean
  aq_merge = stars::st_apply(components, 1:2, adjust_n_weight, 
                             poll = pollutant, .fname = "aq_pred",
                             CLUSTER = cluster, PROGRESS = T) 
  rm(components, aq_interpolated)
  gc(F)
  
  # PREDICTION STANDARD ERROR --------------------------------------------------i
  # same procedure for standard error maps
  if (verbose) message("merge PSE maps...")
  aq_pse = purrr::map(compo_list, 4) |>
    stars::st_as_stars()
  stars::st_dimensions(aq_pse) = stars::st_dimensions(compo_list[[1]])
  aq_pse = merge(aq_pse, name = "band") |> setNames("value")
  
  # adjust spatial resolution in case of O3 (10x10 to 1x1 km, aka 'disaggregate')
  if (dim(aq_pse)[1] != dim(w)[1]){
    if (verbose) message("warp to 1km grid...")
    if (!exists("tg_3d")){
      tg = w[,,,1] 
      tg_3d = tg
      for (i in 2:length(a_types)) {
        tg_3d = c(tg_3d, tg, along="band")
      }
    } 
    # warp to target
    aq_pse = stars::st_warp(
      aq_pse, tg_3d, use_gdal = TRUE, no_data_value=-9999) |> 
      stars::st_set_dimensions(3, a_types) |> 
      setNames("value")
  }
  
  parallel::clusterEvalQ(cluster, {gc(verbose=F)})
  components_pse = c(aq_pse, w, along="band")
  aq_pse_merge = stars::st_apply(components_pse, 1:2, adjust_n_weight, 
                                 poll = pollutant, .fname = "pred_se",
                                 CLUSTER = cluster, PROGRESS = T) 
  aq_merge = c(aq_merge, aq_pse_merge)
  
  rm(components_pse, aq_pse, aq_pse_merge)
  gc(F) 
  
  # inherit attributes
  attributes(aq_merge)[c("pollutant", "stat", "dtime", "area_type")] = 
    c(pollutant, stat, dtime, area_type)
  
  return(aq_merge)
}

check_map_progress = function(parent.dir){
  cog.dirs = grep("cog", list.dirs(parent.dir, recursive = T), value = T)
  polls = purrr::map(stringi::stri_split(cog.dirs, regex = "\\/"), 4) |> unlist()
  
  cog.files = purrr::map(cog.dirs, function(x) grep("pse", list.files(x, recursive = T, full.names = T), 
                                                    invert = T, value = T)) |> 
    unlist() |> as.data.frame() |> setNames("path")
  
  cog.files$poll = stringi::stri_split(cog.files$path, regex = "\\/") |> purrr::map(4) |> unlist()
  cog.files$freq = stringi::stri_split(cog.files$path, regex = "\\/") |> purrr::map(3) |> unlist()
  time = purrr::map(stringi::stri_split(cog.files$path, regex = "\\_"), grep, pattern="^20.*", value = T)
  cog.files$start = purrr::map(time, 1) |> unlist()
  cog.files$stop = purrr::map(time, 2) |> unlist()
  cog.files
  
  tab = table(cog.files$poll, cog.files$start) |> t() |> as.data.frame()
  tab$y = as.numeric(substr(tab$Var1,1,4))
  tab$m = as.numeric(substr(tab$Var1,5,6))
  tab$d = as.numeric(substr(tab$Var1,7,8))
  
  dplyr::group_by(tab, Var2, y) |> 
    dplyr::summarise(n=sum(Freq), .groups = "drop") |> 
    tidyr::pivot_wider(values_from = n, names_from = Var2)
}


