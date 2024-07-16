## Downloads   ==================================================================

station_data_urls = function(country, pollutant, 
                             dataset = c(1,2), 
                             urlfile = NULL){
  if (!is.null(urlfile) && file.exists(urlfile)){
    stop("URL file already exists. Please use a different urlfile name.")
  }
  
  apiUrl = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
  endpoint = "ParquetFile/urls"
  
  # Request body
  request_body = list(
    "countries" = as.list(country),
    "cities" = list(),
    "properties"= as.character(pollutant) |> as.list(),
    "datasets" = as.list(datasets),
    "source" = "Api"
  ) |> jsonlite::toJSON(auto_unbox = T)
  
  res = httr::POST(url = paste0(apiUrl, endpoint), 
                   body = request_body, encode = "json", 
                   httr::content_type("application/json"))
  
  data = httr::content(res, "parse", encoding = "UTF-8", show_col_types = FALSE) 
  if (!is.null(urlfile)) writeLines(data$ParquetFileUrl, urlfile)
  as.list(data$ParquetFileUrl)
}

#' urls can be either a txt file with URLs a list returned by station_data_urls()
#' 
download_station_data = function(urls = "station_data_urls.txt", dir = NULL, cores = 1){
  
  if (class(urls) == "character") urls = readLines(urls)
  stopifnot("No URLs found in the provided input." = length(urls) > 0)
  urls = unlist(urls)
  if (is.null(dir)) dir = file.path(getwd(), "download")
  if (!dir.exists(dir)) {dir.create(dir); message("Created a download directory at ", dir)}
  
  filenames = gsub("https://eeadmz1batchservice02.blob.core.windows.net", dir, urls)
  dirnames = dirname(filenames) |> unique()
  d = purrr::map(dirnames, dir.create, recursive = T, showWarnings = F)
  
  fails = character(0)
  if (cores == 1){
    t = system.time({
      paths = purrr::map(1:length(urls), function(i){
        dest = filenames[i]
        if (!file.exists(dest)){  # && !dest %in% enc_list
          tryCatch(curl::curl_download(urls[i], dest),
                   error = function(e) {fails <<- c(fails, urls[i])}
          )
        }}, 
        .progress = list(type = "iterator", name = "Downloading", clear = F)) |> unlist()
    })
  } else if (cores > 1){
    t = system.time({
      paths = pbmcapply::pbmclapply(1:length(urls), function(i){
        dest = filenames[i]
        if (!file.exists(dest)){  # && !dest %in% enc_list
          tryCatch(curl::curl_download(urls[i], dest),
                   error = function(e) {fails <<- c(fails, urls[i])}
          )
        }}, mc.cores = cores)
    })
  }
  
  if (t[3] > 5) message("Download took ", round(t[3] |> unname()), " seconds.")
  if (length(fails) > 0){
    message("Failed downloads:")
    message(fails)
  }
  return(filenames)
}


filter_quality = function(x, validity = NULL, verification = NULL, remove = T){
  if (!is.null(x)){
    val.name = ver.name = NULL
    if (!is.null(validity)){
      val.name = grep("Validity", names(x), value = T)
      if (length(val.name) > 0){
        x = dplyr::filter(x, .data[[val.name]] %in% validity)
      }
    }
    if (!is.null(verification)){ 
      ver.name = grep("Verification", names(x), value = T)
      if (length(ver.name) > 0){ 
        x = dplyr::filter(x, .data[[ver.name]] %in% verification) 
      }
    }
    if (remove) x = dplyr::select(x, -dplyr::any_of(c(val.name, ver.name)))
  }
  return(x)
}

filter_stations = function(x, type = c("background","traffic"), 
                           area =  c("rural", "urban", "suburban")){
  dplyr::filter(x, Station.Type %in% type & Station.Area %in% area)
}

pivot_poll = function(x){
  if (all(c("Validity", "Verification") %in% names(x))){
    tidyr::pivot_wider(x, 
                       names_from = Air.Pollutant, 
                       values_from = c(Value, Validity, Verification), 
                       values_fn = max) |> 
      rename_with(~gsub("Value_", "", .x))
  } else {
    tidyr::pivot_wider(x, 
                       names_from = Air.Pollutant, 
                       values_from = Value, 
                       values_fn = max)
  }
}

arrow_substr <- function(context, string) {
  stringr::str_sub(string, 4)
}

arrow::register_scalar_function(
  name = "arrow_substr",
  fun = arrow_substr,  
  in_type = arrow::utf8(),
  out_type = arrow::utf8(),
  auto_convert = TRUE
)

open_station_data_by_country = function(country, all_files, 
                                        keep_validity, 
                                        keep_verification,
                                        meta = station_meta){
  
  country_files = grep(paste0("/", country, "/"), all_files, value = T)
  arrow::open_dataset(country_files) |> 
    dplyr::filter(AggType == "hour") |> 
    filter_quality(keep_validity = keep_validity,
                   keep_verification = keep_verification) |>
    dplyr::rename(Sampling.Point.Id = Samplingpoint) |> 
    dplyr::mutate(Sampling.Point.Id = arrow_substr(Sampling.Point.Id)) |> 
    dplyr::select(-c(End, Unit, Pollutant, AggType, ResultTime, 
                     DataCapture, FkObservationLog)) |> 
    dplyr::inner_join(meta, by = dplyr::join_by(Sampling.Point.Id)) |> 
    dplyr::select(-Sampling.Point.Id)
}

pivot_station_data = function(country, all_files, out_dir, 
                              keep_validity, keep_verification, ...){
  cat(country, "- ")
  out_file = file.path(out_dir, paste0(country,"_hourly.parquet"))
  if (!file.exists(out_file)){
    dd_split = open_station_data_by_country(country, all_files, 
                                            keep_validity, 
                                            keep_verification) |> 
      dplyr::group_by(Air.Quality.Station.EoI.Code) |> 
      dplyr::collect() |> 
      dplyr::group_split() |> 
      purrr::map(pivot_poll) |>
      dplyr::bind_rows() |> 
      dplyr::select(Air.Quality.Station.EoI.Code, Countrycode, Start, 
                    dplyr::contains(c("PM10", "PM2.5", "O3", "NO2", "SO2"))) |>
      dplyr::arrange(Air.Quality.Station.EoI.Code, Start) |> 
      write_pq_if(out_file, ...)
  }
}

preprocess_station_data = function(dir = "download", out_dir = "01_hourly", station_meta,
                                   keep_validity = 1, keep_verification = c(1,2), ...){
  stopifnot("Data directory does not exist." = dir.exists(dir))
  if (!dir.exists(out_dir)){
    dir.create(out_dir)
    message("Output directory doesn't exsit. Creating it now.")
  }
  if (is.character(station_meta)) station_meta = arrow::read_parquet(station_meta)
  
  station_meta = station_meta |>
    dplyr::select(Sampling.Point.Id, Air.Quality.Station.EoI.Code, 
                  Countrycode, Air.Pollutant)
  
  country_dirs = strsplit(list.dirs(dl_dir), "/") 
  max_l = max(lengths(country_dirs))
  countries = purrr::map(country_dirs, purrr::pluck(max_l)) |> 
    purrr::discard(is.null) |> 
    unique()
  
  dl_files = list.files(dir, recursive = T, full.names = T)

  x = purrr::map(countries, 
                 \(x) pivot_station_data (x,
                                          all_files = dl_files, 
                                          out_dir = out_dir, 
                                          keep_validity = keep_validity, 
                                          keep_verification = keep_verification, 
                                          ...))
}


add_meta = function(pollutant){
  if (!any(c("Countrycode","StationType","StationArea",
             "Longitude","Latitude") %in% names(pollutant))){
    meta_data = arrow::read_parquet("AQ_stations/EEA_stations_meta_table.parquet")
    by = "Air.Quality.Station.EoI.Code"
    pollutant = dplyr::left_join(pollutant, meta_data, by = by)
  }
  return(pollutant)
}



## Gapfilling  =================================================================

add_ymd = function(x, datetime_column = "Start"){
  dplyr::mutate(x, year = lubridate::year(.data[[datetime_column]]),
                month = lubridate::month(.data[[datetime_column]]),
                doy = lubridate::yday(.data[[datetime_column]]))
}

make_time_grid = function(x, step = 3600){
  stations = unique(x$Air.Quality.Station.EoI.Code) |> droplevels()
  
  time_min = min(x$Start) |> as.POSIXct() + step
  time_max = max(x$Start) |> as.POSIXct() + step
  time_steps = seq(time_min, time_max, by = step) 
  expand_grid(Air.Quality.Station.EoI.Code = stations, Start = time_steps)
}

extract_ts = function(at, x, varname = "ext_var", t_col = "Start"){
  ex = data.frame(at$Air.Quality.Station.EoI.Code, 
                  at$Start,
                  stars::st_extract(x, at, time_column = t_col) |> 
                    dplyr::pull(varname) |> 
                    units::drop_units()
  ) |> 
    setNames(c("Air.Quality.Station.EoI.Code", "Start", varname))
  gc()
  return(ex)
}

fill_pm2.5_gaps = function(country, model, population_table,
                           gap_data = "AQ_data/02_hourly_SSR",
                           out_dir = "AQ_data/03_hourly_gapfilled",
                           overwrite = F){
  
  out_file = file.path(out_dir, paste0(country, "_hourly_gapfilled.parquet"))
  if (!file.exists(out_file) | overwrite){
    
    data = arrow::open_dataset(gap_data) |> 
      dplyr::filter(Countrycode == country) |> 
      dplyr::collect()
    data = dplyr::left_join(data, population_table, 
                            by = dplyr::join_by(Air.Quality.Station.EoI.Code))
    
    pred = predict(model, data)
    ind = !is.na(data$PM10) & is.na(data$PM2.5)
    
    data[ind,"PM2.5"] = pred[ind]
    data$filled_PM2.5 = as.integer(0)
    data$filled_PM2.5[ind] = 1
    
    data = dplyr::select(data, Air.Quality.Station.EoI.Code, Countrycode, Start, 
                    dplyr::contains(c("PM10", "PM2.5", "O3", "NO2", "SO2")))
    
    write_pq_if(data, out_file, T, T)
    gc()
  }
  cat(paste0(country, " - "))
  return(out_file)
}

# Temporal aggregation =========================================================
#'
#'@param p Pollutant (O3, NO2, SO2, PM10, PM2.5)
#'@param step one of 'hour', 'day', or 'month'
#'@param by one of 'day', 'month', or 'year'
check_temp_cov = function(x, p, step, by = "year", collect = F){
  stopifnot("step should be one of 'hour', 'day', or 'month'." = step %in% c("hour","month","day"))
  stopifnot("by should be one of 'day', 'month', or 'year'." = by %in% c("year","month","day"))
  s = switch(step,
             "hour" = switch(by, "year" = 365*24, "month" = 30*24, "day" = 24),
             "day" = switch(by, "year" = 365, "month" = 30),
             "month" = 12)
  temp_vars_group = switch(by,
                           "year" = dplyr::vars(as.symbol("year")),
                           "month" = dplyr::vars(as.symbol("year"), as.symbol("month")),
                           "day" = dplyr::vars(as.symbol("year"), as.symbol("month"), as.symbol("doy")))
  vrs_group = c(dplyr::vars(Air.Quality.Station.EoI.Code), temp_vars_group)
  
  cov.v = paste0("cov.", by)
  suppressWarnings({
    x = dplyr::select(x, dplyr::everything(), v = p) |> 
      dplyr::mutate(
        one = 1, 
        Air.Quality.Station.EoI.Code = as.character(Air.Quality.Station.EoI.Code)) |> 
      dplyr::filter(!is.na(v)) |> 
      dplyr::group_by_at(vrs_group, .add = T) |> 
      dplyr::summarise(cov = sum(one)/s, AirPollutant = p, .groups = "drop") |> 
      dplyr::mutate(cov = dplyr::if_else(cov > 1, 1, cov)) |> 
      dplyr::rename(!!cov.v := cov)
  })
  
  if (collect){
    x = dplyr::collect(x)
  } else {
    x = arrow::as_arrow_table(x)
  }
  return(x)
}

filter_temp_cov = function(x, tc, p){
  polls = c("PM10","PM2.5","O3","NO2")
  thresh = c("PM10" = 10000, "PM2.5"= 10000, "O3"= 500, "NO2"= 350)
  
  tc = dplyr::filter(tc, AirPollutant == p) |> dplyr::select(1:2) 
  
  dplyr::select(x, -dplyr::all_of(polls), v = dplyr::all_of(p)) |> 
    dplyr::filter(!is.na(v) & v > 0 & v < thresh[p]) |> 
    dplyr::rename(!!p := v) |> 
    dplyr::mutate(Air.Quality.Station.EoI.Code = as.character(Air.Quality.Station.EoI.Code)) |>
    dplyr::inner_join(tc, by = dplyr::join_by(Air.Quality.Station.EoI.Code, year)) 
}

find_poll = function(x){
  n = names(x)
  poll = NULL
  for (p in c("PM10","PM2.5","O3","NO2","SO2")){
    if (p %in% n) poll = p
  }
  return(poll)
}


temp_agg_daily_mean = function(x){

    # temporal coverage
  suppressMessages({
    temp_cov_y = check_temp_cov(x, p = "v", step = "hour", by = "year", collect = T) |> 
      dplyr::select(1:3) 
    temp_cov_m = check_temp_cov(x, p = "v", step = "hour", by = "month", collect = T) |> 
      dplyr::select(1:4)
    temp_cov_d = check_temp_cov(x, p = "v", step = "hour", by = "day", collect = T) |> 
      dplyr::select(1:5)
    temp_cov = dplyr::full_join(temp_cov_y, temp_cov_m) |> 
      dplyr::full_join(temp_cov_d) |> 
      dplyr::select(dplyr::everything(), dplyr::starts_with("cov"))
  })
  
  # group by station and time
  # aggregate daily and join with temporal coverage info
  vrs_group = dplyr::vars(Air.Quality.Station.EoI.Code, year, month, doy)
  suppressWarnings({
    agg_d = dplyr::mutate(x, v = dplyr::if_else(v < 0, NA_real_, v)) |> 
      dplyr::group_by_at(vrs_group, .add = T) |> 
      dplyr::summarise(v = mean(v, na.rm = T), .groups = "drop") |> 
      tidyr::drop_na() |> 
      dplyr::left_join(temp_cov, by = dplyr::join_by(Air.Quality.Station.EoI.Code, year, month, doy))
  })
  return(agg_d)
}


temporal_aggregation = function(path, p, perc = NULL, 
                                cov_threshold = 0.75, 
                                out_dir = "AQ_data", 
                                keep_validity = NULL,
                                keep_verification = NULL,
                                overwrite = F){
  
  stopifnot('Pollutant (p) should be one of  PM10, PM2.5, O3, SO2, or NO2.' = 
              p %in% c("PM10","PM2.5","O3","SO2","NO2"))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = T)
  
  cntry = substr(basename(path), 1,2)
  metric = ifelse(is.null(perc), "mean", "perc")
  file_d = file.path(
    out_dir, paste0("04_daily/", p, "_", metric, "/", cntry, 
                    "_", p, "_daily_", metric, ".parquet"))
  
  t = system.time({
    #  check if daily mean has been processed already
    if (file.exists(file_d) & !overwrite){
      agg_d = arrow::read_parquet(file_d) |> 
        dplyr::select(dplyr::everything(), "v" = dplyr::all_of(p))
    } else {
      
      # if not: daily aggregate
      x = arrow::read_parquet(path) |> 
        dplyr::select(1:3, dplyr::contains(p)) |> 
        add_ymd() |> 
        # filter quality if provided
        filter_quality(validity = keep_validity, 
                       verification = keep_verification,
                       remove = T) |> 
        dplyr::select(dplyr::everything(), "v" = dplyr::all_of(p)) |> 
        dplyr::filter(v > 0, v < 9990) # sanity check
      # aggregate
      agg_d = temp_agg_daily_mean(x) 
    }
    # monthly / annual aggregates from daily
    if (nrow(agg_d) > 25){
      agg_m = dplyr::filter(agg_d, cov.month > cov_threshold) |> 
        dplyr::group_by(Air.Quality.Station.EoI.Code, year, month, cov.month) 
      agg_y = dplyr::filter(agg_d, cov.year > cov_threshold) |> 
        dplyr::group_by(Air.Quality.Station.EoI.Code, year, cov.year)
      
        # mean
      if (is.null(perc)){
        agg_m = dplyr::summarise(agg_m, v = mean(v, na.rm = T), .groups = "drop")
        agg_y = dplyr::summarise(agg_y, v = mean(v, na.rm = T), .groups = "drop")
      
        # percentile  
      } else {
        agg_m = dplyr::summarise(agg_m, v = quantile(v, perc, na.rm = T, names=F), .groups = "drop")
        agg_y = dplyr::summarise(agg_y, v = quantile(v, perc, na.rm = T, names=F), .groups = "drop")
      }
      
      # rename v to actual variable name and add station meta data
      agg_d = dplyr::filter(agg_d, cov.day > cov_threshold) |> 
        dplyr::select(1:4, dplyr::starts_with("cov."), v) |> 
        dplyr::rename(!!p := v) 
      agg_m = dplyr::rename(agg_m, !!p := v)
      agg_y = dplyr::rename(agg_y, !!p := v)
      
      pth = file.path(
        out_dir, 
        paste0(c("04_daily","05_monthly", "06_annual"), 
               "/", p, "_", metric, "/", cntry, "_", p, "_", 
               c("daily", "monthly", "annual"), "_", metric, ".parquet"))
      write_pq_if(agg_d, pth[1], overwrite = T)
      write_pq_if(agg_m, pth[2], overwrite = T)
      write_pq_if(agg_y, pth[3], overwrite = T)
    } 
  })
  return(list(Country = cntry, elapsed = as.numeric(round(t[3],1))))
}



# 8h rolling mean --------------------------------------------------------------
# https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-50/appendix-Appendix%20I%20to%20Part%2050

# expand an hourly temporal grid from earliest to latest timestemp
station_time_bounds = function(x){
  suppressWarnings({
  b = dplyr::group_by(x, Air.Quality.Station.EoI.Code) |>
    dplyr::summarise(time_min = min(Start),
                     time_max = max(Start))
  })
  duration = (b$time_max - b$time_min) |> as.numeric()
  b = b[duration > 10,]
  tidyr::drop_na(b)
}

station_time_grid = function(s, st_bounds){
  time_steps = seq(st_bounds$time_min[s], st_bounds$time_max[s], by = "hours") 
  expand.grid(Air.Quality.Station.EoI.Code = st_bounds$Air.Quality.Station.EoI.Code[s],
              Start = time_steps) |> 
    data.table::as.data.table()
}

# handle fill values for the start of the time series
get_left_fill = function(x){
  first6 = x[1:6]
  first7 = x[1:7]
  n_na = sum(is.na(first7))
  if (sum(is.na(first7)) == 0){
    fill = c(rep(NA, 5), mean(first6, na.rm = T),  mean(first7, na.rm = T))
  } else if (sum(is.na(first7)) == 1){
    fill = c(rep(NA, 6), mean(first7, na.rm = T))
  } else if (sum(is.na(first7)) > 1){
    fill = c(rep(NA, 7))
  }
  return(fill)
}

# slow but thorough method
roll_8h_mean = function(x){
  fill_left = get_left_fill(x)
  fill_left_valid = cumsum(!is.na(fill_left)) + 5
  n_valid = zoo::rollapply(x, 8, FUN = function(x) sum(!is.na(x)), 
                           align = "right", fill = fill_left_valid)
  
  m8h = zoo::rollapply(x, 8, mean, na.rm=T, align = "right", fill = fill_left)
  m8h[n_valid < 6] = NA
  m8h
}

# faster, but without improved NA filling at start of time series
roll_8h_mean_dt = function(x){
  # check for NAs
  n_valid = data.table::frollapply(x, 8, FUN = function(x) sum(!is.na(x)), 
                                   align = "right")
  # rolling mean
  m8h = data.table::frollmean(x, 8, na.rm=T, hasNA = T, align = "right")
  
  # replace first few values with original measurements, to avoid losing
  # data through rolling mean calculation (needs 6 values min, NA if less than 6)
  na_ind = which(is.na(m8h[1:8]))
  m8h[na_ind] = x[na_ind]
  m8h[n_valid < 6] = NA
  m8h
}

write_pq_if = function(x, path, check.dir = T, overwrite = F){
  if (check.dir & ! dir.exists(dirname(path))) dir.create(dirname(path), recursive = T)
  if (!file.exists(path) | overwrite) arrow::write_parquet(x, path)
}

temp_agg_daily_max8hm = function(i, bounds, x){
  
  ts_grid = station_time_grid(i, bounds)
  by = dplyr::join_by(Air.Quality.Station.EoI.Code, Start)
  ts_extended = dplyr::left_join(ts_grid, x, by = by, copy=T) |> 
    data.table::as.data.table()
  
  rm8h = dplyr::mutate(ts_extended, v = roll_8h_mean_dt(v)) |> add_ymd() |> 
    dplyr::select(Air.Quality.Station.EoI.Code, Start, year, month, doy, v)
  suppressMessages({
    temp_cov_y = check_temp_cov(rm8h, p = "v", step = "hour", by = "year", collect = T) |> 
      dplyr::select(1:3) 
    temp_cov_m = check_temp_cov(rm8h, p = "v", step = "hour", by = "month", collect = T) |> 
      dplyr::select(1:4) 
    temp_cov_d = check_temp_cov(rm8h, p = "v", step = "hour", by = "day", collect = T) |> 
      dplyr::select(1:5) 
    temp_cov = dplyr::full_join(temp_cov_y, temp_cov_m) |> dplyr::full_join(temp_cov_d)
  })
  
  suppressWarnings({
    rm8h_d = dplyr::group_by(rm8h, Air.Quality.Station.EoI.Code, year, month, doy) |> 
      dplyr::summarise(v = max(v, na.rm = T), .groups = "drop") |> 
      dplyr::mutate(v = dplyr::na_if(v, -Inf) ) |> 
      tidyr::drop_na() |> 
      dplyr::inner_join(temp_cov, by = dplyr::join_by(Air.Quality.Station.EoI.Code, year, month, doy)) 
  })
  gc()
  return(rm8h_d)
}

temporal_aggregation_running = function(path, p, perc, 
                                        cov_threshold = 0.75, 
                                        keep_validity = NULL,
                                        keep_verification = NULL,
                                        out_dir = "AQ_data", 
                                        overwrite = F){
  t = system.time({
    cntry = substr(basename(path), 1,2)
    cat(cntry, "- ")
    outpaths = file.path(
      out_dir, 
      paste0(c("04_daily","05_monthly", "06_annual"), "/",
             paste0(p, c("_max8h_mean","_max8h_perc", "_max8h_perc")), "/", cntry, "_",p,"_", 
             c("daily_max8h_mean", "monthly_max8h_perc", "annual_max8h_perc"), ".parquet"))
    
    if (!all(file.exists(outpaths), !overwrite)){
      
      #  check if daily max 8h mean has been processed already
      if (file.exists(outpaths[1])){
        rm8h_d = arrow::read_parquet(outpaths[1]) |> 
          dplyr::select(dplyr::everything(), "v" = dplyr::all_of(p))
      } else {
        # get temporal min/max per station
        x = arrow::read_parquet(path) |> 
          dplyr::select(1:3, dplyr::contains(p)) |> 
          filter_quality(validity = keep_validity,
                         verification = keep_verification, 
                         remove = T) |> 
          dplyr::select(dplyr::everything(), "v" = dplyr::all_of(p)) |> 
          dplyr::filter(v > 0, v < 9990) # sanity check
        
        bounds = station_time_bounds(x)
        
        if (nrow(bounds) > 0){
          # daily max of 8h mean
          rm8h_d = purrr::map_dfr(1:nrow(bounds), temp_agg_daily_max8hm, 
                                  bounds = bounds, x = x)
        } else {
          rm8h_d = data.frame()
        }
      }
      if (nrow(rm8h_d) > 0){
        # monthly / annual percentile of daily max of 8h mean
        rm8h_m = dplyr::filter(rm8h_d, cov.month > cov_threshold) |> 
          dplyr::group_by(Air.Quality.Station.EoI.Code, year, month, cov.month) |> 
          dplyr::summarise(v = quantile(v, perc, na.rm = T, names=F), .groups = "drop") |> 
          dplyr::rename(!!p := v)
        
        rm8h_y = dplyr::filter(rm8h_d, cov.year > cov_threshold) |> 
          dplyr::group_by(Air.Quality.Station.EoI.Code, year, cov.year) |> 
          dplyr::summarise(v = quantile(v, perc, na.rm = T, names=F), .groups = "drop") |> 
          dplyr::rename(!!p := v)
        
        rm8h_d = dplyr::filter(rm8h_d, cov.day > cov_threshold) |> 
          dplyr::rename(!!p := v) 
        
        write_pq_if(rm8h_d, outpaths[1], overwrite = isTRUE(overwrite)) 
        write_pq_if(rm8h_m, outpaths[2], overwrite = T)
        write_pq_if(rm8h_y, outpaths[3], overwrite = T) 
      }
    }
  })
  return(list(Country = cntry, elapsed = as.numeric(round(t[3],1))))
}


## Climate data conversion  ====================================================

# relative humidity
rh = function(t, d){
  (exp((17.625*d)/(243.04+t))/(exp((17.625*t)/(243.04+t)))) * 100
}

# wind direction
wdir <-function(u, v, units = F, round = F){
  wd = (270-atan2(u,v)*180/pi)%%360 
  if (round) wd = round(wd)
  if (units) units(wd) = "°"
  return(wd)
}

# wind speed
wspeed <-function(u, v,  units = F){
  ws = sqrt(u^2 + v^2)
  if (units) units(ws) = "m/s"
  return(ws)
}

## Sentinel 5P =================================================================

stac_download_S5P = function(day_start, n_days = 10, product = "L2__NO2___", 
                             outdir, overwrite = T){
  query = rstac::stac_search(
    q = stac_source,
    collections = "sentinel-5p-l2-netcdf",           # MS PC
    bbox = bbox,
    datetime = paste0(as.character(day_start), "T00:00:00Z/",
                      as.character(day_start + n_days), "T00:00:00Z"),
    # datetime = paste0(as.character(day_start),"/", 
    #                   as.character(day_start + n_days)),
    limit = 1000) |> 
    get_request() |> 
    items_filter(properties$`s5p:product_type` == product) |> 
    items_sign(sign_planetary_computer())
  
  message("\nDownloading ", length(query$features), " tiles...")
  rstac::assets_download(query, output_dir = outdir, overwrite = overwrite)
  
}

stac_download_S5P_single = function(day, source, product = "L2__NO2___", 
                             outdir, overwrite = T){
  query = rstac::stac_search(
    q = source,
    collections = "sentinel-5p-l2-netcdf",           # MS PC
    bbox = bbox,
    datetime = paste0(as.character(day), "T00:00:00Z/",
                      as.character(day), "T23:59:59Z"),
    limit = 1000) |> 
    rstac::get_request() |> 
    rstac::items_filter(properties$`s5p:product_type` == product) |> 
    rstac::items_sign(rstac::sign_planetary_computer())
  
  query_filtered = rstac::items_filter(query, properties$`s5p:processing_mode` == "RPRO")
  if (length(query_filtered$features) == 0){
    query_filtered = rstac::items_filter(query, properties$`s5p:processing_mode` == "OFFL")
  } else if (length(query_filtered$features) == 0){
    query_filtered = rstac::items_filter(query, properties$`s5p:processing_mode` == "NRTI")
  }
  
  #message("\nDownloading ", length(query$features), " tiles...")
  rstac::assets_download(query, output_dir = outdir, overwrite = overwrite, progress=F)
}

update_S5P_dates = function(x,y){
  return(setdiff(x, y) |> as.Date())
}

warp_S5P = function(x, target, overwrite = F, delete_src = F){
  path_out = sub(".nc", "_warp.nc", x)
  if (!file.exists(path_out) | overwrite){
    subs = sf::gdal_subdatasets(x)
    d = stars::read_stars(c(subs[[5]], subs[[6]])) |> 
      units::drop_units() |> 
      setNames(c("qa","no2")) |> 
      dplyr::transmute(no2 = ifelse(qa < 0.5, NA, no2)) |> 
      stars::st_warp(dest = target)
    stars::write_mdim(d, path_out, layer = "no2")
  }
  if (delete_src) unlink(x)
  return(path_out)
}

s5p_date = function(x){
  bn = basename(x)
  re = regexpr("\\d{8}", bn)
  rm = regmatches(bn, re) |> 
    as.Date(format="%Y%m%d")
  if (length(rm) == 0){
    re = regexpr("\\d{4}-\\d{2}-\\d{2}", bn)
    rm = regmatches(bn, re) |> 
      as.Date(format="%Y-%m-%d")
  }
  return(rm)
}

s5p_meta = function(x, size = T){
  d = purrr::map_vec(x, s5p_date)
  df = data.frame(date = d,
             year = as.numeric(format(d, "%Y")),
             month = as.numeric(format(d, "%m")),
             product = substr(basename(unlist(x)), 1, 19),
             file = unlist(x))
  if (size) df$size = file.info(df$file)$size/1e06 
  return(df)
}

s5p_daily_mosaic = function(d, wdf, outdir, delete_src = F){
  day_files = dplyr::filter(wdf, date == d) 
  out = file.path(outdir, paste0(day_files$product[1], d, ".tif"))
  if (!file.exists(out)){
    mos = stars::st_mosaic(day_files$file) |> 
      stars::read_stars(proxy = T) |> 
      setNames("no2")
    stars::write_stars(mos, out, 
                       options=c("co" = "COMPRESS=DEFLATE", 
                                 "co" = "PREDICTOR=3",
                                 "NUM_THREADS=ALL_CPUS"))
  }
  if (delete_src){
    unlink(day_files$file, recursive = T)
    file.remove(dirname(day_files$file))
  }
  return(out)
}

## Interpolation ===============================================================

load_aq = function(poll, stat, y, m=0, d=0, sf = T, verbose = T){
  if (!any(m==0, d==0)) stop("Supply only either m (month, 1-12) or d (day of year; 1-365).")
  if (!poll %in% c("PM10","PM2.5","O3","NO2")) stop("Select one of PM10, PM2.5, O3, NO2.")
  if (!stat %in% c("perc", "mean")) stop("Select one of mean or perc.")
  dir = file.path("AQ_data", ifelse(m == 0 & d == 0, "06_annual", ifelse(d == 0, "05_monthly", "04_daily")))
  
  info = paste("Loading", substr(basename(dir), 4, nchar(basename(dir))), poll, "data. Year =", y)
  
  if (d > 0){
    dir = dir(dir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    info = paste(info, "; Day of Year =", d)
    x = arrow::open_dataset(dir) |> dplyr::filter(year == y, doy == d) |> dplyr::collect()
  } else if (m > 0){
    dir = dir(dir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(dir)==1)
    info = paste(info, "; Month =", m)
    x = arrow::open_dataset(dir) |> dplyr::filter(year == y, month == m)
  } else {
    dir = dir(dir, pattern = glob2rx(paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(dir)==1)
    x = arrow::open_dataset(dir) |> dplyr::filter(year == y)
  }
  x = dplyr::collect(x) #|> add_meta()
  if (sf) x = sf::st_as_sf(x, coords=c("Longitude", "Latitude"), crs = sf::st_crs(4326), remove = F) |> 
    sf::st_transform(sf::st_crs(3035))
  attr(x, "stat") = stat
  attr(x, "y") = y
  attr(x, "m") = m
  attr(x, "d") = d
  if (verbose) message(info)
  return(x)
}

get_filename = function(dir, var, y, perc = NULL,
                        base.dir = "supplementary"){
  list.files(file.path(base.dir, dir), 
             pattern = glob2rx(paste0(tolower(var), "*",perc, "*", y, ".nc")), 
             full.names = T)
}

read_n_warp = function(x, lyr, name, target, target_dims = NULL, 
                       parallel=1, method="bilinear"){
  x = terra::rast(x, lyrs = lyr)
  terra::time(x) = NULL
  if (class(target)=="character") target = terra::rast(target)
  if (is.null(target_dims)) target_dims = stars::st_as_stars(target) |> stars::st_dimensions()
  x = terra::project(x, target, threads=parallel) |> 
    terra::resample(target, method = method, threads=parallel) |> 
    stars::st_as_stars() |> 
    setNames(name)
  stars::st_dimensions(x) = target_dims
  return(x)
}

load_covariates_EEA = function(x, spatial_ext = NULL, ...){
  pn = c("PM2.5", "PM10", "NO2", "O3")
  poll = pn[which(pn %in% names(x))]
  stat = attr(x, "stat")
  y = attr(x, "y")
  m = attr(x, "m")
  d = attr(x, "d")
  dir = ifelse(d > 0, "01_daily", ifelse(m > 0, "02_monthly", "03_annual"))
  lyr = ifelse(d > 0, d, ifelse(m > 0, m, y))
  dtime = paste("year =", y, ifelse(m > 0, paste("month =", m), paste("doy =", d)))
  
  # static
  clc = terra::rast("supplementary/static/CLC_reclass_8_1km.tif")
  dtm = terra::rast("supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif")
  
  if (!is.null(spatial_ext)){
    clc = terra::crop(clc, spatial_ext)
    dtm = terra::crop(dtm, spatial_ext)
  }
    
  dtm = stars::st_as_stars(dtm) |> setNames("Elevation")
  clc_st = stars::st_as_stars(clc) |> setNames("CLC") |> 
    dplyr::mutate(
      CLC = factor(CLC, levels = 1:8, 
                   labels = c("HDR", "LDR", "IND","TRAF",
                              "UGR","AGR","NAT","OTH")))
  dims = stars::st_dimensions(clc_st)
  
  # measurement-level
  cams_name = paste0("CAMS_", poll)
  cams = read_n_warp(get_filename(dir, poll, y, perc = ifelse(stat == "perc", "perc", NULL)), lyr,
                     name = cams_name, target = clc, target_dims = dims, ...)
  ws = read_n_warp(get_filename(dir, "wind_speed", y), lyr,
                   name = "WindSpeed", target = clc, target_dims = dims, ...)

  l = switch(
    poll, 
    "PM10" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, clc_st, dtm,
                  read_n_warp(get_filename(dir, "rel_humidity", y), lyr,
                              name = "RelHumidity", target = clc, target_dims = dims, ...)), 
    "PM2.5" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, clc_st, dtm),
    "O3" = list(cams, ws, clc_st, dtm,
                read_n_warp(get_filename(dir, "solar_radiation", y), lyr,
                            name = "SolarRadiation", target = clc, target_dims = dims, ...)))
  # "NO2 = ...
  
  strs = do.call(c, l)
  attr(strs, "pollutant") <- poll
  attr(strs, "stat") <- stat
  attr(strs, "dtime") <- dtime
  attr(strs, "y") <- y
  attr(strs, "m") <- m
  attr(strs, "d") <- d
  return(strs)
}

filter_area_type = function(aq, area_type = "RB"){
  if (!area_type %in% c("RB", "UB", "UT")) stop("area_type should be one of c(RB, UB, UT)!")
  
  aq = switch(area_type,
         "RB" = dplyr::filter(aq, StationType == "background", StationArea == "rural"), 
         "UB" = dplyr::filter(aq, StationType == "background", StationArea %in% c("urban", "suburban")), 
         "UT" = dplyr::filter(aq, StationType == "traffic", StationArea %in% c("urban", "suburban"))) |>
    na.omit() |> 
    dplyr::select(-c(Countrycode, StationType, StationArea))
  attr(aq, "area_type") <- area_type
  return(aq)
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
  #if (! identical(sf::st_crs(aq), sf::st_crs(covariates))) stop("CRSs of aq and covariates do not match!")
  
  poll = find_poll(aq)

  aq_ext = stars::st_extract(covariates, aq) 
  aq_join = sf::st_join(dplyr::select(aq, 1:{{poll}}), aq_ext) |> 
    #tidyr::drop_na() |> 
    unique()
  
  target = ifelse(poll %in% c("PM2.5", "PM10"), paste0("log(", poll, ")"), poll) # log transform for PM
  
  frm = as.formula(paste(target, "~", paste(names(aq_cov), collapse = "+")))
  l = lm(frm, aq_join)
  attr(l, "formula") = frm
  attr(l, "log_transformed") = ifelse(poll %in% c("PM2.5", "PM10"), T, F)       # log transform for PM
  return(l)
}

lm_measures = function(l){
  rmse = caret::RMSE(l$model[,1] |> as.vector(),l$fitted.values) |> round(3)
  r2 = caret::R2(l$model[,1] |> as.vector(),l$fitted.values) |> round(3)
  return(list(RMSE = rmse, R2 = r2))
}

mask_missing_CLC = function(covariates, lm){
  lm_clc_levels = levels(linmod$model$CLC)
  cov_clc_levels = levels(covariates$CLC)
  
  if (length(lm_clc_levels) < length(cov_clc_levels)){
    missing_clc_levels = cov_clc_levels[!cov_clc_levels %in% lm_clc_levels]
    covariates$CLC[covariates$CLC %in% missing_clc_levels] = NA
    covariates$CLC = factor(covariates$CLC)
    message("AQ data has no observations with CLC class label(s) ", 
            paste(missing_clc_levels, collapse = ", "), 
            ".\nCorresponding pixels are masked in the covariate data.")
  }
  return(covariates)
}

merge_stars_list = function(lst){
  do.call(rbind, lapply(lst, as.data.frame)) |> 
    stars::st_as_stars()
}

krige_aq_residuals = function(aq, covariates, lm, n.max = Inf, cluster = NULL, 
                              show.vario = F, verbose = F){
  #max.cores = parallel::detectCores()
  #if(n.cores > max.cores) stop("Maximimum number of cores is ", max.cores)
  
  area_type =  attr(aq, "area_type")
  if (!is.null(attr(lm$model, "na.action"))) aq = aq[-attr(lm$model, "na.action"),]
  aq = cbind(aq, lm$model[,2:ncol(lm$model)])
  aq$res = lm$residuals
  
  # variogram
  vario = automap::autofitVariogram(res~1, aq)
  if (show.vario){
    print(vario)
    print(plot(vario))
  } 
  vr_m = vario$var_model
  
  n.cores = length(cluster)
  if (is.null(cluster)){ 
    k = gstat::krige(res~1, aq, covariates, vr_m)
  } else {
    if (verbose) message("Kriging residuals in parallel using ", n.cores, " cores.")
    
    # set up cluster and data
    e = new.env()
    e$aq = aq; e$vr_m = vr_m; e$n.max = n.max
    parallel::clusterExport(cluster, list("aq", "vr_m", "n.max"), envir = e)
    
    # split prediction locations:
    if (verbose) message("Splitting new data.")
    n_rows = dim(covariates)[2]
    splt = rep(1:n.cores, each = ceiling(n_rows/n.cores), length.out = n_rows)
    newdlst = lapply(as.list(1:n.cores), function(w) covariates[,,which(splt == w)])
    
    # run
    if (verbose) message("Kriging interpolation.")
    tictoc::tic()
    krige_lst = parallel::parLapply(cluster, newdlst, function(lst) gstat::krige(res~1, aq, lst, vr_m, nmax = n.max))

    t = tictoc::toc(quiet = T)
    if (verbose) message("Completed.  ", t$callback_msg)
    gc(verbose = F)
    k = merge_stars_list(krige_lst)
  }
    attr(k, "area_type") <- area_type
    return(k)
}

combine_results = function(covariates, kriging, trim_range = NULL){
  # check extents/nrow/ncol
  stopifnot("Linear model prediction not found in covariates object. Make sure to first predict the linear model using 'aq_cov$lm_pred = predict(linmod, newdata = aq_cov)' befor combining with kriging results.)" = "lm_pred" %in% names(covariates))
  res = covariates["lm_pred"]
  
  if (!is.null(trim_range)){
    res$lm_pred = ifelse(res$lm_pred < trim_range[1], trim_range[1], res$lm_pred)
    res$lm_pred = ifelse(res$lm_pred > trim_range[2], trim_range[2], res$lm_pred)
  }
  res$residuals = kriging$var1.pred
  res$aq_interpolated = res$lm_pred + res$residuals
  aq_attrs = attributes(covariates)
  attributes(res)[c("pollutant", "stat", "dtime")] = aq_attrs[c("pollutant", "stat", "dtime")]
  attr(res, "area_type") <- attr(kriging, "area_type")
  return(res)
}

plot_aq_interpolation = function(x, pollutant = NULL, stat=NULL, dtime=NULL, 
                                 area_type=NULL, layer = "aq_interpolated", 
                                 countries = T, ...){
  if (is.null(pollutant)) pollutant = attr(x, "pollutant")
  if (is.null(stat)) stat = attr(x, "stat")
  if (is.null(dtime)) dtime = attr(x, "dtime")
  if (is.null(area_type)) area_type = attr(x, "area_type")
  
  if (stat == "perc"){
    breaks = switch(pollutant,
                    "PM10" = c(0, 20, 30, 40, 50, 75, 100),
                    "O3" = c(0, 90, 100, 110, 120, 140, 200))
  } else {
    breaks = switch(pollutant,
                    "PM10" = c(0, 15, 20, 30, 40, 50, 100),
                    "PM2.5" = c(0, 5, 10, 15, 20, 25, 40),
                    "NO2" = c(0, 10, 20, 30, 40, 45, 100))
  }
  
  max_val = x |> dplyr::pull() |> max(na.rm = T)
  if (max_val > max(breaks)) breaks[length(breaks)] = max_val
  
  cols = c("darkgreen", "forestgreen", "yellow2", "orange", "red", "darkred")
  titl = paste(pollutant, stat, dtime, area_type)
  plot(x[layer], col=cols, breaks = breaks, 
       main = titl, key.pos=1, reset=!countries, ...)
  
  if (countries) plot(giscoR::gisco_countries$geometry |> 
                        sf::st_transform(sf::st_crs(x)), add=T)
}

apply_aq_weights = function(x, ...){
  m = all(is.na(x[1]), is.na(x[2]), is.na(x[3]))  # create mask only from predictions
  c_rb = prod(1 - x[5], x[1], ...)                # weighted RB
  c_ub = prod(x[5], 1 - x[4], x[2], ...)          # weighted UB
  c_ut = prod(x[3], x[4], ...)                    # weighted UT
  r = sum(c_rb, c_ub, c_ut, ...)                  # sum (excluding NAs if na.rm = T)
  r[m==1] = NA                                    # mask
  return(r)
}

merge_aq_maps = function(paths, weights, cluster = NULL){
  # aq_interpolated = paths |> sort() |> stars::read_stars() |> 
  #   setNames(c("RB","UB","UT")[1:length(paths)])
  aq_interpolated = paths |> sort() |> stars::read_stars(along="bands") |> 
    stars::st_set_dimensions("bands", c("RB","UB","UT")[1:length(paths)])
  
  w = weights |> sf::st_crop(sf::st_bbox(aq_interpolated)) |> 
    sf::st_normalize()
  components = c(aq_interpolated, w, along="bands")
  
  if (all(grepl("O3", paths))){
    # aq_merge = dplyr::transmute(
    #   components, p = ((1 - w_urban)*RB) + (w_urban * UB))   # ozone: no urban traffic component
    message("O3 is not yet supported.")
    stop()
    
  } else {
    aq_merge = st_apply(components, 1:2, apply_aq_weights, na.rm=T, 
                        CLUSTER = cluster, PROGRESS = T) 
  }
  return(aq_merge)
}
