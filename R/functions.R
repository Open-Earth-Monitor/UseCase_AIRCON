## Downloads   ==================================================================

generate_request = function(country, pollutant, year_start, year_end = year_start, source = "All"){
  purrr::map(country, function(c) purrr::map(pollutant, function(p) 
    paste0("https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?CountryCode=",
           c, "&CityName=&Pollutant=", p, "&Year_from=", year_start, "&Year_to=", 
           year_end, "&Station=&Samplingpoint=&Source=", source, 
           "&Output=TEXT&UpdateDate=", year_start, "-01-01&TimeCoverage=Year")
  )) |> purrr::flatten() |> unlist()
}

generate_download_urls = function(country, pollutant, year_start, 
                                  year_end = year_start, source = "All", 
                                  file = "download_urls.txt", append = F){
  requests = generate_request(country, pollutant, year_start, year_end, source)
  if (!append) file.create(file)
  purrr::map(requests, .f = function(x) curl::curl(x) |> 
               readLines() |> write(file, append = T)) |> 
    invisible()
  message(paste("Generated", readLines(file) |> length(), "station URLs from",
                length(country), "countries and", length(pollutant), 
                "pollutants between", year_start, "and", year_end))
  message("URLs are written to file: ", file)
}

url_meta_data = function(source = "download_urls.txt"){
  if (length(source) == 1){
    u = readLines(source) |> basename()
  } else {
    u = basename(source)
  }
  data.frame(Country = as.factor(substr(u, 1,2)), 
             Pollutant = factor(readr::parse_number(u), 
                                levels = c(6001, 5, 8, 7),
                                labels = c("PM2.5", "PM10", "NO2", "O3")),
             Year = as.numeric(stringr::str_extract(u, "201[5-9]|202[0-3]")),
             Station = stringr::str_extract(u, "[0-9]{5}"),
             File = u) |> 
    na.omit()
}

check_station_urls = function(file = "download_urls.txt", plot = T){
  df = url_meta_data(file)
  smry = dplyr::summarise(df, .by = names(df)[1:3], Count = dplyr::n())
  if (plot) ggplot2::ggplot(smry, ggplot2::aes(Year, Count, fill = Pollutant)) + 
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::facet_wrap(~Country, scales = "free") +
    ggplot2::ggtitle("Number of Stations per Country & Year")
}

download_station_data = function(url_file = "download_urls.txt", dir = NULL){
  stopifnot(file.exists(url_file))
  if (is.null(dir)) dir = file.path(getwd(), "download")
  if (!dir.exists(dir)) dir.create(dir)
  
  urls = readLines(url_file)
  enc_file = "download_files_wrong_encoding.txt"
  enc_list = readLines(enc_file)
  
  fails = character(0)
  t = system.time({
    paths = purrr::map(urls, function(u){
      dest = file.path(dir, basename(u))
      if (!file.exists(dest) && !dest %in% enc_list){
        tryCatch(curl::curl_download(u, dest),
                 error = function(e) {fails <<- c(fails, u)}
        )
      }}, 
      .progress = list(type = "iterator", name = "Downloading", clear = F)) |> unlist()
  })
  if (t[3] > 5) message("Download took ", t[3] |> unname(), " sec.")
  if (length(fails) > 0){
    message("Failed downloads:")
    message(fails)
  }
  url_meta_data(urls)
}

## Pre-processing  =============================================================
get_encoding = function(file){
  e = readr::guess_encoding(file)
  enc = e$encoding[which.max(e$confidence)]
  return(c(file = file, encoding = enc))
}

read_pollutant = function(files, pollutant = "NO2", countries = NULL, dir = "download"){
  cols = c("AirQualityStationEoICode", "AirPollutant", 
           "Concentration", "DatetimeBegin", "Validity", "Verification")
  files_p = dplyr::filter(files, Pollutant == pollutant, Country %in% countries)
  if (nrow(files_p) > 0){
    data = vroom::vroom(file.path(dir, files_p$File), 
                        col_select = dplyr::all_of(cols),
                        show_col_types = F)
    dplyr::mutate(data, 
                  DatetimeBegin = fasttime::fastPOSIXct(DatetimeBegin), 
                  AirQualityStationEoICode = as.factor(AirQualityStationEoICode)) |> 
      dplyr::filter(!is.na(Concentration))
  } else message("No files for ", pollutant, " in ", countries)
}

read_pollutant_dt = function(files, pollutant = "NO2", countries = NULL, dir = "download"){
  cols = c("AirQualityStationEoICode", "AirPollutant", 
           "Concentration", "DatetimeBegin", "Validity", "Verification")
  enc_list = readLines("download_files_wrong_encoding.txt") |> basename()
  files_p = dplyr::filter(files, Pollutant == pollutant, 
                          Country %in% countries,
                          ! File %in% enc_list,
                          file.exists(file.path("download", File)))
  if (nrow(files_p) > 0){
    data = lapply(file.path(dir, files_p$File), data.table::fread, sep = ",", select=cols)
    data = data.table::rbindlist(data) |> dtplyr::lazy_dt()
    data = dplyr::mutate(data, 
                         DatetimeBegin = fasttime::fastPOSIXct(DatetimeBegin), 
                         AirQualityStationEoICode = as.factor(AirQualityStationEoICode)) |> 
      dplyr::filter(!is.na(Concentration))
  } else {
    data = NULL
    message(countries, "- no files for ", pollutant)
  }
  return(data)
}

filter_quality = function(x, keep_validity = 1, keep_verification = c(1,2)){
  if (!is.null(x)){
    dplyr::filter(x, 
                  Validity %in% keep_validity, 
                  Verification %in% keep_verification) |> 
      dplyr::select(-Validity, -Verification)
  }
}

filter_stations = function(x, 
                           type = c("background","traffic"), 
                           area =  c("rural", "urban", "suburban")){
  dplyr::filter(x, StationType %in% type & StationArea %in% area)
}

pivot_poll = function(x){
  tidyr::pivot_wider(x, 
                     names_from = AirPollutant, 
                     values_from = Concentration, 
                     values_fn = max)
}

add_meta = function(pollutant){
  if (!any(c("Countrycode","StationType","StationArea",
             "Longitude","Latitude") %in% names(pollutant))){
    meta_data = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")
    by = "AirQualityStationEoICode"
    pollutant = dplyr::left_join(pollutant, meta_data, by = by)
  }
  return(pollutant)
}

join_pollutants = function(pollutants, country = NULL){
  cat("\n", country, ": joining -", names(pollutants)[1])
  a = pollutants[[1]] |> pivot_poll()
  
  if (length(pollutants) > 1){
    by = c("AirQualityStationEoICode", "DatetimeBegin")
    for (i in 2:length(pollutants)){
      cat(" -", names(pollutants)[i])
      b = pollutants[[i]] |> pivot_poll()
      a = dplyr::full_join(a, b, by = by)
    }
  }
  a = add_meta(a) |> filter_stations()
  a = dplyr::select(a, AirQualityStationEoICode, 
                    StationArea, StationType, Longitude, Latitude,
                    #Elevation, Population, CLC8,
                    DatetimeBegin, dplyr::everything()) 
  return(a)
}  

preprocess_AQ_by_country = function(country, dl_files = files, outdir = "."){
  out = file.path(outdir, paste0(country, "_hourly_2015-2023_gaps.parquet"))
  if (!file.exists(out)){
    
    pollutants = list(
      no2  = read_pollutant_dt(dl_files, pollutant = "NO2", countries = country) |> filter_quality(),
      o3   = read_pollutant_dt(dl_files, pollutant = "O3", countries = country) |> filter_quality(),
      pm25 = read_pollutant_dt(dl_files, pollutant = "PM2.5", countries = country) |> filter_quality(),
      pm10 = read_pollutant_dt(dl_files, pollutant = "PM10", countries = country) |> filter_quality()
    ) |> purrr::discard(is.null)
    poll_table = join_pollutants(pollutants, country)
    
    if ("dtplyr_step" %in% class(poll_table)) poll_table = data.table::as.data.table(poll_table)
    
    counts = table(poll_table$AirQualityStationEoICode |> droplevels()) |> as.data.frame() |> 
      setNames(c("AirQualityStationEoICode", "count"))
    
    arrow::write_parquet(poll_table, out)
    gc()
    return(counts)
  } else  message_parallel(paste(country, "completed."))
}



## Gapfilling  =================================================================

# make_ecwmf_box = function(bounds, buffer, view=T){
#   xmn = bounds[1]-buffer; xmx = bounds[2]+buffer; ymn = bounds[3]-buffer; ymx = bounds[4]+buffer
#   box = c(ymx, xmn, ymn, xmx)
#   if (view){
#     bbox = sf::st_sfc(list(sf::st_point(c(xmn,ymn)), sf::st_point(c(xmx,ymx)) )) |> 
#       sf::st_bbox() |> sf::st_as_sfc() |> sf::st_as_sf() |> sf::st_set_crs(4326)
#     print(mapview::mapview(bbox, color="red", alpha.regions=0))
#   }
#   return(box)
# }

add_ymd = function(x){
  dplyr::mutate(x, year = lubridate::year(DatetimeBegin),
                month = lubridate::month(DatetimeBegin),
                doy = lubridate::yday(DatetimeBegin))
}

make_time_grid = function(x, step = 3600){
  stations = unique(x$AirQualityStationEoICode) |> droplevels()
  
  time_min = min(x$DatetimeBegin) |> as.POSIXct() + step
  time_max = max(x$DatetimeBegin) |> as.POSIXct() + step
  time_steps = seq(time_min, time_max, by = step) 
  expand_grid(AirQualityStationEoICode = stations, DatetimeBegin = time_steps)
}

extract_ts = function(at, x, varname = "ext_var", t_col = "DatetimeBegin"){
  ex = data.frame(at$AirQualityStationEoICode, 
                  at$DatetimeBegin,
                  stars::st_extract(x, at, time_column = t_col) |> 
                    dplyr::pull(varname) |> 
                    units::drop_units()
  ) |> 
    setNames(c("AirQualityStationEoICode", "DatetimeBegin", varname))
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
    data = dplyr::left_join(data, population_table, by = join_by(AirQualityStationEoICode))
    
    pred = predict(model, data)
    ind = !is.na(data$PM10) & is.na(data$PM2.5)
    
    data[ind,"PM2.5"] = pred[ind]
    data$pseudo = as.integer(0)
    data$pseudo[ind] = 1
    
    arrow::write_parquet(data, out_file)
  }
  cat(paste0(country, " - "))
  return(out_file)
}

# Temporal aggregation =========================================================
#'
#'@param p Pollutant (O3, NO2, PM10, PM2.5)
#'@param step one of 'hour', 'day', or 'month'
#'@param by one of 'day', 'month', or 'year'
check_temp_cov = function(x, p, step, by = "year", collect = F){
  stopifnot("step should be one of 'hour', 'day', or 'month'." = step %in% c("hour","month","day"))
  stopifnot("by should be one of 'day', 'month', or 'year'." = by %in% c("year","month","day"))
  s = switch(step,
             "hour" = switch(by, year = 365*24, "month" = 30*24, "day" = 24),
             "day" = switch(by, year = 365, "month" = 30),
             "month" = 12)
  temp_vars_group = switch(by,
                           "year" = dplyr::vars(as.symbol("year")),
                           "month" = dplyr::vars(as.symbol("year"), as.symbol("month")),
                           "day" = dplyr::vars(as.symbol("year"), as.symbol("month"), as.symbol("doy")))
  vrs_group = c(dplyr::vars(AirQualityStationEoICode), temp_vars_group)
  
  suppressWarnings({
  x = dplyr::select(x, dplyr::everything(), v = p) |> 
    dplyr::filter(!is.na(v)) |> 
    dplyr::mutate(one = 1, AirQualityStationEoICode = as.character(AirQualityStationEoICode)) |> 
    dplyr::group_by_at(vrs_group, .add = T) |> 
    dplyr::summarise(cov = sum(one)/s, AirPollutant = p, .groups = "drop") 
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
    dplyr::mutate(AirQualityStationEoICode = as.character(AirQualityStationEoICode)) |>
    dplyr::inner_join(tc, by = dplyr::join_by(AirQualityStationEoICode, year)) 
}

find_poll = function(x){
  n = names(x)
  poll = NULL
  for (p in c("PM10","PM2.5","O3","NO2")){
    if (p %in% n) poll = p
  }
  return(poll)
}

# temp_agg_mean = function(x, step, collect = F){
#   stopifnot("step should be one of 'year','month', or 'day')" = step %in% c("year","month","day"))
#   temp_vars_sel = switch(step,
#                          "year" = c("year"),
#                          "month" = c("year","month"),
#                          "day" = c("year", "month", "doy"))
#   p = find_poll(x)
#   vrs_sel = c(temp_vars_sel, "v" = p)
#   
#   temp_vars_group = switch(step,
#                            "year" = dplyr::vars(as.symbol("year")),
#                            "month" = dplyr::vars(as.symbol("year"), as.symbol("month")),
#                            "day" = dplyr::vars(as.symbol("year"), as.symbol("month"), as.symbol("doy")))
#   vrs_group = c(dplyr::vars(AirQualityStationEoICode), temp_vars_group)
#   
#   x = dplyr::select(x, AirQualityStationEoICode, vrs_sel) |> 
#     dplyr::group_by_at(vrs_group, .add = T) |> 
#     dplyr::summarise(v = mean(v, na.rm = T), .groups = "drop") |> 
#     dplyr::rename(!!p := v) |> add_meta()
#   if (collect) x = dplyr::collect(x)
#   return(x)
# }

temp_agg_daily_mean = function(x, cov_threshold){
  p = find_poll(x)
  
  # temporal coverage
  suppressMessages({
    temp_cov_y = check_temp_cov(x, p, step = "hour", by = "year", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:2) |> dplyr::mutate(y_cov = 1)
    temp_cov_m = check_temp_cov(x, p, step = "hour", by = "month", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:3) |> dplyr::mutate(m_cov = 1)
    temp_cov_d = check_temp_cov(x, p, step = "hour", by = "day", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:4) |> dplyr::mutate(d_cov = 1)
    temp_cov = dplyr::full_join(temp_cov_y, temp_cov_m) |> dplyr::full_join(temp_cov_d)
  })
  
  # group by station and time
  # aggregate daily and join with temporal coverage info
  vrs_group = dplyr::vars(AirQualityStationEoICode, year, month, doy)
  suppressWarnings({
    agg_d = dplyr::select(x, dplyr::everything(), "v" = p) |> 
      dplyr::group_by_at(vrs_group, .add = T) |> 
      dplyr::summarise(v = mean(v, na.rm = T), .groups = "drop") |> 
      dplyr::rename(!!p := v) |> #add_meta() |> 
      tidyr::drop_na() |> 
      dplyr::left_join(temp_cov, by = dplyr::join_by(AirQualityStationEoICode, year, month, doy))
  })
  return(agg_d)
}


process_temp_agg = function(path, p, perc = NULL, cov_threshold = 0.75, overwrite = F){
  stopifnot('Pollutant (p) should be one of  PM10, PM2.5, O3, or NO2.' = p %in% c("PM10","PM2.5","O3","NO2"))
  tictoc::tic()
  cntry = substr(basename(path), 1,2)
  metric = ifelse(is.null(perc), "mean", "perc")
  file_d = paste0("AQ_data/04_daily/", p, "_", metric, "/", cntry, "_", p, "_daily_", metric, ".parquet")
  
  #  check if daily mean has been processed already
  if (file.exists(file_d) & !overwrite){
    agg_d = arrow::read_parquet(file_d)
  } else {
    # daily aggregate
    x = arrow::read_parquet(path, col_select = c(1:6, p)) |> add_ymd()
    agg_d = temp_agg_daily_mean(x, cov_threshold = cov_threshold) |>  
      dplyr::select(dplyr::everything(), "v" = p)
  }
  # monthly / annual mean / percentile of daily aggregates
  if (nrow(agg_d) > 0){
    if (is.null(perc)){
      agg_m = dplyr::group_by(agg_d, AirQualityStationEoICode, year, month) |> 
        dplyr::summarise(v = mean(v, na.rm = T), 
                         m_cov = sum(d_cov, na.rm=T)/dplyr::n(), 
                         .groups = "drop")
      
      agg_y = dplyr::group_by(agg_d, AirQualityStationEoICode, year) |> 
        dplyr::summarise(v = mean(v, na.rm = T), 
                         y_cov = sum(d_cov, na.rm=T)/dplyr::n(), 
                         .groups = "drop")
    } else {
      agg_m = dplyr::group_by(agg_d, AirQualityStationEoICode, year, month) |> 
        dplyr::summarise(v = quantile(v, perc, na.rm = T),
                         m_cov = sum(d_cov, na.rm=T)/dplyr::n(), 
                         .groups = "drop")
      
      agg_y = dplyr::group_by(agg_d, AirQualityStationEoICode, year) |> 
        dplyr::summarise(v = quantile(v, perc, na.rm = T), 
                         y_cov = sum(d_cov, na.rm=T)/dplyr::n(),
                         .groups = "drop")
    }
    # rename v to actual variable name and add station meta data
    agg_d = dplyr::rename(agg_d, !!p := v) |> add_meta()
    agg_m = dplyr::rename(agg_m, !!p := v) |> add_meta()
    agg_y = dplyr::rename(agg_y, !!p := v) |> add_meta()
    
    pth = paste0("AQ_data/", c("04_daily","05_monthly", "06_annual"), "/", p, "_", metric, "/",
                 cntry, "_", p, "_", c("daily", "monthly", "annual"), "_", metric, ".parquet")
    write_pq_if(agg_d, pth[1], ow = overwrite)
    write_pq_if(agg_m, pth[2], ow = overwrite)
    write_pq_if(agg_y, pth[3], ow = overwrite)
  } 
  end = tictoc::toc()
  return(list(Country = cntry, elapsed = as.numeric(round(end$toc - end$tic))))
}

# show_timings = function(t, n.rows = 3){
#   tt = do.call(rbind, t) |> as.data.frame()
#   tindex = seq(0, nrow(tt), length.out = n.rows+1) |> floor()
#   tlist = list()
#   for (i in 1:n.rows){
#     #print(append(tlist, tt[(tindex[i]+1):tindex[i+1],]))
#     tlist[[i]] = tt[(tindex[i]+1):tindex[i+1],] |> list()
#   }
#   knitr::kable(tlist, row.names = F)
# }

# 8h rolling mean --------------------------------------------------------------
# https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-50/appendix-Appendix%20I%20to%20Part%2050

# expand an hourly temporal grid from earliest to latest timestemp
station_time_bounds = function(x){
  dplyr::group_by(x, AirQualityStationEoICode) |>
    dplyr::summarise(time_min = min(DatetimeBegin),
                     time_max = max(DatetimeBegin))
}

station_time_grid = function(s, st_bounds){
  time_steps = seq(st_bounds$time_min[s], st_bounds$time_max[s], by = "hours") 
  expand.grid(AirQualityStationEoICode = st_bounds$AirQualityStationEoICode[s],
              DatetimeBegin = time_steps) |> 
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
  n_valid = data.table::frollapply(x, 8, FUN = function(x) sum(!is.na(x)), 
                                   align = "right")
  m8h = data.table::frollmean(x, 8, na.rm=T, hasNA = T, align = "right")
  m8h[n_valid < 6] = NA
  m8h
}

write_pq_if = function(x, path, check.dir = T, ow = F){
  if (check.dir & ! dir.exists(dirname(path))) dir.create(dirname(path), recursive = T)
  if (!file.exists(path) | ow) arrow::write_parquet(x, path)
}

temp_agg_daily_max8hm = function(i, bounds, x, cov_threshold){
  
  ts_grid = station_time_grid(i, bounds)
  by = dplyr::join_by(AirQualityStationEoICode, DatetimeBegin)
  ts_extended = dplyr::left_join(ts_grid, x, by = by, copy=T) |> 
    data.table::as.data.table()
  
  rm8h = dplyr::mutate(ts_extended, O3 = roll_8h_mean_dt(O3)) |> add_ymd() |> 
    dplyr::select(AirQualityStationEoICode, DatetimeBegin, year, month, doy, O3)
  suppressMessages({
    temp_cov_y = check_temp_cov(rm8h, p = "O3", step = "hour", by = "year", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:2) |> dplyr::mutate(y_cov = 1)
    temp_cov_m = check_temp_cov(rm8h, p = "O3", step = "hour", by = "month", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:3) |> dplyr::mutate(m_cov = 1)
    temp_cov_d = check_temp_cov(rm8h, p = "O3", step = "hour", by = "day", collect = T) |> 
      dplyr::filter(cov >= cov_threshold) |> dplyr::select(1:4) |> dplyr::mutate(d_cov = 1)
    temp_cov = dplyr::full_join(temp_cov_y, temp_cov_m) |> dplyr::full_join(temp_cov_d)
  })
  
  suppressWarnings({
    rm8h_d = dplyr::group_by(rm8h, AirQualityStationEoICode, year, month, doy) |> 
      dplyr::summarise(O3 = max(O3, na.rm = T), .groups = "drop") |> 
      dplyr::mutate(O3 = dplyr::na_if(O3, -Inf) ) |> 
      tidyr::drop_na() |> 
      dplyr::left_join(temp_cov, by = dplyr::join_by(AirQualityStationEoICode, year, month, doy))
  })
  return(rm8h_d)
}

process_O3_temp_agg_8hm = function(path, perc, cov_threshold = 0.75, overwrite = F){
  start = tictoc::tic()
  cntry = substr(basename(path), 1,2)
  outpaths = paste0("AQ_data/", 
                    c("04_daily","05_monthly", "06_annual"), "/",
                    c("O3_max8h_mean","O3_max8h_perc", "O3_max8h_perc"), "/", cntry, "_O3_", 
                    c("daily_max8h_mean", "monthly_max8h_perc", "annual_max8h_perc"), ".parquet")
  
  #  check if daily max 8h mean has been processed already
  if (file.exists(outpaths[1]) & !overwrite){
    rm8h_d = arrow::read_parquet(outpaths[1])
  } else {
    # get temporal min/max per station
    x = arrow::read_parquet(path)
    bounds = station_time_bounds(x)
    
    if (nrow(bounds) > 0){
      # daily max of 8h mean
      rm8h_d = purrr::map_dfr(1:nrow(bounds), temp_agg_daily_max8hm, bounds = bounds, 
                              x = x, cov_threshold = cov_threshold)
    } else {
      rm8h_d = data.frame()
    }
    if (nrow(rm8h_d) > 0){
      # monthly / annual percentile of daily max of 8h mean
      rm8h_m = dplyr::group_by(rm8h_d, AirQualityStationEoICode, year, month) |> 
        dplyr::summarise(O3 = quantile(O3, perc, na.rm = T),
                         m_cov = sum(d_cov, na.rm=T)/dplyr::n(), .groups = "drop")
      rm8h_y = dplyr::group_by(rm8h_d, AirQualityStationEoICode, year) |> 
        dplyr::summarise(O3 = quantile(O3, perc, na.rm = T),
                         y_cov = sum(d_cov, na.rm=T)/dplyr::n(), .groups = "drop")
      
      write_pq_if(rm8h_d |> add_meta(), outpaths[1], ow = overwrite) 
      write_pq_if(rm8h_m |> add_meta(), outpaths[2], ow = overwrite)
      write_pq_if(rm8h_y |> add_meta(), outpaths[3], ow = overwrite) 
    }
  } 
  end = tictoc::toc()
  return(list(Country = cntry, elapsed = as.numeric(round(end$toc - end$tic))))
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
  
  message("Downloading ", length(query$features), " tiles...")
  rstac::assets_download(query, output_dir = outdir, overwrite = overwrite)
  
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

s5p_meta = function(x){
  d = purrr::map_vec(x, s5p_date)
  data.frame(date = d,
             year = as.numeric(format(d, "%Y")),
             month = as.numeric(format(d, "%m")),
             product = substr(basename(unlist(x)), 1, 19),
             file = unlist(x))
}

s5p_daily_mosaic = function(d, wdf, outdir){
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
