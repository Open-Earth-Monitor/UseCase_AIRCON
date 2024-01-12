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
  data.frame(Country = substr(u, 1,2), 
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
    message("No files for ", pollutant, " in ", countries)
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

add_meta = function(pollutant, meta_data = station_meta){
  by = "AirQualityStationEoICode"
  dplyr::left_join(pollutant, meta_data, by = by)
}

join_pollutants = function(pollutants){
  cat("Joining: -", names(pollutants)[1])
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
                    Elevation, Population, CLC8,
                    DatetimeBegin, dplyr::everything()) 
  return(a)
}  

message_parallel = function(...){
  system(sprintf('echo "%s "', paste0(..., collapse="")))
}

preprocess_AQ_by_country = function(country, dl_files = files, outdir = "."){
  message_parallel(country)
  out = file.path(outdir, paste0(country, "_hourly_2015-2023_gaps.parquet"))
  if (!file.exists(out)){
    
    pollutants = list(
      no2  = read_pollutant_dt(dl_files, pollutant = "NO2", countries = country) |> filter_quality(),
      o3   = read_pollutant_dt(dl_files, pollutant = "O3", countries = country) |> filter_quality(),
      pm25 = read_pollutant_dt(dl_files, pollutant = "PM2.5", countries = country) |> filter_quality(),
      pm10 = read_pollutant_dt(dl_files, pollutant = "PM10", countries = country) |> filter_quality()
    ) |> purrr::discard(is.null)
    poll_table = join_pollutants(pollutants)
    
    if ("dtplyr_step" %in% class(poll_table)) poll_table = data.table::as.data.table(poll_table)
    
    counts = table(poll_table$AirQualityStationEoICode |> droplevels()) |> as.data.frame() |> 
      setNames(c("AirQualityStationEoICode", "count"))
    
    arrow::write_parquet(poll_table, out)
    gc()
    return(counts)
  } else  message_parallel(paste(country, "completed."))
}



## Gapfilling  =================================================================
make_ecwmf_box = function(bounds, buffer, view=T){
  xmn = bounds[1]-buffer; xmx = bounds[2]+buffer; ymn = bounds[3]-buffer; ymx = bounds[4]+buffer
  box = c(ymx, xmn, ymn, xmx)
  if (view){
    bbox = sf::st_sfc(list(sf::st_point(c(xmn,ymn)), sf::st_point(c(xmx,ymx)) )) |> 
      sf::st_bbox() |> sf::st_as_sfc() |> sf::st_as_sf() |> sf::st_set_crs(4326)
    print(mapview::mapview(bbox, color="red", alpha.regions=0))
  }
  return(box)
}

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

fill_pm2.5_gaps = function(country, model, 
                           gap_data = "AQ_data/02_hourly_SSR",
                           out_dir = "AQ_data/03_hourly_gapfilled"){
  
  out_file = file.path(out_dir, paste0(country, "_hourly_gapfilled.parquet"))
  if (!file.exists(out_file)){
    
    data = arrow::open_dataset(gap_data) |> 
      dplyr::filter(Countrycode == country) |> 
      dplyr::collect()
    
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

check_temp_cov = function(x, p, step, collect = F){
  stopifnot("step should be one of 'hour', 'day', or 'month'." = step %in% c("hour","month","day"))
  s = switch(step,
             "hour" = 365*24,
             "day" = 365,
             "month" = 12)
  x = dplyr::select(x, AirQualityStationEoICode, year, v = dplyr::all_of(p)) |> 
    dplyr::filter(!is.na(v)) |> 
    dplyr::mutate(one = 1, AirQualityStationEoICode = as.character(AirQualityStationEoICode)) |> 
    dplyr::group_by(AirQualityStationEoICode, year) |> 
    dplyr::summarise(cov = sum(one)/s, AirPollutant = p, .groups = "drop") 
  
  if (collect) x = dplyr::collect(x)
  return(as_arrow_table(x))
}

filter_temp_cov = function(x, tc, p){
  polls = c("PM10","PM2.5","O3","NO2")
  thresh = c("PM10" = 10000, "PM2.5"= 10000, "O3"= 500, "NO2"= 150)
  
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

temp_agg_mean = function(x, step, collect = F){
  stopifnot("step should be one of 'year','month', or 'day')" = step %in% c("year","month","day"))
  temp_vars_sel = switch(step,
                         "year" = c("year"),
                         "month" = c("year","month"),
                         "day" = c("year", "month", "doy"))
  p = find_poll(x)
  vrs_sel = c(temp_vars_sel, "v" = p)
  
  temp_vars_group = switch(step,
                           "year" = dplyr::vars(as.symbol("year")),
                           "month" = dplyr::vars(as.symbol("year"), as.symbol("month")),
                           "day" = dplyr::vars(as.symbol("year"), as.symbol("month"), as.symbol("doy")))
  vrs_group = c(dplyr::vars(AirQualityStationEoICode), temp_vars_group)
  
  x = dplyr::select(x, AirQualityStationEoICode, SSR, vrs_sel) |> 
    dplyr::group_by_at(vrs_group, .add = T) |> 
    dplyr::summarise(v = mean(v, na.rm = T),
              SSR = mean(SSR, na.rm = T),
              .groups = "drop") |> 
    dplyr::rename(!!p := v) |> add_meta()
  if (collect) x = dplyr::collect(x)
  return(x)
}

# 8hr running mean
# https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-50/appendix-Appendix%20I%20to%20Part%2050
temp_agg_perc = function(x, step, perc, collect = F){
  stopifnot("step should be one of 'year','month', or 'day')" = step %in% c("year","month","day"))
  temp_vars_sel = switch(step,
                     "year" = c("year"),
                     "month" = c("year","month"),
                     "day" = c("year", "month", "doy"))
  p = find_poll(x)
  vrs_sel = c(temp_vars_sel, "v" = p)
  
  temp_vars_group = switch(step,
                     "year" = dplyr::vars(as.symbol("year")),
                     "month" = dplyr::vars(as.symbol("year"), as.symbol("month")),
                     "day" = dplyr::vars(as.symbol("year"), as.symbol("month"), as.symbol("doy")))
  vrs_group = c(dplyr::vars(AirQualityStationEoICode), temp_vars_group)
 
  x = dplyr::select(x, AirQualityStationEoICode, SSR, vrs_sel) |> 
    dplyr::group_by_at(vrs_group) |> 
    dplyr::summarise(v = quantile(v, perc, na.rm = T),
              SSR = mean(SSR, na.rm = T),
              .groups = "drop") |>  dplyr::rename(!!p := v) |> add_meta()
  
  if (collect) x = dplyr::collect(x)
  return(x)
}

process_temp_agg = function(aq, p, step, perc = NULL, overwrite = T, ...){
  stopifnot("step should be one of 'year','month', or 'day')" = step %in% c("year","month","day"))
  dir = switch(step, "year" = "AQ_data/06_annual",
               "month" = "AQ_data/05_monthly",
               "day" = "AQ_data/04_daily")
  if (!dir.exists(dir)) dir.create(dir)
  ttag = (basename(dir) |> strsplit("_"))[[1]][[2]]
  aggtag = ifelse(is.null(perc), "mean", "perc")
  
  out = if (step == "day"){
    dir = file.path(dir, paste0(p, "_", aggtag))
    dir
  } else {
    file.path(dir, paste0(p, "_", ttag, "_", aggtag, ".parquet"))
  }
  fex = all(file.exists(out))
  if (!fex | (fex & overwrite)) {
    aq = filter_temp_cov(aq, stations_covered, p)
    aq = if (is.null(perc)){
      temp_agg_mean(aq, step, ...)
    } else {
      temp_agg_perc(aq, step, perc, ...)
    }
    if (step == "day"){
      arrow::write_dataset(aq, dir, partitioning = "year", 
                           basename_template = paste0(p, "_", ttag, "_", aggtag, "_{i}.parquet"))
    } else {
      arrow::write_parquet(aq, out)
    }
    gc()
  }
}



## Climate data conversion  ====================================================

# relative humidity
rh = function(t, d){
  (exp((17.625*d)/(243.04+t))/(exp((17.625*t)/(243.04+t)))) * 100
}

# wind direction
wdir <-function(u, v, units = F, round = F){
  # if (!any(grepl("proxy", class(u)))){
  #   u = units::drop_units(u)
  #   v = units::drop_units(v)
  # }
  wd = (270-atan2(u,v)*180/pi)%%360 
  if (round) wd = round(wd)
  if (units) units(wd) = "°"
  return(wd)
}

# wind speed
wspeed <-function(u, v,  units = F){
  # if (!any(grepl("proxy", class(u)))){
  #   u = units::drop_units(u)
  #   v = units::drop_units(v)
  # }
  ws = sqrt(u^2 + v^2)
  if (units) units(ws) = "m/s"
  return(ws)
}

## Interpolation ===============================================================

load_poll = function(poll, stat, y, m=0, d=0){
  if (!any(m==0, d==0)) stop("Supply only either m (month, 1-12) or d (day of year; 1-365).")
  if (!poll %in% c("PM10","PM2.5","O3","NO2")) stop("Select one of PM10, PM2.5, O3, NO2.")
  if (!stat %in% c("perc", "mean")) stop("Select one of mean or perc.")
  dir = file.path("AQ_data", ifelse(m == 0 & d == 0, "06_annual", ifelse(d == 0, "05_monthly", "04_daily")))
  
  info = paste("Loading", poll, "data. Year =", y)
  
  if (d > 0){
    dir = paste0(dir, "/", poll, "_", stat)
    info = paste(info, "- Day of Year =", d)
    message(info)
    arrow::open_dataset(dir) |> dplyr::filter(year == y, doy == d) |> dplyr::collect()
  } else if (m > 0){
    pfile = list.files(dir, pattern = glob2rx( paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(pfile)==1)
    info = paste(info, "- Month =", m)
    message(info)
    arrow::read_parquet(pfile) |> dplyr::filter(year == y, month == m)
  } else {
    pfile = list.files(dir, pattern = glob2rx( paste0(poll, "*", stat, "*")), full.names = T)
    stopifnot(length(pfile)==1)
    message(info)
    arrow::read_parquet(pfile) |> dplyr::filter(year == y)
  }
}


load_ETC_covs_annual = function(aq){
  pn = c("PM2.5", "PM10", "NO2", "O3")
  poll = pn[which(pn %in% names(aq))]
  y = unique(aq$year)
  stopifnot(length(y)==1)
  
  clc = terra::rast("supplementary/static/CLC_reclass_8_1km.tif") 
  read_n_warp = function(x, n) terra::rast(x) |> terra::project(clc) |> 
    terra::resample(clc, method = "bilinear") |> 
    stars::st_as_stars() |> 
    #stars::st_set_dimensions(point = F) |> 
    setNames(n)
  
  cams_name = paste0("CAMS_", poll)
  cams = read_n_warp(paste0("supplementary/03_annual/", poll, "_annual_", y, ".nc"), n = cams_name)
  
  ws = read_n_warp(paste0("supplementary/03_annual/Wind_Speed_annual_", y, ".nc"), n = "WindSpeed")
  
  clc_st = stars::st_as_stars(clc) |> setNames("CLC")
  stars::st_dimensions(clc_st)$x$point = NULL; stars::st_dimensions(clc_st)$y$point = NULL
  stars::st_dimensions(clc_st) = stars::st_dimensions(cams)
  
  l = switch(poll, 
             "PM10" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, clc_st, # alt,
                           read_n_warp(paste0("supplementary/03_annual/Rel_Humidity_annual_", y, ".nc"), n = "RelHumidity")), 
             "PM2.5" = list(log(cams) |> setNames(paste0("log_", cams_name)), ws, clc_st), #, alt)
             "O3" = list(cams, ws, clc_st, 
                         read_n_warp(paste0("supplementary/03_annual/Solar_Radiation_annual_", y, ".nc"), n = "SolarRadiation")))
  # "NO2 = ...
  
  do.call(c, l)
}