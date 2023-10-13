## Downloads

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
  fails = character(0)
  t = system.time({
    paths = purrr::map(urls, function(u){
      dest = file.path(dir, basename(u))
      if (!file.exists(dest)){
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

## Pre-processing
read_pollutant = function(files, pollutant = "NO2", countries = NULL, dir = "download"){
  cols = c("AirQualityStationEoICode", "AirPollutant", 
           "Concentration", "DatetimeBegin", "Validity", "Verification")
  files_p = dplyr::filter(files, Pollutant == pollutant, Country %in% countries)
  data = vroom::vroom(file.path(dir, files_p$File), 
                      col_select = dplyr::all_of(cols),
                      show_col_types = F)
  dplyr::mutate(data, 
                DatetimeBegin = fasttime::fastPOSIXct(DatetimeBegin), 
                AirQualityStationEoICode = as.factor(AirQualityStationEoICode)) |> 
    dplyr::filter(!is.na(Concentration))
}

filter_quality = function(x, keep_validity = 1, keep_verification = c(1,2)){
  dplyr::filter(x, 
                Validity %in% keep_validity, 
                Verification %in% keep_verification) |> 
    dplyr::select(-Validity, -Verification)
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
  by =  dplyr::join_by(AirQualityStationEoICode)
  dplyr::left_join(pollutant, meta_data, by = by)
}

join_pollutants = function(pollutants, sf = F){
  cat(" -", pollutants[[1]]$AirPollutant[1])
  a = pollutants[[1]] |> pivot_poll()
  
  if (length(pollutants) > 1){
    by = dplyr::join_by(AirQualityStationEoICode, DatetimeBegin)
    for (i in 2:length(pollutants)){
      cat(" -",pollutants[[i]]$AirPollutant[1])
      b = pollutants[[i]] |> pivot_poll()
      a = dplyr::full_join(a, b, by = by)
    }
  }
  a = add_meta(a) |> filter_stations()
  a = dplyr::select(a, AirQualityStationEoICode, 
                    StationArea, StationType, Longitude, Latitude,
                    Elevation, Population, CLC8,
                    DatetimeBegin, dplyr::everything()) 
  
  if (sf) a = sf::st_as_sf(a, coords = c("Longitude","Latitude"), crs=4326)
  return(a)
}  

## Gapfilling
add_meta = function(pollutant, meta_data = station_meta){
  by =  dplyr::join_by(AirQualityStationEoICode)
  dplyr::left_join(pollutant, meta_data, by=by)
}

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

# relative humidity
rh = function(t, d){
  (exp((17.625*d)/(243.04+t))/(exp((17.625*t)/(243.04+t)))) * 100
}

# wind direction
wdir <-function(u,v){
  (270-atan2(u,v)*180/pi)%%360 
}

# wind speed
wspeed <-function(u,v){
  sqrt(u^2+v^2)
}

