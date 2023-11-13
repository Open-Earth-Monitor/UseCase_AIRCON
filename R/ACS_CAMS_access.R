library(ecmwfr)
source("R/functions.R")
source("R/ecmwf_login.R")

wf_set_key(user = uid_cams, key = key_cams, service = "ads")

stations = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")

box = c(72, -25, 30, 35) # north, west, south, east
variables = c('nitrogen_dioxide', 'ozone', 'particulate_matter_10um',
              'particulate_matter_2.5um')


################### Choose one ###########################

### Forecast data (Aug 2020 - now)
# dataset_short_name = 'cams-europe-air-quality-forecasts'
# type = 'analysis'
# format = "netcdf"

## - - - - - - - - - - - - - - - - - - - - - - - - - - -

### Reanalysis data (2013-2022)
# dataset_short_name = 'cams-europe-air-quality-reanalyses'
# type = c('validated_analysis', 'interim_reanalysis')
# format = "zip"
## - - - - - - - - - - - - - - - - - - - - - - - - - - -


parse_request = function(year, var, dataset = "reanalysis", bbox = box,
                         month = paste0(c(paste0(0,1:9), 10))){
  req = switch(dataset,
               "reanalysis" = list(
                 year = year, month = month, variable = var, model = 'ensemble',
                 level = 0, format = "zip", type = ifelse(year < 2021, 'validated_reanalysis', 'interim_reanalysis'), 
                 dataset_short_name = 'cams-europe-air-quality-reanalyses', 
                 target = paste0("supplementary/cams/EU_", var, "_hrly_", year,"_reanalysis.nc")),
               "forecast" = list(
                   date = paste0(year,"-01-01/",year,"-12-31"), time = paste0(c(paste0(0,0:9), 10:23), ":00"),
                   variable = var, leadtime_hour = 0, level = 0, area = box, 
                   target = paste0("supplementary/cams/EU_", var, "_hrly_", year,"_forecast.nc"),
                   format = "netcdf", type = 'analysis', model = 'ensemble',
                   dataset_short_name = 'cams-europe-air-quality-forecasts')
                )
  return(req)
}


# send multiple requests to ECMWF to keep requests and downloads small.
for (y in 2022:2015){
  for(v in variables){
    request <- parse_request(y,v, "reanalysis")#, month = 10:12)
    
    file <- wf_request(
      user     = uid_cams,   # user ID (for authentification)
      request  = request,  # the request
      transfer = F,     # download the file
      path     = "."       # store data in current working directory
    )
  }
}

