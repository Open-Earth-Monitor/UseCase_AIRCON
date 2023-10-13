library(ecmwfr)
source("R/functions.R")
source("R/ecmwf_login.R")

wf_set_key(user = uid_cams, key = key_cams, service = "ads")

stations = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")

box = c(72, -25, 30, 35) # north, west, south, east
variables = c('nitrogen_dioxide', 'ozone', 'particulate_matter_10um',
              'particulate_matter_2.5um')

for (y in 2022:2015){
  for(v in variables){
    file = paste0("supplementary/cams/EU_", v, "_hrly_", y,".nc")
    request <- list(
      date = paste0(y,"-01-01/",y,"-12-31"),
      format = "netcdf",
      variable = v,
      type = 'analysis',
      model = 'ensemble',
      leadtime_hour = 0,
      level = 0,
      time = paste0(c(paste0(0,0:9), 10:23), ":00"),
      area = box,
      dataset_short_name = 'cams-europe-air-quality-forecasts',
      target = file
    )
    
    # If you have stored your user login information
    # in the keyring by calling cds_set_key you can
    # call:
    file <- wf_request(
      user     = uid_cams,   # user ID (for authentification)
      request  = request,  # the request
      transfer = F,     # download the file
      path     = "."       # store data in current working directory
    )
  }
}

