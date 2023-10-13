library(ecmwfr)
source("functions.R")
source("R/ecmwf_login.R")

wf_set_key(user = uid_era, key = key_era, service = "cds")

stations = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")
box = make_ecwmf_box(c(range(stations$Longitude), 
                       range(stations$Latitude)), buffer = 2, view = T)

variables = c('10m_u_component_of_wind', '10m_v_component_of_wind',
              '2m_dewpoint_temperature', '2m_temperature',
              'surface_net_solar_radiation')

for (y in 2022:2015){
  for(v in variables){
    file = paste0("supplementary/era5/EU_", v, "clim_hrly_", y,".nc")
    request <- list(
      date = paste0(y,"-01-01/",y,"-12-31"),
      format = "netcdf",
      variable = v,
      time = paste0(c(paste0(0,0:9), 10:23), ":00"),
      area = box,
      dataset_short_name = "reanalysis-era5-land",
      target = file
    )
    
    # If you have stored your user login information
    # in the keyring by calling cds_set_key you can
    # call:
    file <- wf_request(
      user     = "149688",   # user ID (for authentification)
      request  = request,  # the request
      transfer = F,     # download the file
      path     = "."       # store data in current working directory
    )
  }
}


