library(ecmwfr)
#source("R/functions.R")
source("R/ecmwf_login.R")

options(keyring_backend="file")
wf_set_key(user = uid_cams, key = key_cams, service = "ads")

#stations = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")

box = c(72, -25, 30, 45) # north, west, south, east
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


parse_request = function(year, var, semester, bbox = box){
  monthdate = switch(semester,
                     "1" = list(month = c(paste0(0,1:6)), 
                              date = paste0(year,"-01-01/",year,"-06-30")),
                     "2" = list(month = paste0(c(paste0(0,7:9), 10:12)), 
                              date = paste0(year,"-07-01/",year,"-12-31")))
  
  dataset = ifelse(year < 2023, "reanalysis", "forecast")
  req = switch(dataset,
               "reanalysis" = list(
                 year = year, month = monthdate[["month"]], variable = var, model = 'ensemble',
                 level = 0, format = "zip", type = ifelse(year < 2021, 'validated_reanalysis', 'interim_reanalysis'), 
                 dataset_short_name = 'cams-europe-air-quality-reanalyses', 
                 target = paste0("CAMS_", var, "_hrly_", year,"_reanalysis_", semester,".zip")),
               "forecast" = list(
                   date = monthdate[["date"]], time = paste0(c(paste0(0,0:9), 10:23), ":00"),
                   variable = var, leadtime_hour = 0, level = 0, area = box, 
                   target = paste0("CAMS_", var, "_hrly_", year,"_forecast_", semester,".zip"),
                   format = "netcdf", type = 'analysis', model = 'ensemble',
                   dataset_short_name = 'cams-europe-air-quality-forecasts')
                )
  return(req)
}


# create and store requests
for (y in 2023:2015){
  for (v in variables){
    for (s in c("1", "2")){
      request = parse_request(y, v, s, box)#, month = 10:12)
      
      cams_req = wf_request(
        user     = uid_cams,   # user ID (for authentification)
        request  = request,  # the request
        transfer = F,     # download the file
        path     = "supplementary/cams_download"       # store data in current working directory
      )
      saveRDS(cams_req, paste0("supplementary/cams_requests/request_",v, "_", y,"_", s, ".rds"))
      message(request$target)
    }
  }
}

# download in case request has been processed ("completed")
reqs = list.files("supplementary/cams_requests", full.names = T)
for (r in reqs[33]) {
  req = readRDS(r)
  req = req$update_status()
  file = file.path("supplementary/cams_download", req$get_request()$target)
  beepr::beep_on_error({
    if (req$get_status() == "completed") {
      message(basename(file))
      if (!file.exists(file)){
        job::job({
          source("R/ecmwf_login.R")
          #req$.__enclos_env__$private$file = basename(file)
          req$.__enclos_env__$private$path = "supplementary/cams_download"
          req$download(verbose = T)
        }, import = "auto", packages = "ecmwfr", title = basename(file))
        message(file, ": now downloading.")
      }
    } else {
      message(file, ": downloaded.")
    }
  })
}

# unzip
zipfiles = list.files("supplementary/cams_download", "CAMS_", full.names = T)
unz_del = function(f){
  if (!grepl("2023",f)){
    system(paste("unzip -u -d supplementary/cams_download", f))
    unlink(f)
  }
}

lapply(zipfiles, unz_del)

pbmcapply::pbmclapply(zipfiles, unz_del, mc.cores = 4)
