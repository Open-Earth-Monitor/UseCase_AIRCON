library(ecmwfr)
#source("functions.R")
options(keyring_backend="file")
source("R/ecmwf_login.R")

# wf_set_key(user = uid_era, key = key_era, service = "cds")

box = c(72, -25, 30, 45)

variables = c('10m_u_component_of_wind', 
              '10m_v_component_of_wind',
              '2m_dewpoint_temperature', 
              '2m_temperature',
              'surface_net_solar_radiation')


# create and store requests
for (y in 2023:2015){
  for(v in variables){
    file = paste0("supplementary/era5_download/ERA5_", v, "_hrly_", y,".nc")
    
    request = list(
      date = paste0(y,"-01-01/",y,"-12-31"),
      format = "netcdf",
      variable = v,
      time = paste0(c(paste0(0,0:9), 10:23), ":00"),
      area = box,
      dataset_short_name = "reanalysis-era5-land",
      target = file
    )
    if (!file.exists(file)){
      
        dl = wf_request(
          request  = request,  
          user     = "149688",   # user ID (for authentification)
          transfer = F,          # download the file
          time_out = 3600*3,
          path     = file
        )
        print(dl)
        saveRDS(dl, paste0("supplementary/era5_requests/request_",v, "_", y,".rds"))
    }
  }
}

# download in case request has been processed ("completed")
reqs = list.files("supplementary/era5_requests", full.names = T)
njobs = 1
for (r in reqs) {
  req = readRDS(r)
  req = req$update_status()
  if (req$get_status() == "completed") {
    file = req$get_request()$target
    if (!file.exists(file)){
      message(basename(file), ": start download ", njobs)
      job::job({
        source("R/ecmwf_login.R")
        req$.__enclos_env__$private$file = file
        req$.__enclos_env__$private$path = req$.__enclos_env__$private$path |> dirname()
        req$download(verbose = T)
      }, import = "auto", packages = "ecmwfr", title = basename(file))
      njobs = njobs+1
    }
  }
}






# single file download jobs
# y = 2023
# v = 'surface_net_solar_radiation'
# url = "https://download-0015-clone.copernicus-climate.eu/cache-compute-0015/cache/data8/adaptor.mars.internal-1705573865.506481-10021-7-94229913-8418-4ab8-9fad-306efec40ebf.nc"
# file = paste0("supplementary/era5_download/ERA5_", v, "_hrly_", y,".nc")
# job::job({
#   options(timeout = 36000)
#   download.file(url, file)
#   }, 
#   title = basename(file), import=NULL)









