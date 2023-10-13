
setwd("/cloud/wwu1/s_oemc/share")

dir = "/cloud/wwu1/s_oemc/share/data/era5"

variables = c('10m_u_component_of_wind', '10m_v_component_of_wind',
              '2m_dewpoint_temperature', '2m_temperature',
              'surface_net_solar_radiation')

dl = list()
for (y in 2022:2015){
  for(v in variables){
    x = data.frame(var = v, 
                  year = y, 
                  file = paste0("EU_", v, "clim_hrly_", y,".nc")) |>  list()
    dl = append(dl, x)
  }
}

dl = do.call(rbind, dl)


dl$request_id = readLines("data/CDS_ERA5_request_ids.txt")

urls = data.frame(url = readLines("data/CDS_ERA5_download_urls.txt"))

for (u in urls$url){
  f = paste0("data/era5/", basename(u))
  if(!file.exists(f)){
    download.file(u, f)
    message(u)
}}
