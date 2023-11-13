#urls = data.frame(url = readLines("supplementary/CCS_ERA5_download_urls.txt")) |> 
urls = data.frame(url = readLines("supplementary/AMS_CAMS_download_urls.txt")) |> 
  unique()
options(timeout = 36000)

for (u in urls$url){
  f = paste0("supplementary/cams/", basename(u))
  if(!file.exists(f)){
    download.file(u, f)
    message(u)
  }
}
