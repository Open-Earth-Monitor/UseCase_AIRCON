urls = data.frame(url = readLines("data/CDS_ERA5_download_urls.txt")) |> 
  unique()
options(timeout = 36000)

for (u in urls$url[31:40]){
  f = paste0("data/era5/", basename(u))
  if(!file.exists(f)){
    download.file(u, f)
    message(u)
  }}