# library(stars)
# era = list.files("supplementary/era5",  full.names = T)
# nms_era = c("v10#long_name=", "u10#long_name=", "d2m#long_name=", "ssr#long_name=", "t2m#long_name=")
# grep_name = function(f, nms){
#   
#   v = gdal_metadata(f)
#   for (n in nms){
#     if (any(grepl(n, v))){
#       v = grep(n, v, value = T)    
#       v = sub("^.*=", "", v)
#       v = gsub("\\(|\\)", "", v)
#       v = gsub(" ", "_", v)
#     }
#   }
#   return(v)
# }
# 
# 
# for (f in era){
#   x = read_stars(f) |> st_dimensions()
#   y = x$time$offset |> substr(1,4)
#   
#   v = grep_name(f, nms_era)
#   
#   path = paste0("supplementary/era5/ERA5_", v, "_", y, ".nc")
#   file.rename(f, path)
#   message(path)
# }


cams = list.files("supplementary/cams", full.names = T)

for (f in cams){
  try({
    unzip(f, exdir = "supplementary/cams_unzip")
    file.rename(f, paste0(f, "_unzipped"))
    })
  message(f)
}


cams_uz = data.frame(file = list.files("supplementary/cams_unzip"))
cams_uz = tidyr::separate(cams_uz, file, remove = F, sep = "\\.", into = c("a",'b','type','c','var','d','ym','e')) |> 
  dplyr::select(-c(a,b,c,d,e)) |> 
  tidyr::separate(ym, sep = "\\-", into = c("y",'m')) |> 
  dplyr::mutate(type = as.factor(type),
                var = as.factor(var),
                y = as.factor(y),
                m = as.factor(m))
summary(cams_uz, maxsum = 12)

