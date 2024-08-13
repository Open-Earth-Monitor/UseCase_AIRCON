library(stars)
library(terra)
library(gdalUtilities)

# ==============================================================================
# Corine LC
labels = c("HDR", "LDR", "IND","TRAF", "UGR","AGR","NAT","OTH")

# 100 m 8 class
clc_8class = "supplementary/static/CLC_reclass_8.tif"
if (!file.exists(clc_8class)){
  lc = rast("supplementary/lcv_landcover.clc_corine_c_100m_0..0cm_2018_eumap_epsg3035_v2020.tif")
  NAflag(lc) = 128
  summary(lc)
  rcl = cbind(c(1:48),
              c(1,2,3, rep(4,3), rep(3,3), rep(5,2), rep(6, 11), rep(7,12), rep(8,14)))
  lc8 = classify(lc, rcl, filename = clc_8class,
                 datatype = "INT1U", NAflag = 0, overwrite=T,
                 gdal=c("COMPRESS=DEFLATE"))
} else {
  clc = read_stars(clc_8class)
  gdalinfo(clc_8class)
}
# NAflag(lc8) = 128
# levels(lc8) =  data.frame(id = c(1:8),
#                           CLC8 = c("HDR", "LDR", "IND","TRAF","UGR","AGR","NAT","OTH"))
clc_bb = st_bbox(clc) |> st_as_sfc()
clc_ext = c(900000.000, 900000.000, 7400000.000, 5500000.000)

# 1 km 8 class
gdalwarp(clc_8class, "supplementary/static/CLC_reclass_8_1km.tif", 
         t_srs = "EPSG:3035", r = "mode", tr = c(1000,1000), #dryrun = T,
         te = clc_ext, overwrite = T,
         wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
         multi = T, co = c("COMPRESS=DEFLATE", "BIGTIFF=YES"))

# 10 km 8 class
gdalwarp(clc_8class, "supplementary/static/clc/CLC_reclass_8_10km.tif", 
         t_srs = "EPSG:3035", r = "mode", tr = c(10000,10000), #dryrun = T,
         te = clc_ext, overwrite = T,
         wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
         multi = T, co = c("COMPRESS=DEFLATE", "BIGTIFF=YES"))

# 1 km fractional cover
clc_100 = rast(clc_8class) |> setNames("CLC")
clc_1km = rast("supplementary/static/CLC_reclass_8_1km.tif")


for (n in 1:length(labels)){
  tictoc::tic()
  clc_s = clc_100 == n
  clc_agg = terra::aggregate(clc_s, 10, sum, na.rm=TRUE) 
  clc_agg_m = terra::mask(clc_agg, clc_1km)
  tictoc::toc()
  plot(clc_agg_m, colNA=1)
  out = paste0("supplementary/static/clc/CLC_", labels[n], "_percent_1km.tif")
  writeRaster(clc_agg_m, out, names = n, datatype = "INT1U", overwrite = T)
}

# 1 km fractional cover
for (n in 1:length(labels)){
  out = paste0("supplementary/static/clc/CLC_", labels[n], "_percent_10km.tif")
  clc_1k = rast(paste0("supplementary/static/clc/CLC_", labels[n], "_percent_1km.tif"))
  clc_agg = terra::aggregate(clc_1k, 10, sum, na.rm=TRUE) / 100
  plot(clc_agg, colNA=1, main=labels[n])
  writeRaster(clc_agg, out, names = labels[n], datatype = "INT1U", overwrite = T)
}

# 5 km radius fractional cover
clc_8class = "supplementary/static/CLC_reclass_8.tif"
clc_100 = rast(clc_8class) |> setNames("CLC")
circle = starsExtra::w_circle(101)
circle[circle==0] = NA
n_cell_5km = sum(circle, na.rm = T) # 8013
circle = circle / n_cell_5km
st_as_stars(circle) |> plot(axes=T)
labels = c("HDR", "LDR", "IND","TRAF", "UGR","AGR","NAT","OTH")

frac_cover_5km_CLC = function(label){
  out = paste0("supplementary/static/clc/CLC_", label, "_percent_5km_radius_100m.tif")
  out2 = paste0("supplementary/static/clc/CLC_", label, "_percent_5km_radius_1km.tif")
  if (!file.exists(out)){
    message(label)
    tictoc::tic()
    clc_s = clc_100 == n
    clc_f = focal(clc_s, w = circle, fun = sum, na.rm=TRUE, na.policy="omit",      # 100m
                  filename = out, wopt = list(datatype = "INT1U"))
    
    clc_agg = aggregate(clc_f, 10, mean, na.rm=TRUE,                               # 1km 
                        filename = out2, wopt = list(datatype = "INT1U"))
    tictoc::toc()
    plot(clc_agg, main = labels[n], colNA=1)
    #writeRaster(clc_s, out, datatype = "INT8U")
    gc()
}}

for (n in 1:length(labels)){
  out = paste0("supplementary/static/clc/CLC_", labels[n], "_percent_5km_radius_100m.tif")
  out2 = paste0("supplementary/static/clc/CLC_", labels[n], "_percent_5km_radius_1km.tif")
  if (!file.exists(out)){
  message(labels[n])
  tictoc::tic()
  clc_s = clc_100 == n
  clc_f = focal(clc_s, w = circle, fun = sum, na.rm=TRUE, na.policy="omit",      # 100m
                filename = out, wopt = list(datatype = "INT1U"))
  
  clc_agg = aggregate(clc_f, 10, mean, na.rm=TRUE,                               # 1km 
                      filename = out2, wopt = list(datatype = "INT1U"))
  tictoc::toc()
  plot(clc_agg, main = labels[n], colNA=1)
  gc()
  }
}
# ==============================================================================

# Population density grid 1KM

# SEDAC: https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-density-rev11/data-download#close
# Center for International Earth Science Information Network - CIESIN - Columbia University. 2018. Gridded Population of the World, Version 4 (GPWv4): Population Density, Revision 11. Palisades, New York: NASA Socioeconomic Data and Applications Center (SEDAC). https://doi.org/10.7927/H49C6VHW. Accessed DAY MONTH YEAR. 

pop_wgs = "supplementary/static/gpw_v4_population_density_rev11_2020_30_sec.tif"
read_stars(pop_wgs)


pop_orig = gdalwarp(pop_wgs, "supplementary/static/pop_density_original_epsg3035.tif", 
                 t_srs = "EPSG:3035",  r = "bilinear", #dryrun = T,
                 te = clc_ext, te_srs = "EPSG:3035",
                 wo = c("NUM_THREADS=16"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3"))

pop1k = gdalwarp(pop_wgs, "supplementary/static/pop_density_1km_epsg3035.tif", 
                 t_srs = "EPSG:3035", te_srs = "EPSG:3035", r = "bilinear", #dryrun = T,
                 te = clc_ext, tr = c(1000,1000),
                 wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3"))

# calculate urban character
pop1k = rast("supplementary/static/pop_density_1km_epsg3035.tif")
v = values(pop1k)
hist(v)
lim = quantile(v, c(0.999), na.rm=T) # 5458.101
v[v>lim] = lim
hist(v)
pop1k[pop1k > lim] = lim             # limit to lim to have a better distribution of weights
pop1k_scaled = pop1k / lim
plot(pop1k_scaled)
writeRaster(pop1k_scaled, "supplementary/static/pop_density_weights_perc99.9_1km_epsg3035.tif")

# ==============================================================================
# COP-DEM

# https://registry.opendata.aws/copernicus-dem/
# https://copernicus-dem-30m.s3.amazonaws.com/readme.html

# Download in 'download_COP-DEM.R'

grid = st_read("supplementary/static/COP-DEM/CopernicusDEM-RP-002_GridFile_I6.0.shp/GEO1988-CopernicusDEM-RP-002_GridFile_I6.0.shp") |> 
  st_transform(st_crs(clc)) |> 
  st_filter(clc_bb)

# Merge
tifs = list.files("supplementary/static/COP-DEM/tiles", full.names = T)
length(tifs) == nrow(grid)
read_stars(tifs[1000]) |> plot()


#vrt = gdalbuildvrt(tifs, "supplementary/static/COP-DEM/dem.vrt")
vrt = "supplementary/static/COP-DEM/dem.vrt"

dem1k = gdalwarp(vrt, "supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif", 
                 t_srs = "EPSG:3035", te_srs = "EPSG:3035", r = "bilinear", #dryrun = T,
                 te = clc_ext, tr = c(1000,1000), overwrite = T,
                 wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3"))

dem1k_mask = mask(rast("supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif"),
                  rast("supplementary/static/CLC_reclass_8_1km.tif"), 
                  filename = "supplementary/static/COP-DEM/COP_DEM_Europe_1km_mask_epsg3035.tif")
plot(dem1k_mask, colNA=1)

read_stars("supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif") |> plot(reset=F)
plot(giscoR::gisco_countries$geometry |> st_transform(3035), add=T, border="red")

dem100 = gdalwarp(vrt, "supplementary/static/COP-DEM/COP_DEM_Europe_100m_epsg3035.tif", 
                 t_srs = "EPSG:3035", r = "bilinear", tr = c(100,100), dryrun = T,
                 te = clc_ext, overwrite = T,
                 wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))

dem10k = gdalwarp("supplementary/static/COP-DEM/COP_DEM_Europe_100m_epsg3035.tif", 
                  "supplementary/static/COP-DEM/COP_DEM_Europe_10km_epsg3035.tif",
                  t_srs = "EPSG:3035", r = "bilinear", tr = c(10000,10000), #dryrun = T,
                  te = clc_ext, overwrite = T,
                  wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                  multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))

dem10k_mask = mask(rast("supplementary/static/COP-DEM/COP_DEM_Europe_10km_epsg3035.tif"),
                  rast("supplementary/static/clc/CLC_AGR_percent_10km.tif"), 
                  filename = "supplementary/static/COP-DEM/COP_DEM_Europe_10km_mask_epsg3035.tif")
plot(dem10k_mask, colNA=1)

# 5km radius fractional cover
dem1km = rast("supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif") |> setNames("DEM")
clc_1km = rast("supplementary/static/CLC_reclass_8_1km.tif") |> setNames("CLC")
dem1km = mask(dem1km, clc_1km)
plot(dem1km)
circle = starsExtra::w_circle(11)
circle[circle==0] = NA
n_cell_5km = sum(circle, na.rm = T) # 
circle = circle / n_cell_5km
st_as_stars(circle) |> plot(axes=T)

out = paste0("supplementary/static/COP-DEM/COP_DEM_Europe_5km_radius_mean_1km_epsg3035.tif")

tictoc::tic()
dem_f = focal(dem1km, w = circle, fun = mean, na.rm=TRUE, na.policy="omit",      # 1km
              filename = out, wopt = list(datatype = "INT2S"), overwrite=T)
tictoc::toc()
plot(dem_f, colNA=1)

# ==============================================================================
# ==============================================================================
# ==============================================================================

