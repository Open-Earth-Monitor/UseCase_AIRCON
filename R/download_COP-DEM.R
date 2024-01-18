# Find relevant tiles

library(stars)
clc = read_stars("supplementary/static/CLC_reclass_8.tif") 
clc_bb = st_bbox(clc) |> st_as_sfc()

grid = st_read("supplementary/static/COP-DEM/CopernicusDEM-RP-002_GridFile_I6.0.shp/GEO1988-CopernicusDEM-RP-002_GridFile_I6.0.shp") |> 
  st_transform(st_crs(clc)) |> 
  st_filter(clc_bb)

plot(st_geometry(grid))


x = terra::rast("s3://copernicus-dem-90m/Copernicus_DSM_COG_30_S90_00_W178_00_DEM", vsi=T)


# Check out AWS bucket and file structure

library(aws.s3)
bucket_exists(
  bucket = "s3://copernicus-dem-90m/", 
  region = "eu-central-1"
)

dem90_tiles <- get_bucket_df(
  bucket = "s3://copernicus-dem-90m/", 
  region = "eu-central-1",
  max = 300
) |> 
  as_tibble()

head(dem90_tiles$Key |> basename())

# Download
# product_name is found in grid, columns ...
download_copdem_90m = function(product_name, dir = "supplementary/static/COP-DEM/tiles"){
  
  tile_name = sub("Copernicus_DSM", "Copernicus_DSM_COG", product_name)
  tile_path = paste0(tile_name, "_DEM/", tile_name, "_DEM.tif")
  out = file.path(dir, basename(tile_path))
  if (!file.exists(out)){
    aws.s3::save_object(
      object = tile_path,
      bucket = "s3://copernicus-dem-90m/", 
      region = "eu-central-1",
      file = out
    )
}}

job::job({
  pbmcapply::pbmclapply(grid$Product30, download_copdem_90m, mc.cores = 8)
})

# Merge
tifs = list.files("supplementary/static/COP-DEM/tiles", full.names = T)
length(tifs) == nrow(grid)
read_stars(tifs[1000]) |> plot()


library(gdalUtilities)

#vrt = gdalbuildvrt(tifs, "supplementary/static/COP-DEM/dem.vrt")
vrt = "supplementary/static/COP-DEM/dem.vrt"

gdalinfo("supplementary/static/CLC_reclass_8_1km.tif")
dem1k = gdalwarp(vrt, "supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif", 
                 t_srs = "EPSG:3035", te_srs = "EPSG:3035", r = "bilinear", #dryrun = T,
                 te = c(944000.000, 942000.000, 6505000.000, 5414000.000), tr = c(1000,1000),
                 wo = c("NUM_THREADS=8"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3"))
read_stars("supplementary/static/COP-DEM/COP_DEM_Europe_1km_epsg3035.tif") |> plot()

dem90 = gdalwarp(vrt, "supplementary/static/COP-DEM/COP_DEM_Europe_90m_epsg3035.tif", 
                 t_srs = "EPSG:3035", r = "bilinear", tr = c(90,90), dryrun = T,
                 wo = c("NUM_THREADS=16"), wm = 500, config_options = c("GDAL_CACHEMAX"="500"),
                 multi = T, co = c("COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=YES"))

