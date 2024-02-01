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



