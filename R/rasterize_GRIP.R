library(sf)
library(dplyr)
#library(stars)
library(terra)
library(tictoc)
setwd("/mnt/cloud/wwu1/ec_bronze/_nogroup/ae78a1ca-a0e8-4e4e-8992-69c34947db65/UseCase_AIRCON")

source("R/functions.R")

grip_ori = vect("supplementary/static/GRIP/GRIP4_region4.shp")[,c("GP_RTP", "GP_RCY")]
grip = subset(grip_ori, grip_ori$GP_RTP <= 3 
             # & grip_ori$GP_RCY == 233             # test with Estonia
              ) |> 
  project("epsg:3035")

#plot(grip, "GP_RTP")

# ==============================================================================
#                        AREA IMPACTED BY ROADS
# ==============================================================================

# make buffers 
tic()
grip$bu = ifelse(grip$GP_RTP < 3, 75, 50)
grip_buff = buffer(grip, "bu")

r = rast(grip_buff, resolution = 1000)
r_rough = aggregate(r, 30) |> terra::as.polygons()
r_rough
toc()

rm(grip_ori, grip)
gc()
#plot(grip_d)

# # aggregate (dissolve) buffers and rasterize to 1km
# tic()
# grip_buff_1 = terra::aggregate(grip_buff[grip_buff$GP_RTP == 1,])  # dissolve = T
# grip_buff_1_chunks = terra::intersect(grip_buff_1, r_rough)
# writeVector(grip_buff_1_chunks, "supplementary/static/GRIP/grip_type_1_buff_chunks.gpkg")
# print(grip_buff_1_chunks)
# rasterizeGeom(grip_buff_1_chunks, r, fun = "area", unit = "km", overwrite=T,
#               filename = "supplementary/static/GRIP/GRIP_type_1_area_1x1km.tif")
# rm(grip_buff_1, grip_buff_1_chunks)
# gc()
# toc()
# 
# tic()
# grip_buff_2 = terra::aggregate(grip_buff[grip_buff$GP_RTP == 2,])  # dissolve = T
# grip_buff_2_chunks = terra::intersect(grip_buff_2, r_rough)
# print(grip_buff_2_chunks)
# writeVector(grip_buff_2_chunks, "supplementary/static/GRIP/grip_type_2_buff_chunks.gpkg")
# rasterizeGeom(grip_buff_2_chunks, r, fun = "area", unit = "km", overwrite=T,
#               filename = "supplementary/static/GRIP/GRIP_type_2_area_1x1km.tif")
# rm(grip_buff_2, grip_buff_2_chunks)
# gc()
# toc()
# 
# tic()
# grip_buff_3 = terra::aggregate(grip_buff[grip_buff$GP_RTP == 3,])  # dissolve = T
# grip_buff_3_chunks = terra::intersect(grip_buff_3, r_rough)
# print(grip_buff_3_chunks)
# writeVector(grip_buff_3_chunks, "supplementary/static/GRIP/grip_type_3_buff_chunks.gpkg")
# rasterizeGeom(grip_buff_3_chunks, r, fun = "area", unit = "km", overwrite=T,
#               filename = "supplementary/static/GRIP/GRIP_type_3_area_1x1km.tif")
# rm(grip_buff_3, grip_buff_3_chunks)
# gc()
# toc()
#####################################################

# combined area affect by road classes 1-3
g = rbind(vect("supplementary/static/GRIP/grip_type_1_buff_chunks.gpkg"),
          vect("supplementary/static/GRIP/grip_type_2_buff_chunks.gpkg"),
          vect("supplementary/static/GRIP/grip_type_3_buff_chunks.gpkg"))
r = rast(g, resolution = 1000)

gr = rasterizeGeom(g, r, fun = "area", unit = "km", overwrite=T,
              filename = "supplementary/static/GRIP/GRIP_types_123_area_1x1km.tif")
rm(g)
gc()

grip_warp = read_n_warp("supplementary/static/GRIP/GRIP_types_123_area_1x1km.tif", 
                        lyr = 1, name = "grip_roads_123", 
                        target = "supplementary/static/CLC_reclass_8_1km.tif",
                        parallel = 8)
write_stars(grip_warp, "supplementary/static/GRIP/GRIP_types_123_area_1x1km.tif") 
#write_stars(grip_warp, "supplementary/static/merge_weights/urban_weights_1km.tif") 

# ==============================================================================
#                    DISTANCE TO NEAREST BY ROAD TYPE
# ==============================================================================


r = rast(grip, resolution = 1000)

d1 = terra::distance(r, grip[grip$GP_RTP == 1,], rasterize = T,  overwrite = T) / 1000
writeRaster(d1, "supplementary/static/GRIP/GRIP_type_1_distance_1x1km.tif", overwrite=T)

d2 = terra::distance(r, grip[grip$GP_RTP == 2,], rasterize = T) / 1000
writeRaster(d2, "supplementary/static/GRIP/GRIP_type_2_distance_1x1km.tif", overwrite=T)

d3 = terra::distance(r, grip[grip$GP_RTP == 3,], rasterize = T) / 1000
writeRaster(d3, "supplementary/static/GRIP/GRIP_type_3_distance_1x1km.tif", overwrite=T) 
 

plot(d1)
plot(d2)
plot(d3)


















