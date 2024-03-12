library(stars)

weights = read_stars(c(
  "supplementary/static/GRIP/GRIP_types_123_area_1x1km.tif",
  "supplementary/static/pop_density_1km_epsg3035.tif",
  "supplementary/static/CLC_reclass_8_1km.tif")) |> 
  setNames(c("w_traffic","w_urban", "clc")) 

# traffic layer already a ratio (percent area affected by traffic)
# quantile(weights$w_traffic, c(0.95, 0.98, 0.99, 0.999, 1), na.rm=T)
# traffic_q99 = quantile(weights$w_traffic, 0.99, na.rm=T)
# traffic_q99

quantile(weights$w_urban, c(0.95, 0.98, 0.99, 0.999, 1), na.rm=T)
urban_q99 = quantile(weights$w_urban, 0.99, na.rm=T)
urban_q99

w = dplyr::mutate(weights, 
                  w_traffic = ifelse(is.na(w_traffic), 0, w_traffic),
                  w_traffic = ifelse(is.na(clc), NA, w_traffic),
                  
                  w_urban = ifelse(is.na(w_urban), 0, w_urban),
                  w_urban = w_urban/urban_q99,
                  w_urban = ifelse(w_urban >= 1.0, 1.0, w_urban),
                  w_urban = ifelse(is.na(clc), NA, w_urban))

write_stars(w, "supplementary/static/merge_weights/traffic_weights_1km.tif", layer = "w_traffic")
write_stars(w, "supplementary/static/merge_weights/urban_weights_1km.tif", layer = "w_urban")

hist(w$w_traffic)
hist(w$w_urban)

plot(w["w_traffic"], breaks="equal")
plot(w["w_urban"], breaks="equal")
