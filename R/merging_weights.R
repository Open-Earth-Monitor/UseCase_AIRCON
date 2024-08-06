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
                  w_traffic = ifelse(w_traffic > 1, 1, w_traffic),
                  w_traffic = w_traffic / 2,                                     # Horalek et al. 2019
                  
                  # w_urban = ifelse(is.na(w_urban), 0, w_urban),
                  # w_urban = w_urban/urban_q99,
                  # w_urban = ifelse(w_urban >= 1.0, 1.0, w_urban),
                  
                  w_lookup = w_urban,
                  w_urban = (w_urban - 100) / 400,                              # Horalek et al. 2019
                  w_urban = ifelse(w_lookup <= 100, 0,  
                                   ifelse(w_lookup >= 500, 1, w_urban)),
                  w_urban = ifelse(is.na(clc), NA, w_urban)
                  )

write_stars(w, "supplementary/static/merge_weights/traffic_weights_1km.tif", layer = "w_traffic")
write_stars(w, "supplementary/static/merge_weights/urban_weights_1km.tif", layer = "w_urban")

hist(w$w_traffic)
hist(w$w_urban)


# plots

w = read_stars(c("supplementary/static/merge_weights/traffic_weights_1km.tif",
                 "supplementary/static/merge_weights/urban_weights_1km.tif")) |> 
  setNames(c("Traffic Exposure", "Urban Character"))

bb = st_bbox(c(xmin = 2500000,ymin = 1400000,xmax = 7400000 ,ymax = 5500000), crs = st_crs(w)) |> 
  st_as_sfc()

w = st_crop(w, bb)

plot(w[1], breaks="equal")
plot(w[2], breaks="equal")

hist(w$`Traffic Exposure`)
plot(w["Traffic Exposure"], col = c(RColorBrewer::brewer.pal(3, "Blues"), "royalblue4"), 
     breaks = c(0,.01,.05,.09,.4), reset=F)
plot(giscoR::gisco_countries$geometry |> sf::st_transform(sf::st_crs(w)), add=T)

hist(w$`Urban Character`)
plot(w["Urban Character"], col = c(RColorBrewer::brewer.pal(4, "Reds")), 
     breaks = c(0,.01,.1,.2,1.00001), reset=F)
plot(giscoR::gisco_countries$geometry |> sf::st_transform(sf::st_crs(w)), add=T)
