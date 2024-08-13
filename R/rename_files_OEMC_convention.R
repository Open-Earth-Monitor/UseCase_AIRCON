# rename AQ data according to OEMC file naming convention

# hourly
fls = list.files("AQ_data/agg/airquality.no2.o3.pm10.pm2p5_1.hourly_pnt_20150101_20231231_eu_epsg.3035_v20240718", recursive = T, full.names = T)
new = paste0(dirname(fls), "/airquality.no2.o3.so2.pm10.pm2p5_hourly_pnt_20150101_20231231_", tolower(stringi::stri_sub(dirname(fls),-2,-1)), "_epsg.3035_v20240718.parquet")
file.rename(fls, new)


# daily
fls = list.files("AQ_data/agg/airquality.no2.o3.pm10.pm2p5_2.daily_pnt_20150101_20231231_eu_epsg.3035_v20240718/", recursive = T, full.names = T)
new = paste0(dirname(fls), "/airquality.no2.o3.so2.pm10.pm2p5_daily_pnt_20150101_20231231_", tolower(stringi::stri_sub(dirname(fls),-2,-1)), "_epsg.3035_v20240718.parquet")
file.rename(fls, new)

# monthly
file.rename("AQ_data/agg/airquality.no2.o3.pm10.pm2p5_3.monthly_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet",
            "AQ_data/agg/airquality.no2.o3.so2.pm10.pm2p5_3.monthly_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet")

# annual
file.rename("AQ_data/agg/airquality.no2.o3.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet",
            "AQ_data/agg/airquality.no2.o3.so2.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet")
