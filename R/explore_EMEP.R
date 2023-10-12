setwd("~/OEMC/UseCase_AIRCON")
library(stars)

emep = read_stars("EMEP01_rv5.0_year.2022met_2021emis.nc")
emep = st_set_crs(emep, 4326)
plot(emep["SURF_ppb_NO2"])
plot(emep["SURF_ugN_NOX"])
plot(emep["SURF_ug_PM10"])
plot(emep["SURF_ug_PM25"])
