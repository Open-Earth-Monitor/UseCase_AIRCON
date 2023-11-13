library(dplyr)
library(pbmcapply)
library(data.table)
library(dtplyr)
source("R/functions.R")

station_meta = arrow::read_parquet("AQ_stations/EEA_stations_meta.parquet")

countries = unique(station_meta$Countrycode) |> as.character() |> sort()
pollutants = c(7,8,5,6001)

dl_file = "download_urls_all_countries.txt" 

# generate_download_urls(countries, pollutants, 2015, 2023, file = "dl_file)
# check_station_urls(dl_file)
files = download_station_data(dl_file) # 2 errors in 87342 files


# read, filter, join pollutants for 2015-2023
measurement_counts = mclapply(countries, preprocess_AQ_by_country, 
                              dl_files = files, outdir = "AQ_data/01_hourly_gaps", 
                              mc.cores = 4)









