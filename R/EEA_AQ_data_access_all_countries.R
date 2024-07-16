library(dplyr)
library(pbmcapply)
library(arrow)
source("R/functions.R")

station_meta = read_parquet("AQ_stations/EEA_stations_meta_SamplingPoint.parquet")
countries = unique(station_meta$Countrycode) |> as.character() |> sort()
pollutants = c(7, 8, 5, 6001)
datasets = c(1,2)
dl_dir = "download"


pq_files = station_data_urls(country = countries, 
                             pollutant = pollutants, 
                             dataset = datasets) |> 
  download_station_data(dir = dl_dir, 
                        cores = 8)


# read, filter, join pollutants for 2015-2023
prep_dir = "AQ_data/01_hourly"

preprocess_station_data(dir = dl_dir, 
                        out_dir = prep_dir, 
                        station_meta = station_meta, 
                        keep_validity = 1, 
                        keep_verification = c(1,2), 
                        progress = F)


prep_data = arrow::open_dataset(prep_dir)

list.files(prep_dir)
