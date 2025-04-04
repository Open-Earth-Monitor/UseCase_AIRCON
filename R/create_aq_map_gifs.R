setwd("/palma/scratch/tmp/jheisig/aq/AQ_maps/v20250306/")


pollutants = c("PM2.5_mean","PM10_perc", "PM10_mean", "O3_perc", "NO2_mean")
time = "monthly"

td = file.path(tempdir(), "gifs")
dir.create(td)


for (p in pollutants){
  dir = file.path(time, p, "pdf/merged")
  l = list.files(dir, paste0("^", p), full.names = T, recursive = T)
  for (i in l) {
    cl = paste0("pdftoppm -r 250 -png ", i," > ", file.path(td, sub(".pdf",".png", basename(i))))
    system(cl)
    cat(".")
  }
  cl2 = paste0("ffmpeg -framerate 4 -pattern_type glob -i '", td, "/", p, "*.png' ", td, "/", p, "_", time, ".gif")
  system(cl2)
  message(p)
}


paste0("cp ", td, "/*.gif ", "~/R/UseCase_AIRCON/AQ_maps/gifs") |> system()
