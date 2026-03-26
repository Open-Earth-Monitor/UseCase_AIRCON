setwd("/palma/scratch/tmp/jheisig/aq/AQ_maps/v20250306/")


time = "daily"
if (time == "daily"){
  pollutants = c("PM2.5_mean","PM10_mean", "O3_max", "NO2_mean")
  dpi = 50
  fm = 15
  pdf_dir = "pdf"
} else {
  pollutants = c("PM2.5_mean","PM10_perc", "PM10_mean", "O3_perc", "NO2_mean")
  dpi = 250
  fm = 4
  pdf_dir = "pdf/merged"
}

td = file.path(tempdir(), "gifs")
dir.create(td)


for (p in pollutants){
  dir = file.path(time, p, pdf_dir)
  l = list.files(dir, paste0("^air_quality.", substr(p, 1,3)), full.names = T, recursive = T, ignore.case = T)
  message(length(l))
  for (i in 1:length(l)) {
    cl = paste0("pdftoppm -r ", dpi, " -png ", l[[i]]," > ", file.path(td, sub(".pdf",".png", basename(l[[i]]))))
    system(cl)
    m = ifelse(i %% 100 == 0, paste0(i,"\n"), ".")
    cat(m)
  }
  cl2 = paste0("ffmpeg -framerate ", fm," -pattern_type glob -i '", td, "/*", tolower(substr(p, 1,3)), "*.png' ", td, "/", p, "_", time, ".gif")
  system(cl2)
  paste0("rm ", td, "/*.png") |> system()
  paste0("cp ", td, "/", p, "*.gif ", "~/R/UseCase_AIRCON/AQ_maps/gifs") |> system()
  message(p)
}


