setwd("/palma/scratch/tmp/jheisig/aq")
library(dplyr)

vrsn = "v20250306"  

check_map_progress = function(parent.dir){
  cog.dirs = grep("cog$", list.dirs(parent.dir, recursive = T), value = T)
  polls = purrr::map(stringi::stri_split(cog.dirs, regex = "\\/"), 4) |> unlist()
  
  cog.files = purrr::map(cog.dirs, function(x) grep("pse", list.files(x, recursive = T, full.names = T), 
                                                    invert = T, value = T)) |> 
    unlist() |> as.data.frame() |> setNames("path")
  
  cog.files$poll = stringi::stri_split(cog.files$path, regex = "\\/") |> purrr::map(4) |> unlist()
  cog.files$freq = stringi::stri_split(cog.files$path, regex = "\\/") |> purrr::map(3) |> unlist()
  time = purrr::map(stringi::stri_split(cog.files$path, regex = "\\_"), grep, pattern="^20.*", value = T)
  cog.files$start = purrr::map(time, 1) |> unlist()
  cog.files$stop = purrr::map(time, 2) |> unlist()
  #cog.files
  
  tab = table(cog.files$poll, cog.files$start) |> t() |> as.data.frame()
  tab$y = as.numeric(substr(tab$Var1,1,4))
  tab$m = as.numeric(substr(tab$Var1,5,6))
  tab$d = as.numeric(substr(tab$Var1,7,8))
  
  tab = group_by(tab, Var2, y) |> 
    summarise(n=sum(Freq), .groups = "drop") |> 
    tidyr::pivot_wider(values_from = n, names_from = Var2)
  
  tab
  
}

(a = check_map_progress(file.path("AQ_maps", vrsn, "annual")))

(m = check_map_progress(file.path("AQ_maps", vrsn, "monthly")))

(dd = check_map_progress(file.path("AQ_maps", vrsn, "daily")))

