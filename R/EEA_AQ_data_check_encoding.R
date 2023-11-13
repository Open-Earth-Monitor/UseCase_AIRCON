library(pbmcapply)
library(dplyr)

get_encoding = function(file){
  e = readr::guess_encoding(file)
  enc = e$encoding[which.max(e$confidence)]
  return(c(file = file, encoding = enc))
}
enc_file = "download_files_wrong_encoding.txt"
enc_list = readLines(enc_file)

files = list.files("download", full.names = T)

enc = pbmclapply(files, get_encoding, mc.cores = 16)
enc_bad = do.call(rbind, enc) |> 
  as.data.frame() |> 
  dplyr::filter(encoding != "UTF-8")

enc_file_update = unique(c(enc_list, enc_bad$file))

writeLines(enc_file_update, enc_file)

print(enc_bad)
message("Number of wrongly encoded files BEFORE: ", length(enc_list))
message("Number of wrongly encoded files AFTER: ", length(enc_bad$file))