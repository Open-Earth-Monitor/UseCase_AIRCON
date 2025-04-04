#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=10
#SBATCH --mem=92G
#SBATCH --partition=normal
#SBATCH --time=24:00:00
#SBATCH --account=uni
#SBATCH --output=PM10_perc_mon_%j.out

#SBATCH --job-name=PM10_perc_mon

module load palma/2023b

module load foss
module load GCC
module load OpenSSL
module load OpenMPI
module load netCDF
module load GDAL/3.9.0
module load R
module load arrow-R

# run R script file
#Rscript oemc_aq/debug_AQ_interpolation_loop.R "PM10" "perc"
Rscript oemc_aq/AQ_interpolation_loop_palma.R "PM10" "perc" 8