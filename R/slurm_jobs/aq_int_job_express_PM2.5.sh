#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=32
#SBATCH --partition=express
#SBATCH --time=01:00:00
#SBATCH --account=uni
#SBATCH --output=aq_int_PM2-5_%j.out

#SBATCH --job-name=aq_int_PM2.5

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
Rscript oemc_aq/AQ_interpolation_loop_palma.R "PM2.5" "mean" 