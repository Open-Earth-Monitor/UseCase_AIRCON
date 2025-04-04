#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --mem=64G
#SBATCH --partition=normal
#SBATCH --time=24:00:00
#SBATCH --account=uni
#SBATCH --output=PM2p5_day_%j.out

#SBATCH --job-name=PM2p5_day

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
Rscript oemc_aq/AQ_interpolation_loop_daily_palma.R "PM2.5" "mean" 6