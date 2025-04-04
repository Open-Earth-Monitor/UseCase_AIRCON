#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=6
#SBATCH --mem=92G
#SBATCH --partition=normal
#SBATCH --time=2:00:00
#SBATCH --account=uni
#SBATCH --output=PM2p5_ann_%j.out

#SBATCH --job-name=PM2p5_ann

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
Rscript oemc_aq/AQ_interpolation_loop_annual_palma.R "PM2.5" "mean" 3