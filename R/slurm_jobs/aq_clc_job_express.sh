#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=24
#SBATCH --partition=normal
#SBATCH --time=8:00:00
#SBATCH --account=uni
#SBATCH --output=clc_normal_%j.out

#SBATCH --job-name=clc_7

module load palma/2021a
module load foss
module load GDAL
module load R

# run R script file
Rscript oemc_aq/aggregate_CLC.R 7