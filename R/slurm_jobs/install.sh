#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=express
#SBATCH --time=01:00:00
#SBATCH --account=uni

#SBATCH --job-name=install
#SBATCH --output=install_%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jheisig@uni-muenster.de

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
Rscript oemc_aq/install.R