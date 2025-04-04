#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --partition=express
#SBATCH --time=00:25:00
#SBATCH --account=uni

#SBATCH --job-name=load_arr
#SBATCH --output=test_arrow_gstat_%j.out

module load palma/2023b

module load foss
module load GCC
module load OpenSSL
module load OpenMPI
module load GDAL/3.9.0
module load R
module load arrow-R

# run R script file
Rscript oemc_aq/test2.R
