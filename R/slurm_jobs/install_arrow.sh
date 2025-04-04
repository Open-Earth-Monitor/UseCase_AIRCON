#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=express
#SBATCH --time=01:30:00
#SBATCH --account=uni

#SBATCH --job-name=install
#SBATCH --output=install2_%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jheisig@uni-muenster.de

#module load palma/2022a

#module load foss
#module load GCC/11.3.0
#module load OpenSSL/3
#module load pkg-config 
#module load OpenMPI/4.1.4
#module load GDAL/3.5.0
#module load R/4.2.1

module load palma/2022b

module load GCC/12.2.0
module load OpenMPI/4.1.4
module load GDAL/3.6.2
module load pkg-config 
module load arrow-R/11.0.0.3-R-4.2.2
module load R

# run R script file
Rscript oemc_aq/install_arrow.R