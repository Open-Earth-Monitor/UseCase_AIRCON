#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=72
#SBATCH --partition=bigsmp
#SBATCH --time=03:20:00
#SBATCH --account=uni

#SBATCH --job-name=aq_interpolation
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jheisig@uni-muenster.de

module load palma/2021a
module load foss
module load GDAL
module load R

# run R script file
Rscript oemc_aq/AQ_interpolation_loop_palma.R