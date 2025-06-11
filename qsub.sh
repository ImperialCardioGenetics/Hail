#!/bin/bash
#PBS -N gVCF_to_VDS
#PBS -l select=1:ncpus=20:mem=140gb
#PBS -l walltime=24:00:00
#PBS -o /rds/general/project/lms-ware-analysis/live/riyad/BGE/Rare/gVCF_to_VDS.out
#PBS -e /rds/general/project/lms-ware-analysis/live/riyad/BGE/Rare/gVCF_to_VDS.err

# 1) Change to the working directory
cd /rds/general/project/lms-ware-analysis/live/riyad/Hail

# 2) Activate your Hail Conda environment
eval "$(~/miniforge3/bin/conda shell.bash hook)"
conda activate hail

python /rds/general/project/lms-ware-analysis/live/riyad/Hail/gVCF_to_VDS.py

