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

python gVCF_to_VDS.py \
  --threads 20 \
  --mem 16g \
  --branch-factor 15 \
  --batch-size 10 \
  --target-records 1000000 \
  --log-path /rds/general/project/lms-ware-analysis/ephemeral/HAIL/run1_hail.log \
  --combiner-tmp /rds/general/project/lms-ware-analysis/ephemeral/HAIL/run1_test_combiner_tmp \
  --gvcf-list /rds/general/project/lms-ware-raw/live/internal/sequencing/mgb-SHaRe-Registry/data/Hail/run1_300.txt \
  --output-vds /rds/general/project/lms-ware-raw/live/internal/sequencing/mgb-SHaRe-Registry/data/Hail/run1_300.vds \
  --sample-qc-out /rds/general/project/lms-ware-raw/live/internal/sequencing/mgb-SHaRe-Registry/data/Hail/run1_300_sample_qc.ht


