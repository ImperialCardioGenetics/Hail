#!/bin/bash

# ---------------------------------------------
# === Set Variables (Edit These) ===
# ---------------------------------------------

# Hail repo (the location of the Hail repository)
HAIL_REPO=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail

# Prefix -- unique identifier used in all outputs and logs
PREFIX=share_test20

# Hail execution parameters
THREADS=10                    # Number of threads to use in Hail
MEMORY=64g                    # Memory total (all CPUs) end with g
BRANCH_FACTOR=15              # Tree fan-out during combine steps
BATCH_SIZE=10                 # Number of gVCFs per merge job
TARGET_RECORDS=1000000        # Target number of records per partition

# Qsub parameters
NCPU=10                       # Number of CPUs
QSUB_MEMORY=64gb              # Memory total (all CPUs) end with gb
WALL_TIME=12:00:00            # Wall time

# TMP directory path (NB must be in /rds/general/project/lms-ware-analysis/ephemeral/)
TMP_DIR=/rds/general/project/lms-ware-analysis/ephemeral/HAIL/${PREFIX}

# Hail logs path
LOG_HAIL=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail/logs/${PREFIX}.log  

# Qsub logs directory
LOG_QSUB=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail/logs/

# File with list of gVCF paths
GVCF_LIST=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail/sandbox/${PREFIX}.txt

# Output VDS path
OUTPUT_VDS=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail/sandbox/${PREFIX}.vds

# Conda path
CONDA_PATH=~/anaconda3/bin/conda

# ---------------------------------------------
# === Checks (Confirm paths and files exist -- DO NOT EDIT!) ===
# ---------------------------------------------

# Function to check file existence
check_file() {
  [[ -f "$1" ]] || { echo "❌ File not found: $1" >&2; exit 1; }
}

# Function to check directory existence
check_dir() {
  [[ -d "$1" ]] || { echo "❌ Directory not found: $1" >&2; exit 1; }
}

# Run checks
echo "🔍 Checking input paths..."
check_dir "$HAIL_REPO"
check_dir "$(dirname "$OUTPUT_VDS")"
check_dir "$(dirname "$LOG_HAIL")"
check_dir "$(dirname "$LOG_QSUB")"
check_file "$GVCF_LIST"
check_file "$CONDA_PATH"

# Delete existing temporary directory safely (only if in allowed ephemeral area)
if [[ "$TMP_DIR" == /rds/general/project/lms-ware-analysis/ephemeral/* ]]; then
  if [[ -d "$TMP_DIR" ]]; then
    echo "⚠️ Deleting existing TMP_DIR: $TMP_DIR"
    rm -r "$TMP_DIR"
  else
    echo "ℹ️ TMP_DIR does not exist yet: $TMP_DIR"
  fi
else
  echo "❌ TMP_DIR is outside the allowed location: $TMP_DIR" >&2
  exit 1
fi

echo "✅ All input files and directories exist."