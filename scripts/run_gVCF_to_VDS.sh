#!/bin/bash

echo "Job started at $(date)"

# === Get and validate input argument ===
# SET_VARIABLES=/rds/general/project/lms-ware-analysis/live/gjp/ImperialCardioGenetics/Hail/set_variables.sh

if [[ -z "$SET_VARIABLES" ]]; then
    echo "❌ Error: No set_variables.sh path provided."
    echo "Usage: $0 path/to/set_variables.sh"
    exit 1
fi

if [[ ! -f "$SET_VARIABLES" ]]; then
    echo "❌ Error: File not found: $SET_VARIABLES"
    exit 1
fi

# === Source the provided variable file ===
source "$SET_VARIABLES" || { echo "❌ Failed to source $SET_VARIABLES"; exit 1; }
echo "✅ Successfully sourced: $SET_VARIABLES"

# ---------------------------------------------
# === Activate Hail conda environment ===
# ---------------------------------------------

echo "Activating Hail conda environment."
eval "$(${CONDA_PATH} shell.bash hook)"
conda activate hail

# ---------------------------------------------
# === Change to working directory ===
# ---------------------------------------------
cd "${HAIL_REPO}" || exit 1

# ---------------------------------------------
# === Script Execution ===
# ---------------------------------------------

echo "Starting Hail gVCF-to-VDS combiner sctipt at $(date)"

# Run the Hail gVCF to VDS combiner script
python "${HAIL_REPO}/scripts/gVCF_to_VDS.py" \
  --threads "$THREADS" \
  --mem "$MEMORY" \
  --branch-factor "$BRANCH_FACTOR" \
  --batch-size "$BATCH_SIZE" \
  --target-records "$TARGET_RECORDS" \
  --log-path "$LOG_HAIL" \
  --combiner-tmp "$TMP_DIR" \
  --gvcf-list "$GVCF_LIST" \
  --output-vds "$OUTPUT_VDS"

echo "Job finished at: $(date)"