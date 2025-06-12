#!/bin/bash

# ---------------------------------------------
# USAGE: bash submit_gVCF_to_VDS.sh path/to/set_variables.sh
# ---------------------------------------------

# Print the start time
echo -e "Script started on: $(date)\n"

# === Get and validate input argument ===
SET_VARIABLES="$1"
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

# === Submit PBS job ===
/opt/pbs/bin/qsub \
  -N "$PREFIX" \
  -o "${LOG_QSUB}/${PREFIX}.out" \
  -e "${LOG_QSUB}/${PREFIX}.err" \
  -v SET_VARIABLES="$SET_VARIABLES" \
  -l select=1:ncpus="${NCPU}":mem="${QSUB_MEMORY}" \
  -l walltime="${WALL_TIME}" \
  "${HAIL_REPO}/scripts/run_gVCF_to_VDS.sh"

# === Finish ===
echo -e "\n✅ Job submitted. Script completed on: $(date)"

