#!/usr/bin/env bash

set -e

# Function to print help message
print_help() {
    echo "Usage: $0 <GENERIC_MEDS_DIR> <GENERIC_MEDS_RESHARD_DIR> <OUTPUT_TABULARIZATION_DIR> <TASKS> <TASKS_DIR> <OUTPUT_MODEL_DIR> <N_PARALLEL_WORKERS> [additional arguments]"
    echo
    echo "Arguments:"
    echo "  GENERIC_MEDS_DIR            Directory containing MIMIC-IV medications data"
    echo "  GENERIC_MEDS_RESHARD_DIR    Directory for resharded MIMIC-IV medications data"
    echo "  OUTPUT_TABULARIZATION_DIR   Output directory for tabularized data"
    echo "  TASKS                       Comma-separated list of tasks to run (e.g., 'long_los,icu_mortality')"
    echo "  TASKS_DIR                   Directory containing task-specific data"
    echo "  OUTPUT_MODEL_DIR            Output directory for models"
    echo "  N_PARALLEL_WORKERS          Number of parallel workers to use"
    echo
    echo "Additional arguments will be passed to the underlying commands."
}

# Check for help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    print_help
    exit 0
fi

# Check if we have the minimum required number of arguments
if [ "$#" -lt 7 ]; then
    echo "Error: Not enough arguments provided."
    print_help
    exit 1
fi

# Assign arguments to variables
GENERIC_MEDS_DIR="$1"
GENERIC_MEDS_RESHARD_DIR="$2"
OUTPUT_TABULARIZATION_DIR="$3"
TASKS="$4"
TASKS_DIR="$5"
OUTPUT_MODEL_DIR="$6"
N_PARALLEL_WORKERS="$7"
SUBJECTS_PER_SHARD=1000
shift 7

# Split the TASKS string into an array
IFS=',' read -ra TASK_ARRAY <<< "$TASKS"

# Print input arguments
echo "Input arguments:"
echo "GENERIC_MEDS_DIR: $GENERIC_MEDS_DIR"
echo "GENERIC_MEDS_RESHARD_DIR: $GENERIC_MEDS_RESHARD_DIR"
echo "OUTPUT_TABULARIZATION_DIR: $OUTPUT_TABULARIZATION_DIR"
echo "TASKS:" "${TASK_ARRAY[@]}"
echo "TASKS_DIR: $TASKS_DIR"
echo "OUTPUT_MODEL_DIR: $OUTPUT_MODEL_DIR"
echo "N_PARALLEL_WORKERS: $N_PARALLEL_WORKERS"
echo "Additional arguments:" "$@"
echo

#Reshard the data
echo "Resharding data"
# MEDS_transform-reshard_to_split \
#   --multirun \
#   worker="range(0,6)" \
#   hydra/launcher=joblib \
#   input_dir="$GENERIC_MEDS_DIR" \
#   cohort_dir="$GENERIC_MEDS_RESHARD_DIR" \
#   'stages=["reshard_to_split"]' \
#   stage="reshard_to_split" \
#   stage_configs.reshard_to_split.n_subjects_per_shard=1000 \
#   "polling_time=5"

#describe codes
# echo "Describing codes"
# meds-tab-describe \
#     "input_dir=${GENERIC_MEDS_RESHARD_DIR}/data" "output_dir=$OUTPUT_TABULARIZATION_DIR"

# echo "Tabularizing static data"
# meds-tab-tabularize-static \
#     "input_dir=${GENERIC_MEDS_RESHARD_DIR}/data" "output_dir=$OUTPUT_TABULARIZATION_DIR" \
#     do_overwrite=False "$@"

meds-tab-tabularize-time-series \
    --multirun \
    worker="range(0,$N_PARALLEL_WORKERS)" \
    hydra/launcher=joblib \
    "input_dir=${GENERIC_MEDS_RESHARD_DIR}/data" "output_dir=$OUTPUT_TABULARIZATION_DIR" \
    do_overwrite=False "$@"

for TASK in "${TASK_ARRAY[@]}"
do
    echo "Running task_specific_caching.py for task: $TASK"
    meds-tab-cache-task \
    hydra/launcher=joblib \
    "input_dir=${GENERIC_MEDS_RESHARD_DIR}/data" "output_dir=$OUTPUT_TABULARIZATION_DIR" \
    "input_label_dir=${TASKS_DIR}/${TASK}/" "task_name=${TASK}" do_overwrite=False "$@"

  echo "Running xgboost for task: $TASK"
  meds-tab-xgboost \
      --multirun \
      worker="range(0,$N_PARALLEL_WORKERS)" \
      "input_dir=${GENERIC_MEDS_RESHARD_DIR}/data" "output_dir=$OUTPUT_TABULARIZATION_DIR" \
      "output_model_dir=${OUTPUT_MODEL_DIR}/${TASK}/" "task_name=$TASK" do_overwrite=False \
      "hydra.sweeper.n_trials=1000" "hydra.sweeper.n_jobs=${N_PARALLEL_WORKERS}" \
      "$@"
done
