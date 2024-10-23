# Tutorial for doing MEDS Tab evaluation from scratch

## Creating a MEDS version of your dataset

export N_PARALLEL_WORKERS=10
export RESHARD_DIR=/dhc/home/robin.vandewater/datasets/AUMC_MEDS_RESHARD
export AUMC_MEDS_DIR=/dhc/home/robin.vandewater/datasets/AUMCdb_1.0.2_MEDS
export TASKS_DIR=/dhc/home/robin.vandewater/projects/MEDS_TAB_MIMIC_IV/tasks
export OUTPUT_TABULARIZATION_DIR=/dhc/home/robin.vandewater/datasets/AUMCdb_1.0.2_MEDS_TAB/TAB
export OUTPUT_MODEL_DIR=/dhc/home/robin.vandewater/datasets/AUMCdb_1.0.2_MEDS_TAB/MODEL_DIR

```bash
bash GENERAL_TUTORIAL/tabularize_meds.sh "${AUMC_MEDS_DIR}" "$RESHARD_DIR" "$OUTPUT_TABULARIZATION_DIR" \
"first_24h_icu_aumcdb" "$TASKS_DIR" "$OUTPUT_MODEL_DIR" "$N_PARALLEL_WORKERS" \
"tabularization.aggs=[static/present,code/count,value/count,value/sum,value/sum_sqd,value/min,value/max]" \
"tabularization.window_sizes=[2h,12h,1d,7d,30d,365d,full]" \
++hydra.callbacks.profiler._target_=hydra_profiler.profiler.ProfilerCallback \
hydra/launcher=submitit_slurm \
hydra.launcher.timeout_min=300 \
hydra.launcher.cpus_per_task=4 \
hydra.launcher.mem_gb=100 \
hydra.launcher.partition="cpu"
```
