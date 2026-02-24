#!/bin/bash
#SBATCH --job-name=mawiflow-feature-importance
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small
#SBATCH --time=24:00:00
#SBATCH --output=logs/feature-importance/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

# Strict shell: E (ERR trap), e (exit on error), u (undef vars), f (noglob), pipefail, x (trace)
set -Eeuf -o pipefail -x

YEAR=$1

source .venv/bin/activate
uv run --offline feature_importance.py \
    --input data/samples/v1.1/mawiflow_samples_n1000_seed42_taxonomy_balanced.parquet \
    --year $YEAR \
    --output results/feature_importance/results_$YEAR.json
