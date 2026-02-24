#!/bin/bash
#SBATCH --job-name=mawiflow-boxplot-stats
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small
#SBATCH --time=71:00:00
#SBATCH --output=logs/boxplot_stats/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

# Strict shell: E (ERR trap), e (exit on error), u (undef vars), f (noglob), pipefail, x (trace)
set -Eeuf -o pipefail -x

source .venv/bin/activate
uv run --offline boxplot_stats.py
