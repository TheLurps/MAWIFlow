#!/bin/bash
#SBATCH --job-name=mawiflow-ks-test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small
#SBATCH --time=24:00:00
#SBATCH --output=logs/ks-test/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

# Strict shell: E (ERR trap), e (exit on error), u (undef vars), f (noglob), pipefail, x (trace)
set -Eeuf -o pipefail -x

source .venv-ks-test/bin/activate
uv run --offline --script ks_test.py $@
