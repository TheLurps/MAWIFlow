#!/bin/bash
#SBATCH --job-name=mawiflow-generate-annotations
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small
#SBATCH --time=71:30:00
#SBATCH --output=logs/generate-annotations/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

source .venv/bin/activate

while read -r line; do
    # remove leading '-' and whitespace
    DIR="${line#*- }"
    uv run python -m mawiflow.annotations -vv \
        -r $HOME/mawiflow/data/raw/$DIR \
        -o $HOME/mawiflow/data/annotations/$DIR
done < <(grep -E '^\s*-' $HOME/mawiflow/params.yaml)
