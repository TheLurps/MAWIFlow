#!/bin/bash
#SBATCH --job-name=mawiflow-combine-flows-with-annotations
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small
#SBATCH --time=71:30:00
#SBATCH --output=logs/combine-flows-with-annotations/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

source .venv/bin/activate

while read -r line; do
    # remove leading '-' and whitespace
    DIR="${line#*- }"
    uv run python -m mawiflow.flows \
        -a $HOME/mawiflow/data/annotations/$DIR \
        -f $HOME/mawiflow/data/cicflowmeter/$DIR \
        -o $HOME/mawiflow/data/flows/$DIR
done < <(grep -E '^\s*-' $HOME/mawiflow/params.yaml)
