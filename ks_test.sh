#!/bin/bash

uv venv --no-project --allow-existing --python 3.11 .venv-ks-test
source .venv-ks-test/bin/activate
uv sync --active --script ks_test.py

for YEAR in {2007..2024}; do
    sbatch _ks_test.sh \
        --reference-year $YEAR \
        --label-wise \
        --output results/ks_test/$YEAR.json \
        --verbose;
done;
