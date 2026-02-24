#!/bin/bash
#SBATCH --job-name=mawiflow-generate-flows
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=small_fat
#SBATCH --time=06:00:00
#SBATCH --output=logs/generate-flows/%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --mail-user=joshua.schraven@hsu-hh.de

set -xeu

# create tmp dir for pcaps
mkdir -p $SLURM_TMPDIR/$1

# uncompress pcaps
for src in $HOME/mawiflow/data/raw/$1/*.gz; do
	out="$SLURM_TMPDIR/$1/$(basename "${src%.gz}")";
	gunzip -c "$src" > "$out";
done

# create output dir
mkdir -p $HOME/mawiflow/data/cicflowmeter/$1

# run the container
FREE_GB=896
BLOCK_SIZE=64kb
apptainer run \
	--env CFM_OPTS="\
	-Xms8g -Xmx${FREE_GB}g \
	-XX:MaxDirectMemorySize=${FREE_GB}g \
	-Dnio.mx=${FREE_GB}gb \
	-Dnio.ms=${FREE_GB}gb \
	-Dnio.blocksize=${BLOCK_SIZE}" \
	--contain \
	--no-home \
	--pwd /CICFlowMeter/bin \
	--bind $SLURM_TMPDIR/$1:/pcap \
	--bind $HOME/mawiflow/data/cicflowmeter/$1:/output \
	$HOME/mawiflow/cicflowmeter.sif \
	/pcap /output
