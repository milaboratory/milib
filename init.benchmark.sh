#!/usr/bin/env bash

set -e
set -o xtrace

export

mkdir -p benchmark-data
cd benchmark-data

#trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

#pids=()

for file in CD4M1_test_R1.fastq.gz CD4M1_test_R2.fastq.gz; do
  if [[ ! -f "$file" ]]; then
    echo "Initiating download of $file"
    #    --silent
    curl --output "$file" --basic --user "$MI_BENCHMARK_DATA_USER:$MI_BENCHMARK_DATA_PASSWORD" "https://${MI_BENCHMARK_DATA_HOST}/$file" # &# Background
    #    pids+=($!)
  fi
done

# Waiting for background curl processes to finish
#for pid in ${pids[*]}; do
#  wait "$pid"
#done

echo "Downlaod finished."
