#!/bin/bash

#
# Expect two arguments:
#  - File describing machines
#  - File describing sample names
#

machine_names=$1
sample_names=$2

set -x

# load machine names in from a file
read -a machines < ${machine_names}

# find sample count
samples=$(wc -w < ${sample_names})
num_machines=${#machines[@]}
div_size=$((samples/num_machines))

# split files - dist is a somewhat randomly chosen prefix --> distributed
split -l $div_size ${sample_names} dist

# get split files
split_files=(dist*)

# per file, change to remove newlines, and ssh files over to the new machine
counter=0
for file in ${split_files[@]}
do
    # remove newlines
    tr '\n' ' ' < $file > new_${file}

    # ssh files over
    scp -i ~/.ec2/dev.pem .s3cfg setup_and_run.sh probes.txt ubuntu@${machines[$counter]}:~
    scp -i ~/.ec2/dev.pem new_${file} ubuntu@${machines[$counter]}:~/samples.txt

    # increment count
    ((counter++))
done

