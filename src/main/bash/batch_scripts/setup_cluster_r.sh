#!/bin/bash

#
# Expect one argument:
#  - File describing machines
#
# We assume that the read counts are already on the machine.
#

machine_names=$1

set -x

# load machine names in from a file
read -a machines < machines.txt

# ssh files over to the new machine
counter=0
for machine in ${machines[@]}
do

    # ssh files over
    scp -i ~/.ec2/dev.pem probes_to_genes setup_and_run_r.sh ubuntu@${machine}:~
done

