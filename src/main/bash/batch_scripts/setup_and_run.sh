#!/bin/bash

# make filesystem
sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /mnt1
sudo mount /dev/xvdb /mnt1
sudo chmod ugoa+rw /mnt1/

# install dependencies
sudo apt-get install openjdk-7-jre r-base emacs23 s3cmd -y

# cd to temp drive
cd /mnt1

# make call directory
mkdir calls

# get run files
s3cmd get s3://wfc_cnv/target.tar.gz 

# extract files
tar xzvf target.tar.gz 

# load samples
read -a samples < ~/samples.txt

# set java opts
export JAVA_OPTS="-Xmx100g -Dspark.local.dir=/mnt1/"

# start/end samples
first_sample=${samples[0]}
last_sample="" # we overwrite this each time we process a sample

# loop over samples and call
for sample in ${samples[@]}
do
    # get sample
    s3cmd get s3://color_aligned_bamfiles/$sample

    # move sample
    mv $sample ${sample}.bam

    # call
    time sh ./target/appassembler/bin/cnv ~/probes.txt calls/ ${sample}.bam -skipAnalysis -partitions 48 -emitCSV

    # set sample name...
    last_sample=$sample
done

# archive name
archive_name=$(echo "calls_${first_sample}-${last_sample}.tar.gz" | sed "s/WFC//g")

# tar calls and upload to s3 for posterity
tar czvf ${archive_name} calls
s3cmd put ${archive_name} s3://wfc_cnv/