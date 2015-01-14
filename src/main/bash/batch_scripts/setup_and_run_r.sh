#!/bin/bash

set -x

# cd to temp drive
cd /mnt1

# remove r files if they already exist
rm -rf r r.tar.gz

# get run files
s3cmd get s3://wfc_cnv/r.tar.gz 

# extract files
tar xzvf r.tar.gz 

# load samples
read -a samples < ~/samples.txt

# load probe/gene mappings
read -a mappings < ~/probes_to_genes

# get genes
genes=$(cat probes_to_genes | awk -F',' '{ print $2 }' | sort | uniq | grep -v other)

# start/end samples
first_sample=${samples[0]}
last_sample="" # we overwrite this each time we process a sample

# loop over samples and move files into r directories
for sample in ${samples[@]}
do
    # write headers for gene files
    for gene in ${genes[@]}
    do
	echo "ChrID\tPosition\tCoverage" > /mnt1/r/ExperimentInput/coverage/${sample}_${gene}.depth
    done
    
    # move sample
    for mapping in ${mappings[@]}
    do
	# get partition index and gene
	idx=$(echo "${mapping}" | awk -F',' '{ print $1 }')
	gene=$(echo "${mapping}" | awk -F',' '{ print $2 }')

	# cat file on to coverage
	cat calls/${sample}.csv/part-${idx} >> /mnt1/r/ExperimentInput/coverage/${sample}_${gene}.depth
    done

    # add sample to list
    echo "\"${sample}\"" >> /mnt1/r/ExperimentInput/SampleInformation.csv

    # set sample name...
    last_sample=$sample
done

# move into r directory
cd r

# open up r install directory
sudo chmod ugoa+rw /usr/local/lib/R/site-library

# run r
R CMD BATCH Scripts/DoC_parameters.R

# cd down
cd ..

# make packaging directory
mkdir r_output

# copy r outputs into packaging directory
cp /mnt1/r/ExperimentInput/PDFs/* /mnt1/r/ExperimentInput/calls/* /mnt1/r/ExperimentInput/bedgraph/* r_output

# archive name
archive_name=$(echo "r_output_${first_sample}-${last_sample}.tar.gz" | sed "s/WFC//g")

# tar calls and upload to s3 for posterity
tar czvf ${archive_name} r_output
s3cmd put ${archive_name} s3://wfc_cnv/
