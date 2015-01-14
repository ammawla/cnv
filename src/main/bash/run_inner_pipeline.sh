#!/bin/bash

set -x

# install r from packages
wget http://watson.nci.nih.gov/cran_mirror/bin/linux/ubuntu/precise/r-base-core_3.0.3-1precise0_amd64.deb
sudo dpkg -i r-base-core_3.0.3-1precise0_amd64.deb
sudo apt-get -f install -y

# make r directory, and chmod it
sudo mkdir -p /usr/local/lib/R/site-library
sudo chmod ugoa+rw /usr/local/lib/R/site-library

#
# Runs CNV pipeline:
# - invokes local spark job to convert a single BAM file into coverage counts
# - transforms intermediate files into format expected by the r scripts
# - runs r scripts on this sample and the median set
#

# arguments
# sample name
SAMPLE_NAME=$1

# parameters
# java memory settings - consume 60gb mem, should be 90% of machine memory
if [ $# -lt 2 ]; then
    JAVA_MEM="-Xmx${2}g"
else
    JAVA_MEM="-Xmx60g"
fi

# spark partition count - coalesce out to 24 partitions
# recommend 3 partitions per core for optimal load balancing/overhead tradeoff
if [ $# -lt 3 ]; then
    PARTITION_COUNT=24
else
    PARTITION_COUNT=$3
fi

# cnv executable version - currently running 0.0.1
CNV_VERSION=0.0.1

# implementation
# move into spark frontend area
cd cnv

# make calls directory
mkdir calls

# execute spark job
setenv JAVA_OPTS="$JAVA_MEM"
sh ./target/appassembler/bin/cnv \
probes.txt \
calls \
${SAMPLE_NAME}.bam \
-skipAnalysis \
-emitCSV \
-coalesce $PARTITION_COUNT 

# get gene data from csv in r directory
genes=$(cat probes_to_genes | sed "s/ /\n/g" | awk -F',' '{ print $2 }' | sort | uniq | grep -v other)
read -a mappings < ./probes_to_genes

# write headers for gene files
for gene in ${genes[@]}
do
    echo -e "ChrID\tPosition\tCoverage" > r/ExperimentInput/coverage/${SAMPLE_NAME}_${gene}.depth
done
    
# move sample
for mapping in ${mappings[@]}
do
# get partition index and gene
    idx=$(echo "${mapping}" | awk -F',' '{ print $1 }')
    gene=$(echo "${mapping}" | awk -F',' '{ print $2 }')
 
 # cat file on to coverage
    cat calls/${SAMPLE_NAME}.csv/part-${idx} >> r/ExperimentInput/coverage/${SAMPLE_NAME}_${gene}.depth
done

# add sample name to file
echo "\"${SAMPLE_NAME}\"" >> r/ExperimentInput/SampleInformation.csv

# cd into r directory, and run
cd r

# run R script - we run the parameters file, not the functions file
# this is documented & explained in the R code itself
R CMD BATCH Scripts/DoC_parameters.R

# cd up to top, and stage results
cd ..

mkdir results

mv r/ExperimentInput/calls/${SAMPLE_NAME}* results
cp r/DoC_parameters.Rout results
cp *.log results

# tar results up
tar czvf results-${SAMPLE_NAME}.tar.gz results