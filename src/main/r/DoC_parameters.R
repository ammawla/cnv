### R Source for Targeted CNV calling ###
### ASN 2011-06-01
### Runs invariant set normalization and calls CNVs based on parameters
#
# Notes:
#
# For the analysis to run, a folder with the path and name specified by experiment.directory must be present and contain the coverage files and the input files
# In addition, a folder that contains tha platform information must be present.  If the platform has not been analyzed previously, prep.partition should be set to TRUE
# See notes below about what other files and formats must be present
#
# Update the information within this file for each run, see notes below for details
#
# This file is run using the command line call: R CMB BATCH thisfilename.R
# Alternatively, the call source(thisfilename.R) can be used within R
#
# R must be installed with these bioconductor packages: genomeIntervals, Biostrings, ADaCGH2
# To install these packages, run R and use the command:
# 	source("http://www.bioconductor.org/biocLite.R")
# 	biocLite("genomeIntervals")
#   biocLite("Biostrings")
#   biocLite("ADaCGH2") not currently implemented, so package not necessary
#  
# R must also have the mclust package installed in order to analyze tumor data, to do this run
#	install.packages("mclust")
#		
## Coverage data should be in folder named "coverage" within experiment directory and should be named with format SAMPLENAME_PARTITIONNAME.depth with 
# 	coverage files should be tab-delimited with column headers as "ChrID", "Position", "Coverage"
# 	example:
# 	ChrID	Position	Coverage
# 	chr17	7562349	321
# 	chr17	7562350	325
# 	chr17	7562351	330
# 	chr17	7562352	337
# 	chr17	7562353	343
# 	chr17	7562354	350
#
# All other input files should be in:
#	experiment.directory (sample.filename, known.cnvs)
#	platform.directory (bait.filename, partition.filename)
#	genome.directory (FASTA for each chromosome)
# 	annotation.directory (gene.filename)
# See example files for specific format required for each input file
#
# Variant calling criteria can be changed depending on parameters
#

# install packages
source("http://www.bioconductor.org/biocLite.R")
biocLite("genomeIntervals", ask=False)
biocLite("Biostrings", ask=False)
install.packages("mclust", repos="http://cran.us.r-project.org")

criteria <- list(
  experiment.name = "Example" # name of experiment/batch used for output
### Set file locations (provide full path)
, experiment.directory = normalizePath("./ExperimentInput") # directory where coverage/ratio data is stored 
, platform.directory = normalizePath("./PlatformInput") # directory where platform data is stored
, genome.directory = normalizePath("./UCSC/hg19/sequence") # directory where genome information is stored
, annotation.directory = normalizePath("./UCSC/hg19/annotation") # directory where gene annotation information is stored
### Set input data filenames (must match expected format)
, sample.filename = "SampleInformation.csv" # file with sample information for processing batch in csv format
, bait.filename = "CaptureRegions.bed" # file with information for bait probes in bed format
, partition.filename = "Platform_partitionInformation.csv" # file with data on how genomic coverage data is stored (typically chromosomes)
, knowncnvs.filename = "knowncnvs.csv" # file with all known CNVs for normalization purposes (including large known CNVs should improve normalization)
, gene.filename = "refFlat.txt" # refesq file from UCSC used for plotting (should be in annotation.directory
, source.filename = normalizePath("./Scripts/DoC_functions.R") # source file for functions
### Set criteria for analysis
, prep.partition = TRUE # if TRUE generate genomic data for bed platform, else already present
, generate.median = TRUE # if TRUE, then generate raw median coverage data	
, gene.graphics = FALSE # if partitions represent genes, then TRUE, else FALSE.  If true, gene is plotted in graphical output
, output.bedgraph = TRUE # if TRUE, bedgraphs are written for raw coverage and normalized ratio data  
, output.normalized = TRUE # if TRUE, output data is written for raw and normalized coverage.  
, output.ratio = TRUE # if TRUE, ratio table is written for each partition.  This is useful if another segmentation method is to be used
, call.cnvs = TRUE # if TRUE, call CNVs using sliding window method.  If not TRUE, no CNV calling performed
, annotate.cnvs = TRUE # if TRUE, annotate CNVs to genes
, sample.type = "Blood" # different cnv calling algorithm in place for tumor (enter "Tumor") DNA, else "Blood"
### Set variant calling criteria
, minimum.coverage = 50 # set minimum median coverage for base inclusion in variant calling 
, minimum.bait = 1 # set minimum distance from non-targeted region for base inclusion in variant calling 
, maximum.selfchain = 2 # set maximum self-chain repeat count for base inclusion in variant calling 
, gain.many.seed.signal = 1.8 # expected signal for gain seed generation
, loss.none.seed.signal = .2 # expected signal for loss seed generation
, gain.many.extend.signal = 1.7 # expected signal for gain seed extension
, loss.none.extend.signal = .3 # expected signal for loss seed extension
, gain.many.call.signal = 1.8 # signal criteria for gain call
, loss.none.call.signal = .2  # signal criteria for loss call
, gain.seed.signal = 1.4 # expected signal for gain seed generation
, loss.seed.signal = .6 # expected signal for loss seed generation
, gain.extend.signal = 1.4 # expected signal for gain seed extension
, loss.extend.signal = .6 # expected signal for loss seed extension
, gain.call.signal = 1.4 # signal criteria for gain call
, loss.call.signal = .6  # signal criteria for loss call
, minimum.base.window = 200 # minimum window base count for call
, minimum.base.pass = 180 # minimum base count that pass criteria for call
, window.size = 60 # window size for scanning
, pass.count = 50 # number of bases in window required to pass criteria for call
, long.gain.seed.signal = 1.3 # expected signal for gain seed generation
, long.loss.seed.signal = .7 # expected signal for loss seed generation
, long.gain.extend.signal = 1.25 # expected signal for gain seed extension
, long.loss.extend.signal = .75 # expected signal for loss seed extension
, long.gain.call.signal = 1.25 # signal criteria for gain call
, long.loss.call.signal = .75  # signal criteria for loss call
, long.window.size = 1000 # window size for scanning
, long.pass.count = 700 # number of bases in window required to pass criteria for call
, max.uncertainty = .1 # maximum permitted uncertainty for state calling using mclust for tumors.  Lower = more stringent. Not relevant for slifing window CNV calling.
, mclust.states = 2 # maximum number of states for mixture model.  Max is 9.  Analysis optimized for 2 states.  More states likely to generate false positives.
)


### Load functions from source file
source(criteria$source.filename)

### Run analysis
run.analysis(criteria)