The scripts in this directory are separate from the main CNV pipeline. These
scripts are intended for running large batches of CNVs together. There
are a few differences between these two workflows:

- The main (clr based) workflow relies on the environment setup from the clr
  repository. The batch workflow assumes that it is running on a bare cluster
  and performs all environment setup for itself (installing resources, setting
  up disks, etc.).

- The batch workflow does not pull down a previous median set. Since the batch
  workflow is intended for processing large groups of samples, we do not need a
  previously generated median set. In the clr workflow, we assume that we are
  only processing a single sample at a time. To perform this processing, we
  must pull down the median set.

- The batch workflow breaks the processing up into two stages:
  
  1. Conversion of BAMs into read counts
  2. Processing of read counts into CNV calls

  The clr pipeline does both of these processes in a single stage. The
  motivation for separating the stages in the batch process is to enable high
  parallelism for the first stage, while then allowing all the samples to be
  jointly called together after processing. There are several reasons for this:

  1. When calling CNVs (step 2), we may want to perform a parameter sweep. By
     separating the two stages, we can now perform a parameter sweep, without
     needing to recalculate the read counts.
  2. The BAM->read count conversion is parallel by sample, but the CNV calling
     process is sequential by gene. Therefore, it may be desirable to have
     different levels of parallelism for each stage.

  The parameters (CNV caller settings and median set) are assumed to be frozen
  when calling CNVs via the clr pipeline. Additionally, we're only ever calling
  a single sample at a time via clr, so we do not have differing degrees of
  parallelism.
