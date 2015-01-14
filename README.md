A copy number variant caller based on [this paper](http://www.ncbi.nlm.nih.gov/pubmed/21486468) and implemented on top of [ADAM](http://www.github.com/bigdatagenomics/adam).

## References

```
Nord, Alex S., et al. "Accurate and exact CNV identification from targeted high-throughput sequence data." BMC genomics 12.1 (2011): 184.

Massie, Matt, et al. ADAM: Genomics Formats and Processing Patterns for Cloud Scale Computing. Technical Report UCB/EECS-2013-207, EECS Department, University of California, Berkeley, 2013.
```

## To build

This project uses [Maven](http://maven.apache.org) and produces a shaded uber-jar. To build, run:

```
mvn package
```

From the repository root.

## To run

The minimum arguments required to run are:

```
$ java -Xmx2g -jar target/cnv-0.0.1.jar 
Argument "READS" is required
 READS                                                           : ADAM read-oriented data
 TARGETS                                                         : Target probe input
 VARIANTS                                                        : BED output
```

E.g.,

```
java -Xmx2g -jar target/cnv-0.0.1.jar reads.bam probes.txt cnvs.bed
```

Running the command without any arguments produces full debug output.