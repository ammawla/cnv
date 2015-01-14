##
# Copyright (c) 2014. Color Genomics, Inc.

"""
Code to parse probes in bed format into a table for coalescing partitions down to genes.
"""

from operator import itemgetter
import sys

# populate arguments
if len(sys.argv) != 4:
    print >> sys.stderr, "probes_to_genes.py <probe file> <platform inputs> <platform output>"
    print >> sys.stderr, "<probe file> - probes in BED format"
    print >> sys.stderr, "<platform input> - description of genes in platform"
    print >> sys.stderr, "<platform output> - partition to gene mapping file"
    exit(1)

probes=sys.argv[1]
platform=sys.argv[2]
output=sys.argv[3]

# open file handles
fpr = open(probes)
fpl = open(platform)
fo = open(output, "w")

# initialize empty list of genes
genes = []

# partition numbering starts at 1
pnum = 1

# loop over platform
for line in fpl:

    # platform file is formatted:
    # <gene>,<contig>,<start>,<end>,<refseq id>
    line = line.split(",")
    gene = line[0]
    chr = line[1].replace("chr", "")
    start = int(line[2])
    end = int(line[3])

    # cat onto list of genes
    genes.append((gene, chr, start, end))

def find_gene(chr, start, end):
    """
    Finds the gene that a probe is associated with.
    """

    # loop over genes
    for gene in genes:

        # de-tuple gene
        (g, gc, gs, ge) = gene

        # are we on the same contig?
        if gc == chr:
            # does our probe overlap the gene?
            if start >= gs and end <= ge:
                return g

    # if we didn't match anything (i.e., are not being called), return other
    return "other"

# create empty probe array
probes = []

# loop over probes
for line in fpr:
    
    # probes are in bed format:
    # <contig>\t<start>\t<end>
    line = line.split()
    chr = line[0]
    start = int(line[1])
    end = int(line[2])

    # append to probe array
    probes.append(chr, start, end)

# sort by contig and then start position
genes = sorted(genes, key=itemgetter(0, 1))

# loop over probes
for probe in probes:

    # extract info...
    (chr, start, end) = probe

    # print to output file
    # partitions range from 00000 to 99999
    print >> fo, ("%05d," % pnum) + find_gene(chr, start, end),
    
    # increment partition count
    pnum += 1

# close file handles
fpl.close()
fpr.close()
=======
import sys
from operator import itemgetter

if len(sys.argv) != 4:

    print >> sys.stderr, "Usage:\npython probes_to_genes.py <probe_file> <platform> <output>"

# probe file
fpr = open(sys.argv[1])

# platform file
fpl = open(sys.argv[2])

# output file
fo = open(sys.argv[3], "w")

genes = []
pnum = 1

# loop over panel and parse out the genes and regions from the panel
for line in fpl:

    line = line.split(",")
    gene = line[0]
    chr = line[1]
    start = int(line[2])
    end = int(line[3])

    genes.append((gene, chr, start, end))

##
# Finds the gene that corresponds to a probe, if one corresponds. If there is
# no gene in the panel that corresponds to this probe, returns "other".
def find_gene(chr, start, end):

    for gene in genes:

        (g, gc, gs, ge) = gene

        if gc == chr:
            if start >= gs and end <= ge:
                return g

    return "other"

probes = []

# loop over probe file and parse
for line in fpr:
    
    line = line.split()
    chr = "chr" + line[0]
    start = int(line[1])
    end = int(line[2])

    probes.append((chr, start, end))

# sort by contig and then position
probes = sorted(probes, key=itemgetter(0, 1))

# loop over probes and identify the genes that correspond to all probes
i = 1
for probe in probes:
    print ("%d" % i) + str(probe)
    i += 1

    (chr, start, end) = probe

    pnum += 1

# close files
fpr.close()
fpl.close()
fo.close()

