##
# Copyright (c) 2014, Color Genomics, Inc.

"""
This script converts an Agilent probe description file with overlapping probes into a flattened
representation. I.e., each covered location is covered by one single "mega-probe."
"""

def chrToInt(chr):
    """
    Converts a test chromosome into an integer ID. For the sex chromosomes, returns 23.
    """

    try:
        return int(chr.replace("chr", ""))
    except:
        return 23

def rcmp (r0, r1):
    """
    Comparison function for two genomic regions. If two regions are on the same chromosome,
    this sorts by reference position. If the chromosome is different, this sorts by chromosome ID.
    """

    if r0[0] == r1[0]:
        return r0[1] - r1[1]
    else:
        return chrToInt(r0[0]) - chrToInt(r1[0])

# assume input file is probes.txt
fp = open("probes.txt")

regions = []

# loop over input file, get reference region, and append to region list
for line in fp:

    line = line.split()

    chr = line[0]
    start = int(line[1])
    end = int(line[2])

    regions.append((chr, start, end))

# sort regions
rgns = sorted(regions, cmp = rcmp)

# take first region
region = regions[0]

# array for coalescing regions down
regions = []

# loop over regions and look for overlapping regions
for r in rgns:
    
    if region[0] != r[0] or region[2] < r[1]:
        # if region doesn't overlap, append

        regions.append(region)
        region = r
    else:
        # if region overlaps the last region, merge

        region = (region[0], region[1], r[2])

# write out to newprobes.txt
fo = open("newprobes.txt", "w")

# loop over regions and print
for r in regions:

    print >> fo, r[0] + '\t%d\t%d' % (r[1], r[2])

fo.close()
