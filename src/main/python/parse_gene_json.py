##
# Copyright (c) 2014. Color Genomics, Inc.

"""
This script converts a JSON file that describes the genes included in our experiment into
the CSV file that is used by the CNV caller to define our experiment.
"""

import json

def decode_gene(gene_name, gene_dict):
    """
    From a dictionary that describes a gene, looks up the gene and extracts important data.
    Returns a string that corresponds to the line for this gene in the platform file.
    """

    # get field data
    try:
        start = gene_dict["start"]
        end = gene_dict["end"]
        chrom = gene_dict["chrom"]
        entrez = gene_dict["EntrezGene"]
    except:
        print "Failed on " + gene_name + ": " + str(gene_dict)
        return ""

    try:
        # translate entrez id to refseq id
        refseq = translate(entrez)
    except:
        print "Couldn't find refseq ID for " + gene_name + " with ID " + str(entrez)
        return ""
        
    # cat fields together and return
    return gene_name + "," + chrom + "," + str(start) + "," + str(end) + "," + refseq

# this is a global, which is bad style, but this is a script, so I'm not as concerned.
translation_dictionary = {}

def translate(entrez_gene_id):
    """
    Translates between Entrez and Refseq IDs, using the Entrez to Refseq dictionary.
    """

    return translation_dictionary[str(entrez_gene_id)]

def load_entrez_to_refseq_translation(filepath, translation_dictionary):
    """
    Loads the relevant data for translation between Entrez and Refseq IDs.
    Translation dictionary is available from ftp://ftp.ncbi.nih.gov/gene/DATA/gene2refseq.gz.
    """

    tfp = open(filepath)

    # loop over lines in dictionary
    for line in tfp:

        # we are using the GRCh38 reference, so only use lines from GRCh38
        if "GRCh38" in line:
            fields = line.split()

            # 
            entrez = fields[1]
            refseq = fields[3]

            # add translation to dictionary
            translation_dictionary[entrez] = refseq

    # close file handle
    tfp.close()

# open json file; assume "gene.list.json"
fp = open("gene.list.json")

# read json data in
gene_data = json.load(fp)

# load translation data
load_entrez_to_refseq_translation("gene2refseq", translation_dictionary)

# open output file and write header
fo = open("Platform_partitionInformation.csv", "w")
print >> fo, "PartitionName,PartitionChr,PartitionStart,PartitionEnd,PartitionRefseq"

# loop over genes, and write data to file
for (gene_name, gene_dict) in gene_data.iteritems():
    
    print >> fo, decode_gene(gene_name, gene_dict)

# close files and exit
fp.close()
fo.close()
