/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.{SequenceDictionary, ReferencePosition, ReferenceRegion}
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.math.exp

object EmitCSV {

  /**
   * Writes coverage data out in CSV form. This is for use with the R scripts at:
   *
   * * http://dl.dropboxusercontent.com/u/39583687/DoC_functions.R
   * * http://dl.dropboxusercontent.com/u/39583687/DoC_parameters.R
   * * http://dl.dropboxusercontent.com/u/39583687/ReadMe.txt
   *
   * @param file Path to write file to.
   * @param data Analyzed coverage data for this sample.
   * @param dict Sequence dictionary containing reference naming information.
   */
  def apply (file: String, 
	     data: RDD[Seq[(ReferencePosition, Double, Double, Double)]],
	     dict: SequenceDictionary,
	     targets: List[ReferenceRegion]) {

    val ecp = new EmitCSVProcessor(dict)

    val kv = data.flatMap(b => b)
      .map(q => (q._1, exp(q._2).toInt))
      .cache
    
    // don't save one file a million times, instead, save a million files once!
    // this is a notable perf improvement...
    kv.partitionBy(new RegionPartitioner(targets))
      .map(ecp.makeLine)
      .saveAsTextFile(file)
  }
}

private[util] class RegionPartitioner(targets: List[ReferenceRegion]) extends Partitioner {
  // we put indices we don't have targets for into partition 0
  def numPartitions = targets.length + 1

  def getPartition(key: Any): Int = key match {
    case rp: ReferencePosition => {
      // indexWhere returns -1 if the index isn't found; so, add 1 to place in partition 0
      1 + targets.indexWhere(_.contains(rp))
    }
    case _ => throw new IllegalArgumentException("Input must be a reference position.")
  }
}

private[util] class EmitCSVProcessor(dict: SequenceDictionary) extends Serializable {

  /**
   * Strips the prefix of the contig name, and prepends the "chr" prefix.
   * This prefix is necessary for the R scripts, which are looking for "chr"
   * as a prefix for chromosomes.
   *
   * @param chr Contig name to clean.
   * @return Contig name with prefix stripped, and "chr" prefix attached.
   */
  def cleanName(chr: String): String = {
    // drop prefix characters; we assume we are seeing a prefix character if the
    // character isn't a number (autosome), X/Y (sex chromosome) or M (mitochondrial chromosome)
    "chr" + chr.dropWhile(c => !(c.isDigit || c == 'X' || c == 'Y' || c == 'M'))
  }

  /**
   * Converts a line into the following format:
   *
   * Chromosome Position Coverage
   *
   * For the header line, returns:
   *
   * ChrID Position Coverage
   *
   * All fields are tab delimited.
   *
   * @param line Coverage data represented by (Position, DoC).
   * @return A string summarizing this datapoint.
   */
  def makeLine(line: (ReferencePosition, Int)): String = {
    if (line._1.referenceName == "-1") {
      "ChrID\tPosition\tCoverage"
    } else {
      cleanName(line._1.referenceName) + "\t" + line._1.pos.toInt + "\t" + line._2
    }
  }
  
}
