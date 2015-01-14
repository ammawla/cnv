/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec

object ProbeRegions {

  /**
   * Creates a set of probes from a text file containing probe definitions.
   *
   * @param sc Context to use for loading the probes.
   * @param file File to load the probes from.
   * @return Returns a list of reference regions that correspond to probes.
   */
  def apply (sc: SparkContext, file: String): List[ReferenceRegion] = {
    // read file in to rdd
    val textRdd = sc.textFile(file)

    // get processor
    val prp = new ProbeRegionsProcessor

    // map lines to text
    val regionsRdd = textRdd.flatMap(prp.lineToRegion)
      .map(v => (v, v))
      .sortByKey()
      .map(kv => kv._2)

    // fold to merge probe regions
    //regionsRdd.fold(List[ReferenceRegion]())(prp.mergeProbeRegion)

    regionsRdd.collect.toList
  }
}

class ProbeRegionsProcessor extends Serializable {

  /**
   * Converts a line from a probe definition file (space separated CSV) into a region.
   * Returns a None type if data couldn't be parsed. Should be used from a flatMap.
   *
   * @param line Line to parse.
   * @return Returns a Option containing a ReferenceRegion. If parsing fails, we print
   * to the console and return a None.
   */
  def lineToRegion (line: String): Option[ReferenceRegion] = {
    val splits = line.split(Array(' ', '\t'))
    assert(splits.length == 3)

    try {
      val chr = splits(0)

      Some(ReferenceRegion(chr,
			   splits(1).toLong - 1,
			   splits(2).toLong))
    } catch {
      case _ : Throwable => {
	println("Couldn't parse: " + line)
	None
      }
    }
  }

  /**
   * Tail-call recursive optimized function to merge region lists.
   *
   * @param currentRegion List of current regions.
   * @param nextRegion Next set of regions.
   * @return Full list of regions, merged in.
   */
  @tailrec final def mergeProbeRegion (currentRegions: List[ReferenceRegion],
				       nextRegion: List[ReferenceRegion]): List[ReferenceRegion] = {
    if (nextRegion.length < 1) {
      currentRegions
    } else {
      if (currentRegions.isEmpty ||
	  !((nextRegion.head.overlaps(currentRegions.head) ||
	     nextRegion.head.isAdjacent(currentRegions.head)) ||
	    (nextRegion.head.overlaps(currentRegions.last) ||
	     nextRegion.head.isAdjacent(currentRegions.last)))) {
	nextRegion ::: currentRegions
      } else {
	val cr = if (nextRegion.head.overlaps(currentRegions.head) ||
		     nextRegion.head.isAdjacent(currentRegions.head)) {
	  nextRegion.head.merge(currentRegions.head) :: currentRegions.drop(1)
	} else {
	  nextRegion.head.merge(currentRegions.last) :: currentRegions.dropRight(1)
	}

	mergeProbeRegion(cr, nextRegion.drop(1))
      }
    }
  }

}
