/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object EmitBED {

  /**
   * Saves a BED file containing copy number calls.
   *
   * @param file File to save calls to.
   * @param calls RDD containing (region, copy number) tuples describing CNV calls.
   */
  def apply (file: String, calls: RDD[(ReferenceRegion, Int)]) {
    val ebp = new EmitBEDProcessor

    calls.sortByKey()
      .map(ebp.makeLine)
      .coalesce(1)
      .saveAsTextFile(file)
  }
}

private[util] class EmitBEDProcessor extends Serializable {
  // array of shade colors for BED tracks
  val shade = Map(0 -> 160,
		  1 -> 270,
		  3 -> 380,
		  4 -> 490,
		  5 -> 610,
		  6 -> 720,
		  7 -> 830,
		  8 -> 940)
  val shadeMax = 1000

  /**
   * Converts a called copy number variant into a line emitted in the BED file.
   *
   * @param call Tuple of (Region, copy number) expressing a copy number call.
   * @return Returns line converted into a string.
   */
  def makeLine(call: (ReferenceRegion, Int)): String = {
    val (loc, cnv) = call

    val s = if (shade.contains(cnv)) {
      shade(cnv)
    } else {
      shadeMax
    }

    loc.referenceName + "\t" + loc.start + "\t" + (loc.end - 1) + "\tcnv=" + cnv + "\t" + s
  }
}
