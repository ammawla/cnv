/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.analysis

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.avro.{ADAMPileup, Base}
import org.bdgenomics.adam.models.{ADAMRod, ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.math.{min, max, log => ln, pow, sqrt, exp, abs}

/**
 * Class to perform analysis. Analysis functions contain closures that must be serialized and
 * passed to RDD, therefore this class must exist and be serializable.
 */
class PileupAnalysis extends Serializable with Logging {

  /**
   * Runs analysis on RDD of rods. This gathers the:
   *
   * * Un-normalized coverage at a locus.
   * * GC content surrounding a locus.
   * * Probe coverage at a locus.
   *
   * @param rdd RDD of rods to analyze.
   * @param targets List of probes; used for analyzing probe coverage.
   * @param gcCompWindow Length of the window used for calculating the GC ratio of a base.
   * E.g., 10 would lead to a window of 10 positions both before and after the base.
   * @return Returns an RDD with analyzed data grouped by reference region. Within each sequence
   * that maps to a reference region, we store a tuple containing the base position, unnormalized
   * coverage, GC ratio, and target capture compensation.
   *
   * @see compensateRdd
   */
  def analyzeRdd(rdd: RDD[(ReferencePosition, Int, Double)],
		 targets: Broadcast[List[(ReferenceRegion, Int)]],
		 gcCompWindow: Int): RDD[Seq[(ReferencePosition, Double, Double, Double)]] = {
    rdd.flatMap(s => getTargetAndCompensate(s, targets.value))
      .map(kv => (kv._1, ArrayBuffer(kv._2)))
      .combineByKey((s: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s,

		    (s1: ArrayBuffer[(ReferencePosition, Double, Double, Double)], s2: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s1 ++ s2,
		    (s1: ArrayBuffer[(ReferencePosition, Double, Double, Double)], s2: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s1 ++ s2)
		      .map(kv => (kv._1, kv._2.toSeq))
      .map(kv => kv._2)
      .map(s => applyGCCompensation(s, gcCompWindow))
  }

  /**
   * Returns the average/min/max coverage across an RDD.
   *
   * @param rdd RDD containing coverage data.
   * @return Returns a tuple containing (average, min, max) coverage for this RDD.
   */
  def getAverageDOC(rdd: RDD[(ReferencePosition, Double)]): (Double, Double, Double) = {
    val sites = rdd.count
    val doc = rdd.flatMap(s => {
      val r: Option[Double] = if (!s._2.isNaN && !s._2.isInfinite) {
	Some(s._2)
      } else {
	log.warn("Have bad value " + s._2 + " at : " + s._1)
	None
      }
      r
    })
    val totalDOC = doc.reduce(_ + _)
    val minDOC = doc.reduce(_ min _)
    val maxDOC = doc.reduce(_ max _)

    (totalDOC / sites.toDouble, minDOC, maxDOC)
  }

  /**
   * Identifies the sequencing target for a site, and derives necessary compensation. As this returns
   * an Option collection, it should be used with a flatMap.
   *
   * @param stat Stat tuple emitted by getStats.
   * @param region List of sequencing probes, zipped with indexes.
   * @return If locus is covered by probes, returns the stat tuple with compensation added. Else,
   * returns a None option.
   *
   * @see getStats
   */
  def getTargetAndCompensate(stat: (ReferencePosition, Int, Double),
			     region: List[(ReferenceRegion, Int)]
			   ): Option[(Int, (ReferencePosition, Double, Double, Double))] = {
    val pos = stat._1

    val targets = region.filter(kv => kv._1.contains(pos))

    if (targets.length == 0) {
      return None
    }

    val targetRegion = targets.head

    val distance = min(pos.pos - targetRegion._1.start, targetRegion._1.end - pos.pos)

    val compensation = ln(10.0 + distance)

    val lcov = ln(stat._2.toDouble)
    val coverage = if (!lcov.isInfinite && !lcov.isNaN) {
      lcov
    } else {
      0.0
    }

    val compensated = (pos, coverage, stat._3, compensation)

    Some((targetRegion._2, compensated))
  }

  /**
   * Collects the GC compensation information over a bucket of data.
   *
   * @param statBucket Group of data bucketed together. Is a seq containing (ReferencePos,
   * coverage, gc at position, probe tiling depth).
   * @param compWindow Size of window for compensation.
   * @return Returns a new bucket of statistics containing (ReferencePos, coverage at site,
   * average gc within window around site, probe tiling depth).
   */
  def applyGCCompensation(statBucket: Seq[(ReferencePosition, Double, Double, Double)],
			  compWindow: Int): Seq[(ReferencePosition, Double, Double, Double)] = {

    var windowPre = List[(ReferencePosition, Double, Double, Double)]()
    var windowPost = List[(ReferencePosition, Double, Double, Double)]()
    var processed = List[(ReferencePosition, Double, Double, Double)]()

    def process(stat: Option[(ReferencePosition, Double, Double, Double)]) {
      def compensate(stat: (ReferencePosition, Double, Double, Double),
		     gc: Double): (ReferencePosition, Double, Double, Double) = {

	val compensation = pow(gc - 0.4, 2)

	(stat._1, stat._2, compensation, stat._4)
      }

      def getGC(): Double = {
	assert(windowPre.length > 0 || windowPost.length > 0)

	val gc = windowPost.map(_._3).fold(0.0)(_ + _) + windowPre.map(_._3).fold(0.0)(_ + _)
	gc / (windowPre.length + windowPost.length).toDouble
      }

      // if we have a new sample, add it to the front of the pre window
      if (stat.isDefined) {
	windowPre = stat.get :: windowPre
      }

      // since we have already added the sample we've received, we should
      // process samples if we have compWindow + 1 samples
      // however, if the input sample is empty, we should process now
      if (windowPre.length == compWindow + 1 || stat.isEmpty) {
	// get gc value
	val gc = getGC

	// pop from end of pre list
	val sample = windowPre.last
	windowPre = windowPre.dropRight(1)

	// compensate sample and add to processed list
	val compensatedSample = compensate(sample, gc)
	processed = compensatedSample :: processed

	// push uncompensated sample to post list
	windowPost = sample :: windowPost

	// pop from end of post list if list is full
	if (windowPost.length > compWindow) {
	  windowPost = windowPost.dropRight(1)
	}
      }
    }

    // sort bucket
    val sortedBucket = statBucket.sortBy(s => s._1)

    // slide window over data
    sortedBucket.foreach(s => process(Some(s)))

    // keep processing until all samples have been processed
    while (windowPre.length > 0) {
      process(None)
    }

    // emit compensated samples
    processed.reverse
  }

  /**
   * Provides wrapper for calling copy number variants on processed coverage data that are grouped
   * into buckets by target. Wrapper function is needed for serialization.
   *
   * @see call
   *
   * @param statBucket A seq containing tuples of reference positions and doubles, where
   * the double represents the corrected coverage at the site.
   * @param avgCoverage The average coverage across this sample.
   * @param ploidy The base ploidy of this sample.
   * @param threshold The threshold for calling a CNV. E.g., a threshold of 0.4 corresponds to
   * calling a deletion at DOC=0.6 and an insertion at DOC=1.4.
   * @param bucketSize The size of the buckets for sweeping across the reference when calling.
   * @param mustMatch The number of bases within each bucket that must be strictly within the
   * threshold for calling.
   * @param snrThreshold The SNR threshold required for making a CNV call.
   * @return Returns a seq of (possibly overlapping) regions, along with integer CNV calls.
   */
  def callRdd(rdd: RDD[Seq[(ReferencePosition, Double)]],
	      avgCoverage: Double,
	      ploidy: Int,
	      threshold: Double,
	      bucketSize: Int,
	      mustMatch: Int,
	      snrThreshold: Double): RDD[(ReferenceRegion, Int)] = {
    rdd.map(b => {
      call(b,
	   avgCoverage,
	   ploidy,
	   threshold,
	   bucketSize,
	   mustMatch,
	   snrThreshold)
    }).flatMap(b => mergeCalls(b))
  }

  /**
   * Calls copy number variants on processed coverage data that are grouped
   * into buckets by target.
   *
   * @see callRdd
   *
   * @param statBucket A seq containing tuples of reference positions and doubles, where
   * the double represents the corrected coverage at the site.
   * @param avgCoverage The average coverage across this sample.
   * @param ploidy The base ploidy of this sample.
   * @param threshold The threshold for calling a CNV. E.g., a threshold of 0.4 corresponds to
   * calling a deletion at DOC=0.6 and an insertion at DOC=1.4.
   * @param bucketSize The size of the buckets for sweeping across the reference when calling.
   * @param mustMatch The number of bases within each bucket that must be strictly within the
   * threshold for calling.
   * @param snrThreshold The SNR threshold required for making a CNV call.
   * @return Returns a seq of (possibly overlapping) regions, along with integer CNV calls.
   */
  def call(statBucket: Seq[(ReferencePosition, Double)],
	   avgCoverage: Double,
	   ploidy: Int,
	   threshold: Double,
	   bucketSize: Int,
	   mustMatch: Int,
	   snrThreshold: Double): Seq[(ReferenceRegion, Int)] = {

    assert(bucketSize >= mustMatch)
    assert(threshold >= 0.0 && threshold <= 0.5)
    assert(snrThreshold >= 0.0)

    // create sliding window of buckets
    val windows = statBucket.sliding(bucketSize)

    /**
     * Makes a CNV call over a window.
     *
     * @param window Window of pileups to call copy number on.
     */
    def callWindow(window: Seq[(ReferencePosition, Double)]): Option[(ReferenceRegion, Int)] = {

      /**
       * Tests to see if a site is within the threshold for calling a copy number variant.
       *
       * @param test The copy number _change_ count. E.g., a deletion would be -1.
       * @param doc Depth of coverage to check.
       */
      def withinThreshold(test: Int,
			  doc: Double): Boolean = {
	val ploidyInv = 1.0 / ploidy
	if (test < 0) {
	  doc > avgCoverage * (1.0 + test * ploidyInv - threshold) &&
	  doc <= avgCoverage * (1.0 + (test + 1) * ploidyInv - threshold)
	} else if (test == 0) {
	  doc > avgCoverage * (1 - threshold) && doc < avgCoverage * (1 + threshold)
	} else {
	  doc >= avgCoverage * (1.0 + (test - 1) * ploidyInv + threshold) &&
	  doc < avgCoverage * (1.0 + test * ploidyInv + threshold)
	}
      }

      /**
       * Function to identify the copy number count at a site.
       *
       * @param doc Depth of coverage to check.
       * @return Copy number count.
       */
      def siteCopyNumber(doc: Double): Int = {

	/**
	 * Tail recursive helper function call to find the copy number of a site.
	 *
	 * @param test Copy number change count to test.
	 * @param doc Depth of coverage to check.
	 *
	 * @see withinThreshold
	 */
	def siteCopyNumberHelper(test: Int,
				 doc: Double): Int = {
	  if(withinThreshold(test, doc)) {
	    test
	  } else {
	    siteCopyNumberHelper(test + 1, doc)
	  }
	}

	siteCopyNumberHelper(-ploidy, doc) + ploidy
      }

      /**
       * Checks to see if a site agrees with the call made across this window.
       *
       * @param call Copy number call made across this window.
       * @param site Tuple identifying the location of a site and the adjusted depth-of-coverage seen.
       * @return True if this site agrees with the call made.
       */
      def agreesWithCall(call: Int,
			 site: (ReferencePosition, Double)): Boolean = {
	call == siteCopyNumber(site._2)
      }

      /**
       * Returns the current region we are looking at.
       */
      def currentRegion: ReferenceRegion = {
	val start = window.head._1
	val end = window.last._1

	assert(start.referenceName == end.referenceName)
	assert(start.pos < end.pos)

	// reference region has an inclusive start and exclusive end, so need to increment end
	ReferenceRegion(start.referenceName, start.pos, end.pos + 1)
      }

      // get coverage average for this site
      val avgCov = window.map(_._2).reduce(_ + _) / window.length.toDouble

      // get variance
      val variance = window.map(p => pow(p._2 - avgCov, 2.0)).reduce(_ + _)

      // snr here is defined by mean over standard deviation
      val snr = if (variance > 1e-6) { // check for div/0
	avgCov / sqrt(variance)
      } else {
	100
      }

      // get copy number for this site
      val copyNumber = siteCopyNumber(avgCov)

      // check for minimum count of bases within threshold
      val numAgree = window.filter(agreesWithCall(copyNumber, _)).length

      // if this is above our threshold, emit call
      if (snr > snrThreshold && copyNumber != ploidy && numAgree >= mustMatch) {

	log.info("At: " + currentRegion + ", DOC: " + avgCov + " SNR: " + snr +
		" VAR: " + variance +" CN: " + copyNumber + "NA: " + numAgree +
		(" R: %1.1f" format (avgCov / avgCoverage)) + ", CNV called")

	Some((currentRegion, copyNumber))
      } else {

	log.info("At: " + currentRegion + ", DOC: " + avgCov + " SNR: " + snr +
		" VAR: " + variance + " CN: " + copyNumber + " NA: " + numAgree +
		(" R: %1.1f" format (avgCov / avgCoverage)) + ", not called")
	None
      }
    }

    // call cnvs
    windows.flatMap(callWindow).toSeq
  }

  /**
   * Function to merge overlapping calls.
   *
   * @param calls Calls to review and merge.
   * @return A minimal set of unique, non-overlapping calls.
   */
  def mergeCalls(calls: Seq[(ReferenceRegion, Int)]): Seq[(ReferenceRegion, Int)] = {
    @tailrec def mergeCallsHelper(merge: Seq[(ReferenceRegion, Int)],
				  calls: Seq[(ReferenceRegion, Int)]
				): Seq[(ReferenceRegion, Int)] = {
      if (calls.length < 2) {
	merge ++ calls
      } else {
	// get head of list
	val headCall = calls.head
	val secondCall = calls(1)
	
	// get new head and next list
	val (headSeq, nextList) = if (headCall._1.overlaps(secondCall._1) ||
				      headCall._1.isAdjacent(secondCall._1) &&
				      headCall._2 == secondCall._2) {
	  //assert(headCall._2 == secondCall._2, "Cannot merge calls that disagree")
	  
	  (Seq[(ReferenceRegion, Int)](),
	   (headCall._1.merge(secondCall._1), headCall._2) +: calls.drop(2))
	} else {
	  (Seq(headCall), calls.drop(1))
	}
	
	mergeCallsHelper(merge ++ headSeq, nextList)
      }
    }

    mergeCallsHelper(Seq(), calls)
  }
}
