/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.normalizations

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition}
import scala.collection.mutable.ArrayBuffer
import scala.math.abs

/**
 * Serializable wrapper for the Apache Commons polynomial spline function.
 *
 * @param spline Spline function to wrap.
 */
private[cnv] case class SerializableSpline(val spline: PolynomialSplineFunction) 
		  extends Serializable {

  /**
   * Wrapper function to get the interpolated value from the spline.
   *
   * @param x Point to evaluate.
   * @return Value of spline at X point.
   */
  def value(x: Double): Double = {
    spline.value(x)
  }
}

object InvariantSetNormalization {

  def apply(
    rdd: RDD[Seq[(ReferencePosition, Double, Double, Double)]],
    medianRdd: RDD[(ReferencePosition, Double, Double, Double)],
    rho: (Double, Double),
    region: Broadcast[List[(ReferenceRegion, Int)]]
  ): RDD[Seq[(ReferencePosition, Double, Double, Double)]] = {
    val isn = new InvariantSetNormalization

    isn.invariantSetNormalization(rdd, medianRdd, rho, region)
  }

}

class InvariantSetNormalization extends Serializable {

  private[normalizations] def getTarget(stat: (ReferencePosition, Double, Double, Double),
		region: List[(ReferenceRegion, Int)]
		): (Int, ArrayBuffer[(ReferencePosition, Double, Double, Double)]) = {

    val pos = stat._1
    val targets = region.filter(kv => kv._1.contains(pos))
    (targets.head._2, ArrayBuffer(stat))
  }

  /**
   * Performs an invariant set normalization against a median dataset. In this method, a new
   * sample is compared against a reference (median) set by rank. For positions whose rank has
   * not changed significantly between the median and the sample, a spline is fit to the data,
   * and then this spline is used to recalibrate all datapoints in the set.
   *
   * This method is described in:
   *
   * Li, Cheng, and W. Hung Wong. "Model-based analysis of oligonucleotide arrays:
   * model validation, design issues and standard error application." Genome Biol 2.8 (2001): 1-11.
   *
   * @param rdd RDD of analyzed data to be recalibrated.
   * @param medianRdd RDD containing median set of coverage data to calibrate against.
   * @param rho Parameters to be used during invariant set normalization.
   * @return Returns an updated version of the per sample RDD which has been normalized by
   * the spline.
   */
  private[normalizations] def invariantSetNormalization(
    rdd: RDD[Seq[(ReferencePosition, Double, Double, Double)]],
    medianRdd: RDD[(ReferencePosition, Double, Double, Double)],
    rho: (Double, Double),
    region: Broadcast[List[(ReferenceRegion, Int)]]
  ): RDD[Seq[(ReferencePosition, Double, Double, Double)]] = {
  
    val flatRdd = rdd.flatMap(b => b)
    val coverRdd = flatRdd.map(q => (q._1, q._2))
    val medianPairRdd = medianRdd.map(q => (q._1, q._2))

    // get ranks of both sample and median
    val sampleRankRdd = rank(coverRdd)
    val medianRankRdd = rank(medianPairRdd)

    // we do not use data from the sex chromosomes to fit this normalization
    // therefore, find the last base that is not on a sex chromosome
    // removed autosomal condition for time being, add back later...
    val lastAutosomalBase = coverRdd.sortByKey(false)
      .first
      ._1

    // the invariant set rdd contains:
    // ((sample coverage, sample rank), (median coverage, median rank), rank is invariant?)
    var invSetRdd = sampleRankRdd.filter(kv => kv._1.compare(lastAutosomalBase) <= 0)
      .join(medianRankRdd)
      .map(t => (t._2._1, t._2._2, true)) // all points start as invariant
      .cache

    // get the number of samples that are in the invariant set
    var numSamples = invSetRdd.filter(kv => kv._3)
      .count
    var oldNumSamples = numSamples
    var coeffs = (rho._1 * 10, rho._2 * 10)

    // update equation
    def update(sample: ((Double, Int), (Double, Int), Boolean)
	     ): ((Double, Int), (Double, Int), Boolean) = {
      val sampleRank = sample._1._2
      val medianRank = sample._2._2

      // normalized "average" rank
      val air = (medianRank + sampleRank).toDouble / (2 * numSamples).toDouble

      // normalized rank difference
      val prd = abs(medianRank - sampleRank).toDouble / numSamples.toDouble

      // threshold for keeping point in set
      val threshold = (coeffs._2 - rho._1) * air + coeffs._1
      val keep = prd < threshold

      (sample._1, sample._2, keep)
    }

    // loop to filter points out
    do {
      oldNumSamples = numSamples

      // do updates
      invSetRdd = invSetRdd.map(update)

      // decay the constants
      if (coeffs._1 > rho._1) {
	coeffs = (0.9 * coeffs._1, 0.9 * coeffs._2)
      }

      // count number of samples
      numSamples = invSetRdd.filter(kv => kv._3)
	.count

      // loop until we aren't seeing a large change in the invariant set
    } while (oldNumSamples - numSamples >= 50)

    // collect data for spline fit
    val splineData = invSetRdd.filter(kv => kv._3)
      .map(kv => (kv._2._1, ArrayBuffer(kv._1._1)))
      .combineByKey((ab: ArrayBuffer[Double]) => ab,
		  (ab1: ArrayBuffer[Double], ab2: ArrayBuffer[Double]) => ab1 ++ ab2,
		  (ab1: ArrayBuffer[Double], ab2: ArrayBuffer[Double]) => ab1 ++ ab2)
		    .map(kv => (kv._1, kv._2.toSeq))
      .map(kv => {
	val (y, x) = kv

	val xAvg = x.reduce(_ + _) / x.length.toDouble

	(xAvg, y)
      }).map(kv => (kv._1, ArrayBuffer(kv._2)))
	.combineByKey((ab: ArrayBuffer[Double]) => ab,
		  (ab1: ArrayBuffer[Double], ab2: ArrayBuffer[Double]) => ab1 ++ ab2,
		  (ab1: ArrayBuffer[Double], ab2: ArrayBuffer[Double]) => ab1 ++ ab2)
		    .map(kv => (kv._1, kv._2.head))
		    .sortByKey()
		    .collect

    invSetRdd.unpersist()

    val x = splineData.map(p => p._1)
    val y = splineData.map(p => p._2)

    // fit spline
    val splineFitter = new SplineInterpolator()
    val spline = splineFitter.interpolate(x, y)
    val serializableSpline = SerializableSpline(spline)

    // join
    val medToJoin = medianRdd.map(q => (q._1, (q._2, q._3, q._4)))
    val rddToJoin = flatRdd.map(q => (q._1, (q._2, q._3, q._4)))

    // perform join and normalize
    rddToJoin.join(medToJoin)
      .map(kvv => {
	val (k, (s, m)) = kvv

	val normalized = s._1 - serializableSpline.value(m._1)

	(k, normalized, s._2, s._3)
      }).map(getTarget(_, region.value))
	.combineByKey((s: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s,
		      (s1: ArrayBuffer[(ReferencePosition, Double, Double, Double)], s2: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s1 ++ s2,
		      (s1: ArrayBuffer[(ReferencePosition, Double, Double, Double)], s2: ArrayBuffer[(ReferencePosition, Double, Double, Double)]) => s1 ++ s2)
			.map(kv => kv._2.toSeq)
  }

  /**
   * Computes the rank for an RDD. For an array where an ordering can be applied, rank is defined
   * as the index of the position that a value would occupy in the sorted equivalent of this array.
   * E.g., for:
   *
   * v = Array(2, 0, 7, 3)
   * sort(v) = Array(0, 2, 3, 7)
   * rank(v) = Array(1, 0, 3, 2)
   *
   * @param rdd RDD of (ReferencePosition, coverage) tuples to be ranked.
   * @return Returns an RDD of (ReferencePosition, (Coverage, rank)) tuples.
   */
  private[normalizations] def rank(rdd: RDD[(ReferencePosition, Double)]
				): RDD[(ReferencePosition, (Double, Int))] = {
    val sortRdd = rdd.keyBy(q => q._2)
      .sortByKey()
      .cache

    val partitionSizes = sortRdd.mapPartitionsWithIndex((idx: Int, iter: Iterator[(Double, (ReferencePosition, Double))]) => {
      Iterator((idx, iter.length))
    }).collect
      .toSeq
      .sortBy(kv => kv._1)

    val partitions = partitionSizes.map(kv => kv._1)
    val sizes = partitionSizes.map(kv => kv._2)
    val starts = sizes.scan(0)(_ + _).dropRight(1)
    val partitionStartMap = partitions.zip(starts).toMap

    def keyPartitions(idx: Int,
		      iter: Iterator[(Double, (ReferencePosition, Double))]
		      ): Iterator[(ReferencePosition, (Double, Int))] = {
      val partitionStart = partitionStartMap(idx)
      val indexedIter = iter.zipWithIndex
      indexedIter.map(kv => (kv._1._2._1, (kv._1._2._2, kv._2 + partitionStart)))
    }

    val indexedRdd = sortRdd.mapPartitionsWithIndex(keyPartitions)
    sortRdd.unpersist()

    indexedRdd
  }

}
