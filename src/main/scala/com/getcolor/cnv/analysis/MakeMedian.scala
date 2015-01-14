/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.analysis

import com.getcolor.cnv.avro.MedianSample
import org.bdgenomics.adam.avro.{ADAMPileup, Base}
import org.bdgenomics.adam.models.{ADAMRod, ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.math.{min, max, log, pow, sqrt, exp, abs}

class MakeMedian extends Serializable {

  /**
   * This method takes an array of RDDs containing coverage info, and merges them together
   * into a median set that contains the average coverage at each position over all samples.
   *
   * @param rdds An array containing RDDs that should be aggregated into a median set.
   * @return An RDD containing the average coverage depth at all observed sites.
   */
  def makeMedian(rdds: Array[RDD[Seq[(ReferencePosition, Double, Double, Double)]]],
		 inputMedian: Option[String],
		 outputMedian: Option[String],
		 minimumCoverage: Double
	       ): RDD[(ReferencePosition, Double, Double, Double)] = {

    val (rddsToLoop, medianR, numSamples): (Array[RDD[Seq[(ReferencePosition, Double, Double, Double)]]],
					    RDD[(ReferencePosition, (Double, Double, Double))],
					    Int) = if (inputMedian.isEmpty) {
      // seed the median set with the first RDD we have
      var median = rdds.head
	.flatMap(b => b)
	.map(q => (q._1, (exp(q._2), q._3, q._4)))
      
      // loop over all other rdds and merge in
      (rdds.drop(1), median, rdds.length)
    } else {
      // get spark context
      val sc = rdds.head.context

      // load file
      val medianRdd: RDD[MedianSample] = sc.adamLoad(inputMedian.get)

      (rdds, medianRdd.map(s => {
	val pos = ReferencePosition(s.getReferenceName, s.getPosition)
	
	(pos, (s.getCoverage, s.getGcRatio, s.getBaitComp))
      }), medianRdd.first.getNumberOfSamples + rdds.length)
    }

    var median = medianR
    median.cache

    rddsToLoop.foreach(rdd => {
      val remappedRdd = rdd.flatMap(b => b)
	.map(q => (q._1, (exp(q._2), q._3, q._4)))
	.cache

      // merge into median set
      median = median.union(remappedRdd)
	.map(kv => (kv._1, ArrayBuffer(kv._2)))
	.combineByKey((s: ArrayBuffer[(Double, Double, Double)]) => s,
		    (s1: ArrayBuffer[(Double, Double, Double)], s2: ArrayBuffer[(Double, Double, Double)]) => s1 ++ s2,
		    (s1: ArrayBuffer[(Double, Double, Double)], s2: ArrayBuffer[(Double, Double, Double)]) => s1 ++ s2)
		      .map(kv => (kv._1, kv._2.toSeq))
	.map(kv => {
	  val (key, seq) = kv

	  val newCount = seq.reduce((v1: (Double, Double, Double), v2: (Double, Double, Double)) => {
	    (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
	  })

	  (key, newCount)
	})

      remappedRdd.unpersist()
    })

    // normalize counts by sample size
    val medianFlattened = median.map(kv => (kv._1, 
					    kv._2._1 / numSamples, 
					    kv._2._2 / numSamples, 
					    kv._2._3 / numSamples))

    median.unpersist()

    if (outputMedian.isDefined) {
      println("Saving median dataset to " + outputMedian.get)
      medianFlattened.map(q => {
	MedianSample.newBuilder()
	  .setReferenceName(q._1.referenceName)
	  .setPosition(q._1.pos)
	  .setCoverage(q._2)
	  .setGcRatio(q._3)
	  .setBaitComp(q._4)
	  .setNumberOfSamples(numSamples)
	  .build()
      }).adamSave(outputMedian.get)
    }

    medianFlattened.filter(q => q._2 >= minimumCoverage)
      .map(q => (q._1, log(q._2), q._3, q._4))
  }

}
