/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.normalizations {

  import org.apache.spark.SparkContext._
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.{LinearRegressionModel,
					    LinearRegressionWithSGD,
					    LabeledPoint}
  import org.apache.spark.mllib.hack.CNVLinearRegressionModel
  import org.apache.spark.rdd.RDD
  import org.bdgenomics.adam.models.ReferencePosition
  import org.jblas.{DoubleMatrix, Solve}
  import scala.math.{pow, sqrt, exp}
  
  object FitCoefficients {
    
    def apply(rdd: RDD[(ReferencePosition, Double, Double, Double)],
	      iters: Int,
	      threshold: Double,
	      disableFit: Boolean): FitCoefficients = {
      val cf = new CoefficientFitter
      
      val model = cf.trainCoefficients(rdd, iters, threshold)
      
      new FitCoefficients(model, disableFit)
    }
    
  }
  
  private[normalizations] class CoefficientFitter extends Serializable {
    
    /**
     * Converts a sample into a feature for training a linear regression model. As points can be
     * filtered out due to having too low coverage, this method returns an Option collection and
     * should be used via a flatMap.
     *
     * @param avgCov Average coverage across the median set.
     * @param trainingCoverageThreshold Threshold to use for including points in the training set.
     * For targeted sequencing, a threshold of 50 is recommended.
     * @param point Point to convert into a feature. A tuple containing (locus position, log coverage
     * at site, gc content compensation for site, target depth compensation for site).
     *
     * @see trainCoefficients
     */
    def pointToFeature(trainingCoverageThreshold: Double,
		       point: (ReferencePosition, Double, Double, Double)): Option[LabeledPoint] = {
      if (point._2 < trainingCoverageThreshold) {
	None
      } else {
	Some(new LabeledPoint(point._2, Vectors.dense(point._3, point._4)))
      }
    }
    
    /**
     * Trains a linear regression to fit the coefficients used for compensation. The linear regression
     * is fit using stochastic gradient descent as provided by the Spark MLLib.
     *
     * @param avgCov The average coverage across all genomes that have been pooled.
     * @param rdd The RDD to use for training.
     * @param iters The number of iterations to use for fitting this linear regression model.
     * @param threshold A threshold to use for excluding points that have low coverage. Typically, it
     * is recommended to exclude points with coverage lower than 50.
     * @return Returns a trained linear regression model.
     *
     * @see pointToFeature
     * @see compensateRdd
     */
    def trainCoefficients(rdd: RDD[(ReferencePosition, Double, Double, Double)],
			  iters: Int,
			  threshold: Double): LinearRegressionModel = {
      
      // we do not use data from the sex chromosomes to fit this normalization
      // therefore, find the last base that is not on a sex chromosome
      // removed autosomal condition for time being, add back later...
      val lastAutosomalBase = rdd.map(q => (q._1, 1))
	.sortByKey(false)
	.first
	._1
      
      // get median, mean, sd
      val medianSet = rdd.sample(false, 0.001, 4 /* seed for random generation */)
	.map(q => q._2)
	.collect
	.toSeq
	.sortWith(_ < _)
      val median = medianSet(medianSet.length / 2)
      val mean = medianSet.sum / medianSet.length.toDouble
      val variance = medianSet.map(v => pow(v - mean, 2.0))
	.reduce(_ + _) / medianSet.length.toDouble
      val sd = sqrt(variance)
      
      // filter bases that are in autosome and that have a z-score between -1 and 1
      val featureRdd = rdd.filter(q => {
	val zscore = (q._2 - median) / sd
	
	(zscore >= -1.0 || zscore <= 1.0) && (q._1.compare(lastAutosomalBase) <= 0)
      }).flatMap(pointToFeature(threshold, _))
      
      // sample from table for fit
      val local = featureRdd.sample(false, 0.001, 4).collect()
      
      // solve for least squares fit
      val y = new DoubleMatrix(local.map(_.label))
      val x0 = new DoubleMatrix(local.map(v => v.features.toArray(0)))
      val x1 = new DoubleMatrix(local.map(v => v.features.toArray(1)))
      val x2 = DoubleMatrix.ones(local.length)
      val x = DoubleMatrix.concatHorizontally(DoubleMatrix.concatHorizontally(x0, x1), x2)
      val xt = x.transpose()
      val xtx = xt.mmul(x)
      val xty = xt.mmul(y)
      val soln = Solve.solve(xtx, xty)
      
      // run linear regression
      /*
       * This code calls out to MLLib and should be used for distributed training on the dataset.
       * However, right now, it isn't working, so we collect the data and process it locally.
       * TODO: work with MLLib team to fix this
       val ftrs = LinearRegressionWithSGD.train(featureRdd, iters)
       println("From sgd:")
       println("Coefficients: ")
       ftrs.weights.map(println)
       println("Intercept: " + ftrs.intercept)
       */
      
      // return lin reg model
      new CNVLinearRegressionModel(Vectors.dense(soln.get(0, 0), soln.get(1, 0)), soln.get(2, 0))
    }
    
  }
  
  class FitCoefficients(coeffs: LinearRegressionModel, disableFit: Boolean) extends Serializable {
    
    /**
     * Performs compensation on an RDD of analyzed data. Compensation is done using the
     * collected GC values, and probe coverage, as well as the coefficients that were trained
     * via linear regression.
     *
     * @param rdd RDD on which to compensate. RDD contains site grouped data, as output by analyzeRdd.
     * @param coeffs Compensation coefficients trained by linear regression.
     * @return RDD containing tuples with the position of each pileup, and the normalized coverage at
     * that site.
     *
     * @see analyzeRdd
     * @see trainCoefficients
     */
    def compensateRdd(rdd: RDD[Seq[(ReferencePosition, Double, Double, Double)]]
		    ) : RDD[Seq[(ReferencePosition, Double)]] = {
      def comp(elem: (ReferencePosition, Double, Double, Double)): (ReferencePosition, Double) = {
	if (disableFit) {
	  (elem._1, exp(elem._2)) // if fit is disabled, do not use coefficients
	} else {
	  (elem._1, exp(elem._2 - coeffs.predict(Vectors.dense(Array(elem._3, elem._4))))) // this is unintuitive
	}
      }
      
      rdd.map(s => s.map(comp))
    }
  }
}

/**
 * This hack needs to be done because Spark 1.0.0 made a class constructor package private. We
 * were using this class constructor to work around an issue we encountered above. We'll hopefully
 * fix this in a later revision, but we're not using this code at the moment, so it isn't a high
 * priority to fix.
 */
package org.apache.spark.mllib.hack {
  import org.apache.spark.mllib.regression.LinearRegressionModel
  import org.apache.spark.mllib.linalg.{ Vector => MLLibVector }

  class CNVLinearRegressionModel (
    override val weights: MLLibVector,
    override val intercept: Double) extends LinearRegressionModel(weights, intercept) {
  }
}
