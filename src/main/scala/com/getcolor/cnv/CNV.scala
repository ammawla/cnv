/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv

import com.getcolor.cnv.analysis.{MakeMedian, PileupAnalysis, GenerateCounts}
import com.getcolor.cnv.normalizations.{FitCoefficients, InvariantSetNormalization}
import com.getcolor.cnv.util.{EmitBED, EmitCSV, ProbeRegions}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{Option => option, Argument}
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.cli.{ADAMSparkCommand,
                                ADAMCommandCompanion,
                                ParquetArgs,
                                SparkArgs,
                                Args4j,
                                Args4jBase}
import org.bdgenomics.adam.models.{ADAMRod, ReferencePosition, SequenceDictionary}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMRDDFunctions}
import org.bdgenomics.adam.util.ParquetLogger
import java.util.logging.Level

object CNV extends ADAMCommandCompanion {

  val commandName = "CNV"
  val commandDescription = "Call copy-number variants using ADAM"

  def apply (args: Array[String]) = {
    new CNV(Args4j[CNVArgs](args))
  }
}

class CNVArgs extends Args4jBase with ParquetArgs with SparkArgs {

  @Argument (metaVar = "TARGETS", required = true, usage = "Target probe input", index = 0)
  var targetInput: String = _

  @Argument (metaVar = "OUTPUTDIR", required = true, usage = "Output directory", index = 1)
  var outputDir: String = _

  @Argument (metaVar = "SAMPLES", required = true, usage = "ADAM read-oriented data", index = 2)
  var readInputs: Array[String] = Array()

  @option (name = "-cnvWindow", usage = "Window used for initial copy number variant call, default value = 20")
  var cnvWindow = 20

  @option (name = "-cnvWindowInThreshold", usage = "Number of bases in window required to meet threshold, default value = 18")
  var cnvWindowInThreshold = 18

  @option (name = "-threshold", usage = "DOC threshold for calling a CNV, default value = 0.4")
  var threshold = 0.4

  @option (name = "-gcCalWindow", usage = "Window used for calibrating DOC based on GC ratio, default value = 100")
  var gcCalWindow = 100

  @option (name = "-snrThreshold", usage = "SNR threshold that must be exceeded to call a location, default value = 9.0")
  var snrThreshold = 9.0

  @option (name = "-ploidy", usage = "Default ploidy for non-CNV regions, default is 2.")
  var ploidy = 2

  @option (name = "-numIters", usage = "Number of iterations to use for training, default is 10.")
  var numIters = 10

  @option (name = "-trainingCoverageThreshold", usage = "Threshold for coverage to use for training, default is 50.")
  var trainingCoverageThreshold = 3

  @option (name = "-inputMedian", usage = "Path to input median set.")
  var inputMedian = ""

  @option (name = "-outputMedian", usage = "Path to save median set at.")
  var outputMedian = ""

  @option (name = "-minimumCoverage", usage = "Minimum coverage to allow for sites for calling, default is 50")
  var minimumCoverage = 50.0

  @option (name = "-emitCSV", usage = "Also emit CSV of coverage data per sample.")
  var emitCSV = false

  @option (name = "-disableFit", usage = "Disable the linear regression fit.")
  var disableFit = false

  @option (name = "-skipAnalysis", usage = "Skips the analysis stage.")
  var skipAnalysis = false

  @option (name = "-partitions", usage = "Repartitions reads after reading.")
  var partitions = -1
}

class CNV (protected val args: CNVArgs) extends ADAMSparkCommand[CNVArgs] with Logging {

  val companion = CNV

  def run (sc: SparkContext, job: Job) {

    // Quiet Parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    // analysis instance
    val pa = new PileupAnalysis()

    // get targets from file
    log.info("Loading targets.")
    val targets = ProbeRegions(sc, args.targetInput)
    val bcastTargets = sc.broadcast(targets.zipWithIndex)

    val piles: Array[RDD[Seq[(ReferencePosition, Double, Double, Double)]]] = args.readInputs.map(i => {
      log.info("Loading reads from " + i)
      val rds: RDD[ADAMRecord] = sc.adamLoad(i)

      val reads = if (args.partitions > 0) {
	rds.filter(r => r.getReadMapped).repartition(args.partitions)
      } else {
	rds.filter(r => r.getReadMapped)
      }

      log.info("Have " + reads.count + " reads.")

      log.info("Converting to counts.")
      val rods = GenerateCounts(reads)

      log.info("Have " + rods.count + " rods.")

      // analyze rdd
      log.info("Performing analysis.")
      val analyzedRdd = pa.analyzeRdd(rods, bcastTargets, args.gcCalWindow)

      if (args.emitCSV || args.skipAnalysis) {
	val dict: SequenceDictionary = rds.adamGetSequenceDictionary()
	val csvPath = args.outputDir + "/" + i.split("/").last.dropRight(3) + "csv"
	log.info("Saving CSV at: " + csvPath)
	EmitCSV(csvPath, analyzedRdd, dict, targets)
      }

      val coverageBeforeNormalization = pa.getAverageDOC(analyzedRdd.flatMap(b => b)
	.map(q => (q._1, q._2)))

      log.info("Before normalization, for" + i)
      log.info("Average coverage is: " + coverageBeforeNormalization._1)
      log.info("Min coverage is: " + coverageBeforeNormalization._2)
      log.info("Max coverage is: " + coverageBeforeNormalization._3)

      analyzedRdd
    })

    if (!args.skipAnalysis) {
      
      // options for packing/unpacking median
      val readMedian: Option[String] = if (args.inputMedian != "") {
	Some(args.inputMedian)
      } else {
	None
      }
      val saveMedian: Option[String] = if (args.outputMedian != "") {
	Some(args.outputMedian)
      } else {
	None
      }
      
      // make median rdd
      val medianMaker = new MakeMedian()
      val medianRdd = medianMaker.makeMedian(piles, readMedian, saveMedian, args.minimumCoverage)
      
      // train coeffs
      log.info("Training coeffs")
      val coeffs = FitCoefficients(medianRdd, 
				   args.numIters, 
				   args.trainingCoverageThreshold,
				   args.disableFit)
      
      // per set, normalize and call
      (0 until piles.length).foreach(i => {
	val name = args.readInputs(i)
	val analyzedRdd = piles(i)
	
	log.info("Normalizing and calling " + i)
	
	// normalize piles
	val normalizedRdd = InvariantSetNormalization(analyzedRdd,
						      medianRdd,
						      (0.003, 0.007),
						      bcastTargets)
	val coverageAfterNormalization = pa.getAverageDOC(analyzedRdd.flatMap(b => b)
	  .map(q => (q._1, q._2)))
	log.info("After normalization,")
	log.info("Average coverage is: " + coverageAfterNormalization._1)
	log.info("Min coverage is: " + coverageAfterNormalization._2)
	log.info("Max coverage is: " + coverageAfterNormalization._3)
	
	// compensate rdd
	val compensatedRdd = coeffs.compensateRdd(analyzedRdd)
	val (avgDoc, minDoc, maxDoc) = pa.getAverageDOC(compensatedRdd.flatMap(b => b))
	log.info("Average coverage is: " + avgDoc)
	log.info("Min coverage is: " + minDoc)
	log.info("Max coverage is: " + maxDoc)
	
	// call cnvs
	log.info("Calling CNVs.")
	val calledCnvs = pa.callRdd(compensatedRdd,
				    avgDoc,
				    args.ploidy,
				    args.threshold,
				    args.cnvWindow,
				    args.cnvWindowInThreshold,
				    args.snrThreshold)
	log.info("Have " + calledCnvs.count + " CNVs.")
	
	val outputPath = args.outputDir + "/" + name.split("/").last.dropRight(3) + "bed"
	
	// emit calls
	EmitBED(outputPath, calledCnvs)
      })      
    }

    log.info("Fin!")
  }
}
