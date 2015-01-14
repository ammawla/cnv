/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.analysis

import org.bdgenomics.adam.avro.{ADAMPileup, Base}
import org.bdgenomics.adam.models.{ADAMRod, ReferencePosition, ReferenceRegion}
import org.scalatest.FunSuite
import scala.math.abs

class PileupAnalysisSuite extends FunSuite {

  val epsilon = 1e-6
  def fpEquals(a: Double, b: Double): Boolean = {
    abs(a - b) < epsilon
  }

  val pa = new PileupAnalysis

  test("acquire target and compensate vs. distance away from start of probe") {
    val targets = List(ReferenceRegion("1", 0L, 1001L),
		       ReferenceRegion("2", 100L, 701L)).zipWithIndex

    val stat_chr1 = (ReferencePosition("1", 90L), 100, 0.5)

    val (target_1, comp_1) = pa.getTargetAndCompensate(stat_chr1, targets).get

    assert(target_1 === 0)
    assert(comp_1._1 == ReferencePosition("1", 90L))
    assert(fpEquals(comp_1._3, 0.5))
    assert(fpEquals(comp_1._2, 4.60517))

    val stat_chr2 = (ReferencePosition("2", 700L), 150, 0.6)

    val (target_2, comp_2) = pa.getTargetAndCompensate(stat_chr2, targets).get

    assert(target_2 === 1)
    assert(comp_2._1 == ReferencePosition("2", 700L))
    assert(fpEquals(comp_2._3, 0.6))
    assert(fpEquals(comp_2._2, 5.010635))
  }

  test("perform simple gc compensation - window of sample only (window = 0)") {
    val bucket = Seq((ReferencePosition("1", 10L), 100.0, 0.4, 0.0),
		     (ReferencePosition("1", 11L), 90.0, 0.5, 0.0),
		     (ReferencePosition("1", 12L), 110.0, 0.2, 0.0))

    val compensated = pa.applyGCCompensation(bucket, 0)

    assert(compensated.length === 3)
    assert(compensated(0)._1 == ReferencePosition("1", 10L))
    assert(fpEquals(compensated(0)._3, 0.0))
    assert(compensated(1)._1 == ReferencePosition("1", 11L))
    assert(fpEquals(compensated(1)._3, 0.01))
    assert(compensated(2)._1 == ReferencePosition("1", 12L))
    assert(fpEquals(compensated(2)._3, 0.04))
  }

  test("perform gc compensation - sliding window of sample + 2 (window = 1)") {
    val bucket = Seq((ReferencePosition("1", 10L), 100.0, 0.4, 0.0),
		     (ReferencePosition("1", 11L), 90.0, 0.5, 0.0),
		     (ReferencePosition("1", 12L), 110.0, 0.2, 0.0),
		     (ReferencePosition("1", 13L), 105.0, 0.3, 0.0),
		     (ReferencePosition("1", 14L), 95.0, 0.8, 0.0))

    val compensated = pa.applyGCCompensation(bucket, 1)

    assert(compensated.length === 5)
    assert(compensated(0)._1 == bucket(0)._1)
    assert(fpEquals(compensated(0)._3, 0.0025))
    assert(compensated(1)._1 == bucket(1)._1)
    assert(fpEquals(compensated(1)._3, 0.0011111111))
    assert(compensated(2)._1 == bucket(2)._1)
    assert(fpEquals(compensated(2)._3, 0.0044444444))
    assert(compensated(3)._1 == bucket(3)._1)
    assert(fpEquals(compensated(3)._3, 0.0011111111))
    assert(compensated(4)._1 == bucket(4)._1)
    assert(fpEquals(compensated(4)._3, 0.0225))
  }

  test ("call copy number variants on simple dataset") {
    val bucket = Seq((ReferencePosition("1", 10L), 1.7),
		     (ReferencePosition("1", 11L), 2.0),
		     (ReferencePosition("1", 12L), 2.1),
		     (ReferencePosition("1", 13L), 1.1),
		     (ReferencePosition("1", 14L), 0.9),
		     (ReferencePosition("1", 15L), 1.2),
		     (ReferencePosition("1", 16L), 0.75),
		     (ReferencePosition("1", 17L), 0.65),
		     (ReferencePosition("1", 18L), 0.4))

    val calls = pa.call(bucket,
			1.0,
			2,
			0.25,
			3,
			2,
			0)

    assert(calls.length === 2)
    assert(calls.head._1 === ReferenceRegion("1", 10L, 13L))
    assert(calls.head._2 === 4)
    assert(calls.last._1 === ReferenceRegion("1", 16L, 19L))
    assert(calls.last._2 === 1)
  }

  test ("merge overlapping calls") {
    val calls = Seq((ReferenceRegion("1", 10L, 13L), 4),
		    (ReferenceRegion("1", 15L, 18L), 1),
		    (ReferenceRegion("1", 16L, 19L), 1),
		    (ReferenceRegion("1", 17L, 20L), 1))

    val merged = pa.mergeCalls(calls)

    assert(merged.length === 2)
    assert(merged(0)._1 === ReferenceRegion("1", 10L, 13L))
    assert(merged(0)._2 === 4)
    assert(merged(1)._1 === ReferenceRegion("1", 15L, 20L))
    assert(merged(1)._2 === 1)
  }

  test ("merge adjacent calls that agree") {
    val calls = Seq((ReferenceRegion("1", 24L, 27L), 1),
		    (ReferenceRegion("1", 27L, 30L), 1),
		    (ReferenceRegion("1", 30L, 33L), 3))

    val merged = pa.mergeCalls(calls)

    assert(merged.length === 2)
    assert(merged(0)._1 === ReferenceRegion("1", 24L, 30L))
    assert(merged(0)._2 === 1)
    assert(merged(1)._1 === ReferenceRegion("1", 30L, 33L))
    assert(merged(1)._2 === 3)
  }

}
