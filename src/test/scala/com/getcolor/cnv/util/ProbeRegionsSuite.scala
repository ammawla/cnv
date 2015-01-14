/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.ReferenceRegion
import org.scalatest.FunSuite
import scala.math.abs

class ProbeRegionsSuite extends FunSuite {

  val prp = new ProbeRegionsProcessor

  test ("parse simple lines") {
    val region1 = prp.lineToRegion("chr11 1000 12000")

    assert(region1.get === ReferenceRegion("chr11", 999L, 12000L))

    val region2 = prp.lineToRegion("chr9 1 1000")

    assert(region2.get === ReferenceRegion("chr9", 0L, 1000L))
  }

  test ("merge two overlapping regions") {
    val merged = prp.mergeProbeRegion(List(ReferenceRegion("1", 0L, 1000L)),
				      List(ReferenceRegion("1", 500L, 1500L)))

    assert(merged.length === 1)
    assert(merged.head === ReferenceRegion("1", 0L, 1500L))
  }

  test ("merge two adjacent regions") {
    val merged = prp.mergeProbeRegion(List(ReferenceRegion("1", 0L, 1000L)),
				      List(ReferenceRegion("1", 1000L, 2000L)))

    assert(merged.length === 1)
    assert(merged.head === ReferenceRegion("1", 0L, 2000L))
  }

  test ("fold over a list of regions to merge") {
    val regions = List(ReferenceRegion("1", 0L, 1000L),
		       ReferenceRegion("1", 800L, 1400L),
		       ReferenceRegion("1", 1400L, 2000L),
		       ReferenceRegion("2", 2000L, 3000L))

    val folded: List[ReferenceRegion] = regions.map(List(_))
      .fold(List[ReferenceRegion]())(prp.mergeProbeRegion)

    assert(folded.length === 2)
    assert(folded.last === ReferenceRegion("1", 0L, 2000L))
    assert(folded.head === ReferenceRegion("2", 2000L, 3000L))
  }

  test ("parse and fold regions") {
    val regions = List("chr11 1000 2000",
		       "chr11 2000 3000",
		       "chr9 1 1000",
		       "chr9 900 1900")

    val folded = regions.flatMap(t => prp.lineToRegion(t))
      .map(l => List(l))
      .fold(List[ReferenceRegion]())(prp.mergeProbeRegion)

    assert(folded.length === 2)
    assert(folded.last === ReferenceRegion("chr11", 999L, 3000L))
    assert(folded.head === ReferenceRegion("chr9", 0L, 1900L))
  }

}
