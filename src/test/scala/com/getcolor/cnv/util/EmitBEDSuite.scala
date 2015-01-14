/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.ReferenceRegion
import org.scalatest.FunSuite

class EmitBEDSuite extends FunSuite {

  val ebp = new EmitBEDProcessor

  test ("emit simple calls") {
    val call1 = ebp.makeLine((ReferenceRegion("chrom1", 0L, 1001L), 3))

    assert(call1 === "chrom1\t0\t1000\tcnv=3\t380")

    val call2 = ebp.makeLine((ReferenceRegion("chrom11", 100L, 110L), 1))

    assert(call2 === "chrom11\t100\t109\tcnv=1\t270")
  }
}
