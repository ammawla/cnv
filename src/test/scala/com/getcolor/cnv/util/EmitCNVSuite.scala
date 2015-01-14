/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.util

import org.bdgenomics.adam.models.{ReferencePosition, SequenceDictionary, SequenceRecord}
import org.scalatest.FunSuite

class EmitCSVSuite extends FunSuite {

  val sd = new SequenceDictionary(Vector(SequenceRecord("chrom1", 1000L)))

  val ecp = new EmitCSVProcessor(sd)

  test ("test the prefixes we expect to see") {
    // no prefix
    assert(ecp.cleanName("X") === "chrX")
    assert(ecp.cleanName("21") === "chr21")
    
    // "chrom" prefix
    assert(ecp.cleanName("chromY") === "chrY")
    assert(ecp.cleanName("chrom9") === "chr9")
    
    // "chr" prefix
    assert(ecp.cleanName("chrM") === "chrM")
    assert(ecp.cleanName("chr1") === "chr1")
  }

  test ("emit simple line") {
    val call1 = ecp.makeLine((ReferencePosition("chrom1", 1L), 3))

    assert(call1 === "chr1\t1\t3")
  }

  test ("emit header line") {
    val header = ecp.makeLine((ReferencePosition("-1", -1L), -1))

    assert(header === "ChrID\tPosition\tCoverage")
  }
}
