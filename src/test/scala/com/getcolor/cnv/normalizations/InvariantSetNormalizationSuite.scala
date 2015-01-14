/*
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.normalizations

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.util.SparkFunSuite
import scala.math.abs

class InvariantSetNormalizationSuite extends SparkFunSuite {
  
  sparkTest("make a rank for a single partition RDD") {
    val l: List[(ReferencePosition, Double)] = List((ReferencePosition("0", 0L), 4.0),
						    (ReferencePosition("0", 1L), 3.0), 
						    (ReferencePosition("0", 2L), 6.0), 
						    (ReferencePosition("0", 3L), 2.0), 
						    (ReferencePosition("0", 4L), 11.0), 
						    (ReferencePosition("0", 5L), 1.0),
						    (ReferencePosition("0", 6L), 5.0),
						    (ReferencePosition("0", 7L), 10.0))
    val rdd: RDD[(ReferencePosition, Double)] = sc.parallelize(l, 1)
    val ins = new InvariantSetNormalization

    val rank: RDD[(Int, Int)] = ins.rank(rdd)
      .map(rdi => (rdi._2._2, rdi._1.pos.toInt))
    
    def getRankValue(i: Int): Int = {
      val r = rank.filter(v => v._1 == i)
      assert(r.count === 1)
      r.first._2.toInt
    }

    assert(rank.count === 8)
    assert(getRankValue(0) === 5)
    assert(getRankValue(1) === 3)
    assert(getRankValue(2) === 1)
    assert(getRankValue(3) === 0)
    assert(getRankValue(4) === 6)
    assert(getRankValue(5) === 2)
    assert(getRankValue(6) === 7)
    assert(getRankValue(7) === 4)
  }

  sparkTest("make a rank for a multi partition RDD") {
    val l: List[(ReferencePosition, Double)] = List((ReferencePosition("0", 0L) , 4.0 ),
						    (ReferencePosition("0", 1L) , 3.0 ), 
						    (ReferencePosition("0", 2L) , 6.0 ), 
						    (ReferencePosition("0", 3L) , 2.0 ), 
						    (ReferencePosition("0", 4L) , 11.0), 
						    (ReferencePosition("0", 5L) , 1.0 ),
						    (ReferencePosition("0", 6L) , 5.0 ),
						    (ReferencePosition("0", 7L) , 10.0),
						    (ReferencePosition("0", 8L) , 0.0 ),
						    (ReferencePosition("0", 9L) , 7.0 ),
						    (ReferencePosition("0", 10L), 8.0 ),
						    (ReferencePosition("0", 11L), 14.0),
						    (ReferencePosition("0", 12L), 15.0),
						    (ReferencePosition("0", 13L), 9.0 ),
						    (ReferencePosition("0", 14L), 13.0),
						    (ReferencePosition("0", 15L), 12.0))
    val rdd: RDD[(ReferencePosition, Double)] = sc.parallelize(l, 2)
    val ins = new InvariantSetNormalization

    val rank: RDD[(Int, Int)] = ins.rank(rdd)
      .map(rdi => (rdi._2._2, rdi._1.pos.toInt))
    
    def getRankValue(i: Int): Int = {
      val r = rank.filter(v => v._1 == i)
      assert(r.count === 1)
      r.first._2.toInt
    }

    assert(rank.count === 16)
    assert(getRankValue(0)  === 8 )
    assert(getRankValue(1)  === 5 )
    assert(getRankValue(2)  === 3 )
    assert(getRankValue(3)  === 1 )
    assert(getRankValue(4)  === 0 )
    assert(getRankValue(5)  === 6 )
    assert(getRankValue(6)  === 2 )
    assert(getRankValue(7)  === 9 )
    assert(getRankValue(8)  === 10)
    assert(getRankValue(9)  === 13)
    assert(getRankValue(10) === 7 )
    assert(getRankValue(11) === 4 )
    assert(getRankValue(12) === 15)
    assert(getRankValue(13) === 14)
    assert(getRankValue(14) === 11)
    assert(getRankValue(15) === 12)
  }
}
