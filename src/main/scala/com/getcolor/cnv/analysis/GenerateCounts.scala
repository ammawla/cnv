/**
 * Copyright (c) 2014. Color.
 */

package com.getcolor.cnv.analysis

import net.sf.samtools.{CigarOperator, TextCigarCodec}
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.adam.util._
import scala.collection.JavaConverters._

object GenerateCounts {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def apply(rdd: RDD[ADAMRecord]): RDD[(ReferencePosition, Int, Double)] = {
    val gc = new GenerateCounts
    rdd.flatMap(gc.readToCounts)
      .map(kv => (kv._1, gc.countToCounts(kv._2)))
      .combineByKey(c => c, gc.combineCounts, gc.combineCounts)
      .map(kv => (kv._1, kv._2._1, kv._2._2))
  }

}

class GenerateCounts extends Serializable {

  def countToCounts(c: (Int, Int)): (Int, Double) = (c._1, c._2.toDouble)

  def combineCounts(c1: (Int, Double), c2: (Int, Double)) = {
    val tot = c1._1 + c2._1
    val gc = (c1._1.toDouble * c1._2 + c2._1.toDouble * c2._2) / tot.toDouble
    
    (tot, gc)
  }

  def combineCountAndCounts(c1: (Int, Double), c2: (Int, Int)) = combineCounts(c1,
									       countToCounts(c2))

  def readToCounts(record: ADAMRecord): Seq[(ReferencePosition, (Int, Int))] = {
    try {
      var referencePos = record.getStart
      var readPos = 0
      val isReverseStrand = record.getReadNegativeStrand
      val refName = record.getContig.getContigName
      
      val cigar = GenerateCounts.CIGAR_CODEC.decode(record.getCigar.toString)
      
      var countList = List[(ReferencePosition, (Int, Int))]()
      val seq: String = record.getSequence
      
      cigar.getCigarElements.asScala.foreach(cigarElement =>
	cigarElement.getOperator match {
          // MATCH (alignment match, sequence match, or sequence mismatch)
          case CigarOperator.M | CigarOperator.EQ | CigarOperator.X =>
	    
            for (i <- 0 until cigarElement.getLength) {
	      
	      val pos = ReferencePosition(refName, referencePos)
	      val gc = if (seq(readPos) == 'G' || seq(readPos) == 'C' || seq(readPos) == 'S') {
		1
	      } else {
		0
	      }
	      
	      countList = (pos, (1, gc)) :: countList
	      
              readPos += 1
              referencePos += 1
            }
	    
          case _ =>
            if (cigarElement.getOperator.consumesReadBases()) {
              readPos += cigarElement.getLength
            }
            if (cigarElement.getOperator.consumesReferenceBases()) {
              referencePos += cigarElement.getLength
            }
	}
      )
      
      countList.toSeq
    } catch {
      case _ : Throwable => Seq()
    }
  }
  
}
