/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.ananas.overlapping

import net.fnothaft.ananas.models.{ CanonicalKmer, Sequence }
import org.apache.spark.graphx.Edge
import scala.annotation.tailrec
import scala.math.max

object Overlap extends Serializable {
  
  def apply(triple: (Double, (Sequence, Sequence)),
            minLength: Int): Option[Edge[Overlap]] = {
    // unpack triple
    val (predictedSimilarity, (s1, s2)) = triple
    
    // if predicted length is less than threshold, exit early
    if ((predictedSimilarity * max(s1.length, s2.length)).toInt < minLength ||
        s1.id >= s2.id) {
      None
    } else {
      // get k-mer array of the first read
      val s1Array = s1.sequenceKmers
      
      // get k-mers of the second read as a map
      val s2Array = s2.sequenceKmers
      val s2Hashes = s2Array.zipWithIndex
        .map(p => (p._1.hashCode, (p)))
        .toMap

      // find the k-mer that "starts" the overlap
      @tailrec def findStartKmer(idx: Int = 0): (Int, Int) = {
        val s2kmer = s2Hashes.get(s1Array(idx).hashCode)
        if (s2kmer.isDefined) {
          (idx, s2kmer.get._2)
        } else {
          findStartKmer(idx + 1)
        }
      }
      
      val (startKmerIdx1, startKmerIdx2) = findStartKmer()
      val startCanonicality = s2Array(startKmerIdx2)
        .isOriginal
      
      // find the k-mer that "ends" the overlap
      @tailrec def findEndKmer(idx: Int = s1Array.length - 1): (Int, Int) = {
        val s2kmer = s2Hashes.get(s1Array(idx).hashCode)
          .filter(_._1.isOriginal == startCanonicality)
        if (s2kmer.isDefined) {
          (idx, s2kmer.get._2)
        } else {
          findEndKmer(idx - 1)
        }
      }
      val (endKmerIdx1, endKmerIdx2) = findEndKmer()
      
      // where is this sequence anchored on the alignment?
      def whereAnchored(start: Int, end: Int, length: Int, switchStrands: Boolean): Option[Position] = {
        val compTuple = if (switchStrands) {
          ((length - start) < 15, end < 15)
        } else {
          (start < 15, (length - end) < 15)
        }
        compTuple match {
          case (true, true) => Some(Position.CONTAINED)
          case (true, false) => Some(Position.START)
          case (false, true) => Some(Position.END)
          case _ => None
        }
      }
      
      // did we switch strands?
     val switchStrands = startCanonicality ^ s1Array(startKmerIdx1).isOriginal

      // is the set intersection of these hashes larger than the min overlap length?
      // first, a quick comparison
      if (endKmerIdx1 - startKmerIdx1 + 15 >= minLength &&
          ((!switchStrands && endKmerIdx2 - startKmerIdx2 + 15 >= minLength) ||
           (switchStrands && startKmerIdx2 - endKmerIdx2 + 15 >= minLength))) {
        // get position metadata
        val pos1 = whereAnchored(startKmerIdx1, endKmerIdx1, s1Array.length, false)
        val pos2 = whereAnchored(startKmerIdx2, endKmerIdx2, s2Array.length, switchStrands)
   
        def computeOverlapSize: Int = {
          val s1HashSet = s1Array.drop(startKmerIdx1)
            .take(endKmerIdx1 - startKmerIdx1)
            .map(_.hashCode)
            .toSet
          val s2HashSet = if (switchStrands) {
            s2Array.drop(endKmerIdx2)
              .take(startKmerIdx2 - endKmerIdx2)
              .map(_.hashCode)
              .toSet
          } else {
            s2Array.drop(startKmerIdx2)
              .take(endKmerIdx2 - startKmerIdx2)
              .map(_.hashCode)
              .toSet
          }            
          
          s1HashSet.intersect(s2HashSet).size
        }

        // is this metadata valid?
        pos1.flatMap(p1 => pos2.flatMap(p2 => {
          // now, do the _true_ comparison of overlap size
          val overlapSize = computeOverlapSize

          if (overlapSize >= minLength) {
            Some(Edge(s1.id, s2.id, Overlap(switchStrands,
                                            s1Array(startKmerIdx1),
                                            s1Array(endKmerIdx1),
                                            p1,
                                            p2,
                                            overlapSize)))
          } else {
            None
          }
        }))
      } else {
        None
      }
    }
  }
}

case class Overlap(switchesStrands: Boolean,
                   startKmer: CanonicalKmer,
                   endKmer: CanonicalKmer,
                   alignmentPosition1: Position,
                   alignmentPosition2: Position,
                   overlapSize: Int) {
}
