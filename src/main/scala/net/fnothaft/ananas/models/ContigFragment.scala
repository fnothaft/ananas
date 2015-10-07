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
package net.fnothaft.ananas.models

import net.fnothaft.ananas.debruijn.TransientKmerVertex
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment
import scala.annotation.tailrec

object ContigFragment extends Serializable {

  protected[ananas] def buildFromNCF(fragment: NucleotideContigFragment): ContigFragment = {
    // is this the last fragment in a contig?
    val isLast = fragment.getFragmentNumber == (fragment.getNumberOfFragmentsInContig - 1)

    val sequence = IntMer.fromSequence(fragment.getFragmentSequence)
      .map(_.asInstanceOf[CanonicalKmer])

    new ContigFragment(fragment.getContig.getContigName,
                       sequence,
                       isLast,
                       Option(fragment.getFragmentStartPosition).fold(0)(_.toInt))
  }

  def loadFromFile(sc: SparkContext,
                   filename: String): RDD[ContigFragment] = {
    sc.loadSequence(filename)
      .flankAdjacentFragments(16)
      .map(buildFromNCF)
  }
}

case class ContigFragment(id: String,
                          sequence: Array[CanonicalKmer],
                          isLast: Boolean,
                          startPos: Int) extends Fragment[String] {

  def flattenFragment: Array[(CanonicalKmer, TransientKmerVertex[String])] = {
    val length = if (isLast) {
      sequence.length
    } else {
      sequence.length - 1
    }

    // create an array to hold the k-mers
    val array = new Array[(CanonicalKmer, TransientKmerVertex[String])](length)

    @tailrec def flatten(kmer: CanonicalKmer,
                         sequenceIter: Iterator[CanonicalKmer],
                         idx: Int = 0) {
      if (!sequenceIter.hasNext && isLast) {
        val kSet = Set((id, idx + startPos))

        // get correct orientation; see ANANAS-18
        val tkv = if (kmer.isOriginal) {
          TransientKmerVertex[String](forwardTerminals = kSet)
        } else {
          TransientKmerVertex[String](reverseTerminals = kSet)
        }
        array(idx) = (kmer, tkv)
      } else if (sequenceIter.hasNext) {
        val nextKmer = sequenceIter.next
        val kMap = Map(((id, idx + startPos) -> nextKmer.longHash))

        // get correct orientation; see ANANAS-18
        val tkv = if (kmer.isOriginal) {
          TransientKmerVertex[String](forwardStronglyConnected = kMap)
        } else {
          TransientKmerVertex[String](reverseStronglyConnected = kMap)
        }
        array(idx) = (kmer, tkv)

        flatten(nextKmer, sequenceIter, idx + 1)
      }
    }

    // flatten our fragment
    flatten(sequence.head,
            sequence.toIterator.drop(1))

    // return the flattened sequence
    array
  }
}
