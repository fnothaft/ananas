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

import net.fnothaft.ananas.AnanasFunSuite
import org.bdgenomics.adam.models.Alphabet
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.math.abs
import scala.util.Random

class OverlapGraphSuite extends AnanasFunSuite {

  def randomString(seed: Int, len: Int): (String, Random) = {
    val r = new Random(seed)

    ((0 until len).map(i => r.nextInt(4))
      .map(i => i match {
        case 0 => "A"
        case 1 => "C"
        case 2 => "G"
        case _ => "T"
      }).reduceLeft(_ + _), r)
  }

  sparkTest("compute overlaps for ten 1000 bp reads, all drawn from the same strand") {
    val baseString = randomString(123, 2000)._1
    var read = -1
    val reads = sc.parallelize(baseString
      .sliding(1000, 100)
      .toSeq
      .map(s => {
        read += 1
        AlignmentRecord.newBuilder()
          .setStart(read)
          .setSequence(s)
          .build()
      }))

    val overlapGraph = OverlapGraph.overlapReads(reads,
                                                 500,
                                                 256,
                                                 1,
                                                 Some(123456L))
                                                   .cache()

    val edges = overlapGraph.edges
    assert(edges.count === 27)
    assert(!edges.map(ev => ev.attr.switchesStrands).reduce(_ || _))
  }

  sparkTest("compute overlaps for ten 1000 bp reads, drawn from different strands") {
    val (baseString, rv) = randomString(123, 2000)
    var read = -1
    val reads = sc.parallelize(baseString
      .sliding(1000, 100)
      .toSeq
      .map(s => {
        read += 1
        val flipStrand = rv.nextBoolean()
        val fs = if (flipStrand) {
          Alphabet.dna.reverseComplement(s)
        } else {
          s
        }
        AlignmentRecord.newBuilder()
          .setStart(read)
          .setSequence(fs)
          .build()
      }))

    val overlapGraph = OverlapGraph.overlapReads(reads,
                                                 500,
                                                 256,
                                                 1,
                                                 Some(123456L))
                                                   .cache()

    val edges = overlapGraph.edges
    assert(edges.count === 27)
    assert(edges.map(ev => ev.attr.switchesStrands).reduce(_ || _))
    assert(!edges.map(ev => ev.attr.switchesStrands).reduce(_ && _))
  }
}
