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
package net.fnothaft.ananas.graph

import net.fnothaft.ananas.AnanasFunSuite
import net.fnothaft.ananas.overlapping._
import org.apache.spark.graphx._
import org.bdgenomics.formats.avro._
import scala.util.Random

class EmitAssemblySuite extends AnanasFunSuite {

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

  sparkTest("create assembly from small graph, single strand reads") {
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

    val stringGraph = TransitiveReduction(overlapGraph)

    val assembly = EmitAssembly(stringGraph)
      .collect

    assert(assembly.length === 2)
    assert(assembly.map(_.getFragmentSequence).mkString("A") === baseString)
  }
}
