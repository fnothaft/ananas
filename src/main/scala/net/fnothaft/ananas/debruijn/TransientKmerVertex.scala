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
package net.fnothaft.ananas.debruijn

import net.fnothaft.ananas.models.{ CanonicalKmer, IntMer}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

object TransientKmerVertex extends Serializable {

  private[debruijn] def merge[T](v1: TransientKmerVertex[T],
                                 v2: TransientKmerVertex[T]): TransientKmerVertex[T] = {
    
    TransientKmerVertex(v1.forwardTerminals ++ v2.forwardTerminals,
                        v1.forwardStronglyConnected ++ v2.forwardStronglyConnected,
                        v1.forwardLinked ++ v2.forwardLinked,
                        v1.reverseTerminals ++ v2.reverseTerminals,
                        v1.reverseStronglyConnected ++ v2.reverseStronglyConnected,
                        v1.reverseLinked ++ v2.reverseLinked)
  }

  private[debruijn] def mergeCanon[T](k1: CanonicalKmer,
                                      v1: TransientKmerVertex[T],
                                      k2: CanonicalKmer,
                                      v2: TransientKmerVertex[T]): (CanonicalKmer,
                                                                    TransientKmerVertex[T]) = {
    val i1 = k1.asInstanceOf[IntMer]
    val i2 = k2.asInstanceOf[IntMer]
    assert(k1.sameExceptForOrientation(k2),
           "Asked to merge %s and %s, which are not canonical twins. I1: %d %d %s, I2: %d %d %s.".format(
      k1, k2,
      i1.kmer, i1.mask, i1.isOriginal,
      i2.kmer, i2.mask, i2.isOriginal))

    val (fwdKmer, fwdVertex, revVertex) = if (k1.isOriginal) {
      (k1, v1, v2)
    } else {
      (k2, v2, v1)
    }

    (fwdKmer, TransientKmerVertex(fwdVertex.forwardTerminals ++ revVertex.reverseTerminals,
                                  fwdVertex.forwardStronglyConnected ++ revVertex.reverseStronglyConnected,
                                  fwdVertex.forwardLinked ++ revVertex.reverseLinked,
                                  fwdVertex.reverseTerminals ++ revVertex.forwardTerminals,
                                  fwdVertex.reverseStronglyConnected ++ revVertex.forwardStronglyConnected,
                                  fwdVertex.reverseLinked ++ revVertex.forwardLinked))
  }

  private[debruijn] def toEdges[T](rdd: RDD[(CanonicalKmer, TransientKmerVertex[T])]): RDD[Edge[Unit]] = {
    rdd.flatMap(kv => {
      val (kmer, vertex) = kv
      val srcId = kmer.longHash

      // merge map values and eliminate dupes
      (vertex.forwardStronglyConnected.values ++ vertex.forwardLinked.values ++
       vertex.reverseStronglyConnected.values ++ vertex.reverseLinked.values)
        .toSet
        .map((v: Long) => (new Edge[Unit](srcId, v)))
    })
  }
}

case class TransientKmerVertex[T](forwardTerminals: Set[(T, Int)] = Set.empty[(T, Int)],
                                  forwardStronglyConnected: Map[(T, Int), Long] = Map.empty[(T, Int), Long],
                                  forwardLinked: Map[(T, Int), Long] = Map.empty[(T, Int), Long],
                                  reverseTerminals: Set[(T, Int)] = Set.empty[(T, Int)],
                                  reverseStronglyConnected: Map[(T, Int), Long] = Map.empty[(T, Int), Long],
                                  reverseLinked: Map[(T, Int), Long] = Map.empty[(T, Int), Long]) {
}
