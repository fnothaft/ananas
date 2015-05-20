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

import net.fnothaft.ananas.models.CanonicalKmer
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

object TransientKmerVertex extends Serializable {

  private[debruijn] def merge(v1: TransientKmerVertex,
                              v2: TransientKmerVertex): TransientKmerVertex = {
    
    TransientKmerVertex(v1.terminals ++ v2.terminals,
                        v1.stronglyConnected ++ v2.stronglyConnected,
                        v1.linked ++ v2.linked)
  }

  private[debruijn] def toEdges(rdd: RDD[(CanonicalKmer, TransientKmerVertex)]): RDD[Edge[Unit]] = {
    rdd.flatMap(kv => {
      val (kmer, vertex) = kv
      val srcId = kmer.longHash

      // merge map values and eliminate dupes
      (vertex.stronglyConnected.values ++ vertex.linked.values)
        .toSet
        .map((v: Long) => (new Edge[Unit](srcId, v)))
    })
  }
}

private[debruijn] case class TransientKmerVertex(terminals: Set[(Long, Int)] = Set.empty,
                                                 stronglyConnected: Map[(Long, Int), Long] = Map.empty,
                                                 linked: Map[(Long, Int), Long] = Map.empty) {
}
