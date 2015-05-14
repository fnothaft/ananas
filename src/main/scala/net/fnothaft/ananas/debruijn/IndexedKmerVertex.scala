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
import org.apache.spark.rdd.RDD

private[debruijn] object IndexedKmerVertex extends Serializable {

  def makeRdd(rdd: RDD[(CanonicalKmer, TransientKmerVertex)]): RDD[(Long, IndexedKmerVertex)] = {
    rdd.map(kv => {
      val (km, vertex) = kv
      (km.longHash, IndexedKmerVertex(km,
                                      vertex.terminals,
                                      vertex.stronglyConnected,
                                      vertex.linked))
    })
  }
}

case class IndexedKmerVertex(kmer: CanonicalKmer,
                             terminals: Set[(Long, Int)],
                             stronglyConnected: Map[(Long, Int), Long],
                             linked: Map[(Long, Int), Long]) {
}
