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

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.minhash.MinHash

object OverlapGraph extends Serializable {

  private def hashReads(reads: RDD[AlignmentRecord]): RDD[MinHashableSequence] = {
    reads.zipWithUniqueId
      .map(MinHashableSequence(_))
  }

  def overlapReads(reads: RDD[AlignmentRecord],
                   overlapLength: Int,
                   signatureLength: Int,
                   bands: Int,
                   seed: Option[Long]): Graph[MinHashableSequence, Overlap] = {

    // hash and cache the reads
    val hashedReads = hashReads(reads)
      .cache()

    // compute overlaps
    val overlaps = if (bands == 1) {
      MinHash.exactMinHash(hashedReads,
                           signatureLength,
                           seed)
    } else {
      MinHash.approximateMinHash(hashedReads,
                                 signatureLength,
                                 bands,
                                 seed)
    }
      
    // filter out bad overlaps and create edges
    val overlapEdges = overlaps.flatMap(Overlap(_, overlapLength))

    // create graph
    val g = Graph(hashedReads.map(r => (r.id, r)), overlapEdges)

    // unpersist reads
    hashedReads.unpersist()

    g
  }
}
