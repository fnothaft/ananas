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

import net.fnothaft.ananas.avro.AvroKmerVertex
import net.fnothaft.ananas.models.{ CanonicalKmer, Fragment }
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.HadoopUtil
import parquet.avro.AvroReadSupport
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.annotation.tailrec

object IndexedDeBruijnGraph extends Serializable {

  private def flattenFragment(fragment: Fragment): Array[(CanonicalKmer, TransientKmerVertex)] = {
    val length = fragment.sequences
      .map(_.length)
      .sum

    // create an array to hold the k-mers
    val array = new Array[(CanonicalKmer, TransientKmerVertex)](length)

    @tailrec def flatten(kmer: CanonicalKmer,
                         sequenceIter: Iterator[CanonicalKmer],
                         sequencesIter: Iterator[Array[CanonicalKmer]],
                         idx: Int = 0) {
      if (!sequenceIter.hasNext && !sequencesIter.hasNext) {
        array(idx) = (kmer, TransientKmerVertex(terminals = Set((fragment.id, idx))))
      } else {
        val (nk, nsi) = if (sequenceIter.hasNext) {
          val nextKmer = sequenceIter.next
          array(idx) = (kmer, TransientKmerVertex(stronglyConnected = Map(((fragment.id, idx) -> nextKmer.longHash))))

          (nextKmer, sequenceIter)
        } else {
          val nextSequence = sequencesIter.next
          val nextKmer = nextSequence.head
          array(idx) = (kmer, TransientKmerVertex(linked = Map(((fragment.id, idx) -> nextKmer.longHash))))

          (nextKmer, nextSequence.toIterator.drop(1))
        }

        flatten(nk, nsi, sequencesIter, idx + 1)
      }
    }

    // flatten our fragment
    flatten(fragment.sequences.head.head,
            fragment.sequences.head.toIterator.drop(1),
            fragment.sequences.toIterator.drop(1))

    // return the flattened sequence
    array
  }

  def buildFromFragments(fragments: RDD[Fragment]): Graph[IndexedKmerVertex, Unit] = {
    // flatmap all fragments to transient kmer vertices! w00t
    val kmers = fragments.flatMap(flattenFragment)
      .reduceByKey(TransientKmerVertex.merge)
      .cache()

    // map to vertices
    val vertices = IndexedKmerVertex.makeRdd(kmers)

    // map to edges
    val edges = TransientKmerVertex.toEdges(kmers)

    // create graph
    val graph = Graph(vertices, edges)

    // unpersist kmer rdd
    kmers.unpersist()

    graph
  }

  def loadFromFile(sc: SparkContext,
                   filepath: String): Graph[IndexedKmerVertex, Unit] = {

    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroKmerVertex]])
    val rawVertices = sc.newAPIHadoopFile(filepath,
      classOf[ParquetInputFormat[AvroKmerVertex]],
      classOf[Void],
      classOf[AvroKmerVertex],
      ContextUtil.getConfiguration(job)
    ).map(kv => IndexedKmerVertex(kv._2))

    // map vertices into an RDD
    val vertexRdd = rawVertices.keyBy(_.kmer.longHash)

    // map vertices to edges
    val edgeRdd = vertexRdd.flatMap(kv => {
      val (id, vertex) = kv

      (vertex.stronglyConnected.values ++ vertex.linked.values)
        .map(v => new Edge[Unit](id, v))
    })
    
    Graph(vertexRdd, edgeRdd)
  }

  def saveToFile(filepath: String,
                 graph: Graph[IndexedKmerVertex, Unit]) {
    graph.vertices
      .map(_._2.toAvro)
      .adamParquetSave(filepath)
  }
}
