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
import net.fnothaft.ananas.models.Fragment
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.HadoopUtil
import parquet.avro.AvroReadSupport
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.reflect.ClassTag

trait DeBruijnGraph[T <: KmerVertex, L] extends Serializable {
  protected val vertexCompanion: KmerVertexCompanion[T, L]

  def loadFromFile(sc: SparkContext,
                   filepath: String)(implicit tagT: ClassTag[T]): Graph[T, Unit] = {

    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroKmerVertex]])
    val rawVertices = sc.newAPIHadoopFile(filepath,
      classOf[ParquetInputFormat[AvroKmerVertex]],
      classOf[Void],
      classOf[AvroKmerVertex],
      ContextUtil.getConfiguration(job)
    ).map(kv => vertexCompanion(kv._2))

    // map vertices into an RDD
    val vertexRdd = rawVertices.keyBy(_.kmer.longHash)

    // map vertices to edges
    val edgeRdd = vertexRdd.flatMap(kv => {
      val (id, vertex) = kv

      vertex.connectsTo
        .map(v => new Edge[Unit](id, v))
    })
    
    Graph(vertexRdd, edgeRdd)
  }

  def saveToFile(filepath: String,
                 graph: Graph[T, Unit]) {
    graph.vertices
      .map(_._2.toAvro)
      .adamParquetSave(filepath)
  }

  def buildFromFragments[F <: Fragment[L]](fragments: RDD[F])(implicit tTag: ClassTag[T]): Graph[T, Unit] = {
    // flatmap all fragments to transient kmer vertices! w00t
    val kmers = fragments.flatMap(_.flattenFragment)
      .reduceByKey(TransientKmerVertex.merge[L](_, _))
      .cache()

    // map to vertices
    val vertices = vertexCompanion.makeRdd(kmers)

    // map to edges
    val edges = TransientKmerVertex.toEdges[L](kmers)

    // create graph
    val graph = Graph(vertices, edges)

    // unpersist kmer rdd
    kmers.unpersist()

    graph
  }
}
