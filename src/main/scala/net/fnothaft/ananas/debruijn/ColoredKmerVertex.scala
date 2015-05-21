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
import net.fnothaft.ananas.models.CanonicalKmer
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

private[debruijn] object ColoredKmerVertex extends KmerVertexCompanion[ColoredKmerVertex, String] {

  def makeRdd(rdd: RDD[(CanonicalKmer, TransientKmerVertex[String])]): RDD[(Long, ColoredKmerVertex)] = {
    rdd.map(kv => {
      val (km, vertex) = kv
      (km.longHash, ColoredKmerVertex(km,
                                      vertex.terminals,
                                      vertex.stronglyConnected))
    })
  }

  def apply(vertex: AvroKmerVertex): ColoredKmerVertex = {
    def long(l: java.lang.Long): Long = l
    def int(i: java.lang.Integer): Int = i

    val kmer = CanonicalKmer(vertex.getKmer)

    val tb = asScalaBuffer(vertex.getTerminalNames)
      .zip(asScalaBuffer(vertex.getTerminalIdx).map(v => int(v)))
      .toSet

    val sc = asScalaBuffer(vertex.getScNames)
      .zip(asScalaBuffer(vertex.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(vertex.getScLink).map(v => long(v)))
      .toMap

    new ColoredKmerVertex(kmer, tb, sc)
  }
}

case class ColoredKmerVertex(kmer: CanonicalKmer,
                             terminals: Set[(String, Int)],
                             stronglyConnected: Map[(String, Int), Long]) extends KmerVertex {

  def toAvro: AvroKmerVertex = {
    val tb = terminals.toBuffer
    val scb = stronglyConnected.toBuffer

    def long(l: Long): java.lang.Long = l
    def int(i: Int): java.lang.Integer = i

    AvroKmerVertex.newBuilder()
      .setKmer(kmer.toAvro)
      .setTerminalNames(bufferAsJavaList(tb.map(_._1)))
      .setTerminalIdx(bufferAsJavaList(tb.map(v => int(v._2))))
      .setScNames(bufferAsJavaList(scb.map(_._1._1)))
      .setScIdx(bufferAsJavaList(scb.map(v => int(v._1._2))))
      .setScLink(bufferAsJavaList(scb.map(v => long(v._2))))
      .build()
  }

  def connectsTo: Iterable[Long] = stronglyConnected.values
}
