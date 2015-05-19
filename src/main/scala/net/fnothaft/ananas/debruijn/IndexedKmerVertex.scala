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

import net.fnothaft.ananas.avro.KmerVertex
import net.fnothaft.ananas.models.CanonicalKmer
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

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

  def apply(vertex: KmerVertex): IndexedKmerVertex = {
    def long(l: java.lang.Long): Long = l
    def int(i: java.lang.Integer): Int = i

    val kmer = CanonicalKmer(vertex.getKmer)

    val tb = asScalaBuffer(vertex.getTerminalIds).map(v => long(v))
      .zip(asScalaBuffer(vertex.getTerminalIdx).map(v => int(v)))
      .toSet

    val sc = asScalaBuffer(vertex.getScId).map(v => long(v))
      .zip(asScalaBuffer(vertex.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(vertex.getScLink).map(v => long(v)))
      .toMap

    val link = asScalaBuffer(vertex.getLinkId).map(v => long(v))
      .zip(asScalaBuffer(vertex.getLinkIdx).map(v => int(v)))
      .zip(asScalaBuffer(vertex.getLinkLink).map(v => long(v)))
      .toMap

    new IndexedKmerVertex(kmer, tb, sc, link)
  }
}

case class IndexedKmerVertex(kmer: CanonicalKmer,
                             terminals: Set[(Long, Int)],
                             stronglyConnected: Map[(Long, Int), Long],
                             linked: Map[(Long, Int), Long]) {

  def toAvro: KmerVertex = {
    val tb = terminals.toBuffer
    val scb = stronglyConnected.toBuffer
    val lb = linked.toBuffer

    def long(l: Long): java.lang.Long = l
    def int(i: Int): java.lang.Integer = i
    
    KmerVertex.newBuilder()
      .setKmer(kmer.toAvro)
      .setTerminalIds(bufferAsJavaList(tb.map(v => long(v._1))))
      .setTerminalIdx(bufferAsJavaList(tb.map(v => int(v._2))))
      .setScId(bufferAsJavaList(scb.map(v => long(v._1._1))))
      .setScIdx(bufferAsJavaList(scb.map(v => int(v._1._2))))
      .setScLink(bufferAsJavaList(scb.map(v => long(v._2))))
      .setLinkId(bufferAsJavaList(lb.map(v => long(v._1._1))))
      .setLinkIdx(bufferAsJavaList(lb.map(v => int(v._1._2))))
      .setLinkLink(bufferAsJavaList(lb.map(v => long(v._2))))
      .build()
  }
}
