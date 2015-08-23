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

import net.fnothaft.ananas.avro.{ AvroKmerVertex, Link }
import net.fnothaft.ananas.models.CanonicalKmer
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

private[debruijn] object IndexedKmerVertex extends KmerVertexCompanion[IndexedKmerVertex, Long] {

  def makeRdd(rdd: RDD[(CanonicalKmer, TransientKmerVertex[Long])]): RDD[(Long, IndexedKmerVertex)] = {
    rdd.map(kv => {
      val (km, vertex) = kv
      (km.longHash, IndexedKmerVertex(km,
                                      vertex.forwardTerminals,
                                      vertex.forwardStronglyConnected,
                                      vertex.forwardLinked,
                                      vertex.reverseTerminals,
                                      vertex.reverseStronglyConnected,
                                      vertex.reverseLinked))
    })
  }

  def apply(vertex: AvroKmerVertex): IndexedKmerVertex = {
    def long(l: java.lang.Long): Long = l
    def int(i: java.lang.Integer): Int = i

    val kmer = CanonicalKmer(vertex.getKmer)

    // build forward direction of k-mer
    val fl = vertex.getForwardLink
    val (ftb, fsc, flink) = if (fl != null) {
      (asScalaBuffer(fl.getTerminalIds).map(v => long(v))
      .zip(asScalaBuffer(fl.getTerminalIdx).map(v => int(v)))
      .toSet, asScalaBuffer(fl.getScId).map(v => long(v))
      .zip(asScalaBuffer(fl.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(fl.getScLink).map(v => long(v)))
      .toMap, asScalaBuffer(fl.getLinkId).map(v => long(v))
      .zip(asScalaBuffer(fl.getLinkIdx).map(v => int(v)))
      .zip(asScalaBuffer(fl.getLinkLink).map(v => long(v)))
      .toMap)
    } else {
      (Set.empty[(Long, Int)],
       Map.empty[(Long, Int), Long],
       Map.empty[(Long, Int), Long])
    }

    // build reverse direction of k-mer
    val rl = vertex.getReverseLink
    val (rtb, rsc, rlink) = if (rl != null) {
      (asScalaBuffer(rl.getTerminalIds).map(v => long(v))
      .zip(asScalaBuffer(rl.getTerminalIdx).map(v => int(v)))
      .toSet, asScalaBuffer(rl.getScId).map(v => long(v))
      .zip(asScalaBuffer(rl.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(rl.getScLink).map(v => long(v)))
      .toMap, asScalaBuffer(rl.getLinkId).map(v => long(v))
      .zip(asScalaBuffer(rl.getLinkIdx).map(v => int(v)))
      .zip(asScalaBuffer(rl.getLinkLink).map(v => long(v)))
      .toMap)
    } else {
      (Set.empty[(Long, Int)],
       Map.empty[(Long, Int), Long],
       Map.empty[(Long, Int), Long])
    }

    new IndexedKmerVertex(kmer, ftb, fsc, flink, rtb, rsc, rlink)
  }
}

case class IndexedKmerVertex(kmer: CanonicalKmer,
                             forwardTerminals: Set[(Long, Int)],
                             forwardStronglyConnected: Map[(Long, Int), Long],
                             forwardLinked: Map[(Long, Int), Long],
                             reverseTerminals: Set[(Long, Int)],
                             reverseStronglyConnected: Map[(Long, Int), Long],
                             reverseLinked: Map[(Long, Int), Long]) extends KmerVertex {

  def toAvro: AvroKmerVertex = {
    def long(l: Long): java.lang.Long = l
    def int(i: Int): java.lang.Integer = i
    
    val builder = AvroKmerVertex.newBuilder()
      .setKmer(kmer.toAvro)

    // populate forward direction
    val ftb = forwardTerminals.toBuffer
    val fscb = forwardStronglyConnected.toBuffer
    val flb = forwardLinked.toBuffer

    if (ftb.nonEmpty || fscb.nonEmpty || flb.nonEmpty) {
      builder.setForwardLink(Link.newBuilder()
      .setTerminalIds(bufferAsJavaList(ftb.map(v => long(v._1))))
      .setTerminalIdx(bufferAsJavaList(ftb.map(v => int(v._2))))
      .setScId(bufferAsJavaList(fscb.map(v => long(v._1._1))))
      .setScIdx(bufferAsJavaList(fscb.map(v => int(v._1._2))))
      .setScLink(bufferAsJavaList(fscb.map(v => long(v._2))))
      .setLinkId(bufferAsJavaList(flb.map(v => long(v._1._1))))
      .setLinkIdx(bufferAsJavaList(flb.map(v => int(v._1._2))))
      .setLinkLink(bufferAsJavaList(flb.map(v => long(v._2))))
      .build())
    }

    // populate reverse direction
    val rtb = reverseTerminals.toBuffer
    val rscb = reverseStronglyConnected.toBuffer
    val rlb = reverseLinked.toBuffer

    if (rtb.nonEmpty || rscb.nonEmpty || rlb.nonEmpty) {
      builder.setReverseLink(Link.newBuilder()
      .setTerminalIds(bufferAsJavaList(rtb.map(v => long(v._1))))
      .setTerminalIdx(bufferAsJavaList(rtb.map(v => int(v._2))))
      .setScId(bufferAsJavaList(rscb.map(v => long(v._1._1))))
      .setScIdx(bufferAsJavaList(rscb.map(v => int(v._1._2))))
      .setScLink(bufferAsJavaList(rscb.map(v => long(v._2))))
      .setLinkId(bufferAsJavaList(rlb.map(v => long(v._1._1))))
      .setLinkIdx(bufferAsJavaList(rlb.map(v => int(v._1._2))))
      .setLinkLink(bufferAsJavaList(rlb.map(v => long(v._2))))
      .build())
    }

    builder.build()
  }

  def connectsTo: Iterable[Long] = {
    forwardStronglyConnected.values ++ forwardLinked.values ++
    reverseStronglyConnected.values ++ reverseLinked.values
  }
}
