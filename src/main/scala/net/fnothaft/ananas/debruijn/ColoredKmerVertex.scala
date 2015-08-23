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

private[debruijn] object ColoredKmerVertex extends KmerVertexCompanion[ColoredKmerVertex, String] {

  def makeRdd(rdd: RDD[(CanonicalKmer, TransientKmerVertex[String])]): RDD[(Long, ColoredKmerVertex)] = {
    rdd.map(kv => {
      val (km, vertex) = kv
      (km.longHash, ColoredKmerVertex(km,
                                      vertex.forwardTerminals,
                                      vertex.forwardStronglyConnected,
                                      vertex.reverseTerminals,
                                      vertex.reverseStronglyConnected))
    })
  }

  def apply(vertex: AvroKmerVertex): ColoredKmerVertex = {
    def long(l: java.lang.Long): Long = l
    def int(i: java.lang.Integer): Int = i

    val kmer = CanonicalKmer(vertex.getKmer)

    // build forward direction of k-mer
    val fl = vertex.getForwardLink
    val (ftb, fsc) = if (fl != null) {
      (asScalaBuffer(fl.getTerminalNames)
      .zip(asScalaBuffer(fl.getTerminalIdx).map(v => int(v)))
      .toSet, asScalaBuffer(fl.getScNames)
      .zip(asScalaBuffer(fl.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(fl.getScLink).map(v => long(v)))
      .toMap)
    } else {
      (Set.empty[(String, Int)], Map.empty[(String, Int), Long])
    }

    // build reverse direction of k-mer
    val rl = vertex.getReverseLink
    val (rtb, rsc) = if (rl != null) {
      (asScalaBuffer(rl.getTerminalNames)
      .zip(asScalaBuffer(rl.getTerminalIdx).map(v => int(v)))
      .toSet, asScalaBuffer(rl.getScNames)
      .zip(asScalaBuffer(rl.getScIdx).map(v => int(v)))
      .zip(asScalaBuffer(rl.getScLink).map(v => long(v)))
      .toMap)
    } else {
      (Set.empty[(String, Int)], Map.empty[(String, Int), Long])
    }

    new ColoredKmerVertex(kmer, ftb, fsc, rtb, rsc)
  }
}

case class ColoredKmerVertex(kmer: CanonicalKmer,
                             forwardTerminals: Set[(String, Int)],
                             forwardStronglyConnected: Map[(String, Int), Long],
                             reverseTerminals: Set[(String, Int)],
                             reverseStronglyConnected: Map[(String, Int), Long]) extends KmerVertex {

  def toAvro: AvroKmerVertex = {
  
    def long(l: Long): java.lang.Long = l
    def int(i: Int): java.lang.Integer = i

    val builder = AvroKmerVertex.newBuilder()
      .setKmer(kmer.toAvro)

    // build forward link
    val ftb = forwardTerminals.toBuffer
    val fscb = forwardStronglyConnected.toBuffer

    if (ftb.nonEmpty || fscb.nonEmpty) {
      builder.setForwardLink(Link.newBuilder()
        .setTerminalNames(bufferAsJavaList(ftb.map(_._1)))
        .setTerminalIdx(bufferAsJavaList(ftb.map(v => int(v._2))))
        .setScNames(bufferAsJavaList(fscb.map(_._1._1)))
        .setScIdx(bufferAsJavaList(fscb.map(v => int(v._1._2))))
        .setScLink(bufferAsJavaList(fscb.map(v => long(v._2))))
        .build())
    }

    // build forward link
    val rtb = reverseTerminals.toBuffer
    val rscb = reverseStronglyConnected.toBuffer

    if (rtb.nonEmpty || rscb.nonEmpty) {
      builder.setReverseLink(Link.newBuilder()
        .setTerminalNames(bufferAsJavaList(rtb.map(_._1)))
        .setTerminalIdx(bufferAsJavaList(rtb.map(v => int(v._2))))
        .setScNames(bufferAsJavaList(rscb.map(_._1._1)))
        .setScIdx(bufferAsJavaList(rscb.map(v => int(v._1._2))))
        .setScLink(bufferAsJavaList(rscb.map(v => long(v._2))))
        .build())
    }

    builder.build()
  }

  def connectsTo: Iterable[Long] = forwardStronglyConnected.values ++ reverseStronglyConnected.values
}
