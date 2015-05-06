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
package net.fnothaft.ananas.graph

import net.fnothaft.ananas.overlapping.{ MinHashableSequence, Overlap, Position }
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }

private[graph] trait Msg {
  val id: VertexId
}

private[graph] case class InitMsg() extends Msg {
  val id = -1L
}

private[graph] case class LabelMsg(id: VertexId,
                                   hop: Int,
                                   switchStrands: Boolean,
                                   kmer: IntMer,
                                   pos: Position,
                                   from: VertexId) extends Msg {
}

private[graph] case class RelabelMsg(id: VertexId,
                                     hop: Int,
                                     relabelId: VertexId,
                                     switchStrands: Boolean,
                                     from: VertexId) extends Msg {
}

private[graph] object LabelVertex extends Serializable {

  def apply(sequence: MinHashableSequence,
            edges: Array[Edge[Overlap]]): LabelVertex = {
    LabelVertex(sequence,
                startEdges = edges.filter(e => {
                  if (e.dstId == sequence.id) {
                    e.attr.alignmentPosition1 == Position.START
                  } else {
                    e.attr.alignmentPosition2 == Position.START
                  }
                }).sortBy(e => {
                  if (e.dstId == sequence.id) e.srcId
                  else e.dstId
                }),
                endEdges = edges.filter(e => {
                  if (e.dstId == sequence.id) {
                    e.attr.alignmentPosition1 == Position.END
                  } else {
                    e.attr.alignmentPosition2 == Position.END
                  }
                }).sortBy(e => {
                  if (e.dstId == sequence.id) e.srcId
                  else e.dstId
                }))
  }
}

private[graph] case class Subsequence(from: VertexId,
                                      sequence: Array[IntMer],
                                      round: Int,
                                      to: VertexId = -1L) {
}

private[graph] case class LabelVertex(sequence: MinHashableSequence,
                                      sequences: Array[(VertexId,
                                                        Subsequence)] = Array.empty,
                                      startEdges: Array[Edge[Overlap]] = Array.empty,
                                      endEdges: Array[Edge[Overlap]] = Array.empty) {

  lazy val maxIter = if (sequences.isEmpty) {
    -1
  } else {
    sequences.map(_._2.round).max
  }
  lazy val msgsToSend = sequences.filter(_._2.round == maxIter)
}

object EmitAssembly extends Serializable {

  private def trimEnd(a: Array[IntMer],
                      switchesStrands: Boolean,
                      startKmer: IntMer,
                      endKmer: IntMer): Array[IntMer] = {
    if (switchesStrands) {
      a.takeWhile(_ != startKmer)
    } else {
      a.takeWhile(_ != endKmer)
    }
  }

  private def trimStart(a: Array[IntMer],
                        switchesStrands: Boolean,
                        startKmer: IntMer,
                        endKmer: IntMer): Array[IntMer] = {
    if (switchesStrands) {
      a.dropWhile(_ != endKmer)
    } else {
      a.dropWhile(_ != startKmer)
    }
  }

  private def flip(a: Array[IntMer]): Array[IntMer] = {
    a.map(_.flipCanonicality).reverse
  }

  private def vprog(vid: VertexId,
                    v: LabelVertex,
                    m: Msg): LabelVertex = m match {
                      case initMsg: InitMsg => {
                        if (v.startEdges.isEmpty && !v.endEdges.isEmpty) {
                          val edge = v.endEdges.head
                          val seq = trimStart(v.sequence.sequenceKmers,
                                              edge.attr.switchesStrands, 
                                              edge.attr.startKmer,
                                              edge.attr.endKmer)
                          val edgeId = if (edge.srcId == v.sequence.id) {
                            edge.dstId
                          } else {
                            edge.srcId
                          }

                          LabelVertex(v.sequence,
                                      Array((v.sequence.id, Subsequence(edgeId, seq, 0))),
                                      v.startEdges,
                                      v.endEdges.tail)
                        } else if (!v.startEdges.isEmpty && v.endEdges.isEmpty) {
                          val edge = v.startEdges.head
                          val seq = trimEnd(v.sequence.sequenceKmers,
                                            edge.attr.switchesStrands, 
                                            edge.attr.startKmer,
                                            edge.attr.endKmer)
                          val edgeId = if (edge.srcId == v.sequence.id) {
                            edge.dstId
                          } else {
                            edge.srcId
                          }
                          
                          LabelVertex(v.sequence,
                                      Array((v.sequence.id, Subsequence(edgeId, seq, 0))),
                                      v.startEdges.tail,
                                      v.endEdges)
                        } else {
                          v
                        }
                      }
                      case lblMsg: LabelMsg => {
                        val (newSeq, newStart, newEnd) = if (lblMsg.pos == Position.START) {
                          val edge = v.startEdges.head
                          val to = if (edge.srcId == vid) {
                            edge.dstId
                          } else {
                            edge.srcId
                          }
                          val ss = trimEnd(v.sequence.sequenceKmers,
                                           edge.attr.switchesStrands,
                                           edge.attr.startKmer,
                                           edge.attr.endKmer)
                          ((lblMsg.id, Subsequence(lblMsg.from, ss, lblMsg.hop + 1, to)),
                           v.startEdges.tail,
                           v.endEdges)
                        } else {
                          val edge = v.endEdges.head
                          val to = if (edge.srcId == vid) {
                            edge.dstId
                          } else {
                            edge.srcId
                          }
                          val ss = trimStart(v.sequence.sequenceKmers,
                                             edge.attr.switchesStrands,
                                             edge.attr.startKmer,
                                             edge.attr.endKmer)
                          ((lblMsg.id, Subsequence(lblMsg.from, ss, lblMsg.hop + 1, to)),
                           v.startEdges,
                           v.endEdges.tail)
                        }
                        LabelVertex(v.sequence,
                                    newSeq +: v.sequences,
                                    v.startEdges,
                                    v.endEdges)
                      }
                      case relabelMsg: RelabelMsg => {
                        LabelVertex(v.sequence,
                                    v.sequences.map(s => {
                                      val (id, seq) = s
                                      if (id == relabelMsg.relabelId) {
                                        val ns = if (relabelMsg.switchStrands) {
                                          seq.sequence.reverse
                                        } else {
                                          seq.sequence
                                        }
                                        (relabelMsg.id, Subsequence(relabelMsg.from,
                                                                    ns,
                                                                    relabelMsg.hop + 1,
                                                                    seq.from))
                                      } else {
                                        s
                                      }
                                    }),
                                    v.startEdges,
                                    v.endEdges)
                      }
                    }

  private def sendMsg(et: EdgeTriplet[LabelVertex, Overlap]): Iterator[(VertexId, Msg)] = {
    if (et.srcAttr.maxIter == et.dstAttr.maxIter) {
      // we enforce that src id is always lower than dst id

      // get the vertex we are sending
      val srcVertices = et.srcAttr.msgsToSend.filter(_._2.to == et.dstId)
      val dstVertices = et.dstAttr.msgsToSend.filter(_._2.to == et.srcId)

      if (srcVertices.length > 0 && dstVertices.length > 0) {
        val (id, vertex) = srcVertices.head
        val (oldLabel, _) = dstVertices.head
        
        Iterator((et.dstId, RelabelMsg(id,
                                       vertex.round + 1,
                                       oldLabel,
                                       et.attr.switchesStrands,
                                       et.srcId)))
      } else {
        Iterator.empty
      }
    } else if (et.srcAttr.maxIter > et.dstAttr.maxIter) {
      // get the vertex we are sending
      val srcVertices = et.srcAttr.msgsToSend.filter(_._2.to == et.dstId)

      if (srcVertices.length > 0) {
        val (id, vertex) = srcVertices.head
        
        Iterator((et.dstId, LabelMsg(id,
                                     vertex.round + 1,
                                     et.attr.switchesStrands,
                                     et.attr.startKmer,
                                     et.attr.alignmentPosition2,
                                     et.srcId)))
      } else {
        Iterator.empty
      }
    } else {
      // get the vertex we are sending
      val dstVertices = et.dstAttr.msgsToSend.filter(_._2.to == et.dstId)

      if (dstVertices.length > 0) {
        val (id, vertex) = dstVertices.head
        
        Iterator((et.srcId, LabelMsg(id,
                                     vertex.round + 1,
                                     et.attr.switchesStrands,
                                     et.attr.endKmer,
                                     et.attr.alignmentPosition1,
                                     et.dstId)))
      } else {
        Iterator.empty
      }
    }
  }

  private def merge(msg1: Msg,
                    msg2: Msg): Msg = {
    if (msg1.id <= msg2.id) {
      msg1
    } else {
      msg2
    }
  }

  def apply(graph: Graph[MinHashableSequence, Overlap]): RDD[NucleotideContigFragment] = {
    // get our edges
    val localEdges = graph.collectEdges(EdgeDirection.Either)

    // join with graph
    val graphWithEdges = graph.mapVertices((vid, v) => LabelVertex(v))
      .joinVertices(localEdges)((vid, vd, v) => LabelVertex(vd.sequence, edges = v))

    // pregel it 'til you make it...?
    val taggedAssemblies = Pregel(graphWithEdges,
                                  InitMsg().asInstanceOf[Msg])(vprog,
                                             sendMsg,
                                             merge)
    
    // flat map assemblies down
    taggedAssemblies.vertices
      .flatMap(v => v._2.sequences.map(s => (s._1, (s._2.sequence, s._2.round))))
      .groupByKey()
      .map(kv => {
        val (ctgId, ctgSeq) = kv

        // sort contig sequence and flatten into a string
        val ctg = ctgSeq.toSeq
          .sortBy(_._2)
          .flatMap(_._1)
          .toArray

        NucleotideContigFragment.newBuilder()
          .setContig(Contig.newBuilder()
          .setContigName("contig_%d".format(ctgId))
          .build())
          .setFragmentSequence(IntMer.toSequence(ctg))
          .build()
      })
  }
}
