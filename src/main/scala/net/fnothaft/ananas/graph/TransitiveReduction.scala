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
import org.apache.spark.graphx._
import scala.annotation.tailrec
import scala.math.{ max, min }

// this class exists because graphx's joinVertices operator requires
// the graph to have the same _vertex_ types after the join! frustrating.
private[graph] case class VertexWithEdges(v: MinHashableSequence,
                                          edges: Array[Edge[Overlap]] = Array.empty,
                                          keepEdges: Set[VertexId] = Set.empty) {


  def neighbors: Array[VertexId] = edges.map(e => {
    if (e.srcId == v.id) {
      e.dstId
    } else {
      e.srcId
    }
  })

  def keepEdge(vid: VertexId): Boolean = {
    keepEdges(vid)
  }
}

object TransitiveReduction extends Serializable {

  def apply(graph: Graph[MinHashableSequence, Overlap]): Graph[MinHashableSequence, Overlap] = {
    // get our edges
    val localEdges = graph.collectEdges(EdgeDirection.Either)

    // join with graph
    val graphWithEdges = graph.mapVertices((vid, v) => VertexWithEdges(v))
      .joinVertices(localEdges)((vid, vd, v) => VertexWithEdges(vd.v, v))

    // unpersist original graph
    graph.unpersist()

    // aggregate messages
    val aggregatedMsgs = graphWithEdges.aggregateMessages((ctx: EdgeContext[VertexWithEdges, Overlap, Array[(VertexId, VertexId)]]) => {
      ctx.sendToDst(ctx.srcAttr.neighbors.map(i => (ctx.srcId, i)))
      ctx.sendToSrc(ctx.dstAttr.neighbors.map(i => (ctx.dstId, i)))
    }, (a1: Array[(VertexId, VertexId)], a2: Array[(VertexId, VertexId)]) => a1 ++ a2, new TripletFields(true, true, false))

    // here be dragons
    // this function joins the prior vertex with all neighbors against
    // it's message set, and uses this to filter out all 
    def joinFn(vertexId: VertexId,
               vertex: VertexWithEdges,
               messages: Array[(VertexId, VertexId)]): VertexWithEdges = {

      // get start edges
      val startEdges: Map[VertexId, Int] = vertex.edges.filter(e => {
        if (e.srcId == vertex.v.id) {
          e.attr.alignmentPosition1 == Position.START
        } else {
          e.attr.alignmentPosition2 == Position.START
        }
      }).map(e => {
        if (e.srcId == vertex.v.id) {
          (e.dstId, e.attr.overlapSize)
        } else {
          (e.srcId, e.attr.overlapSize)
        }
      }).toMap

      // get end edges
      val endEdges: Map[VertexId, Int] = vertex.edges.filter(e => {
        if (e.srcId == vertex.v.id) {
          e.attr.alignmentPosition1 == Position.END
        } else {
          e.attr.alignmentPosition2 == Position.END
        }
      }).map(e => {
        if (e.srcId == vertex.v.id) {
          (e.dstId, e.attr.overlapSize)
        } else {
          (e.srcId, e.attr.overlapSize)
        }
      }).toMap

      // filter out all messages we received that are spurious
      val goodStartMsgs = messages.filter(v => startEdges.contains(v._1))
      val goodEndMsgs = messages.filter(v => endEdges.contains(v._1))

      // make a map from each tuple by grouping by the smaller id
      val sortStarts = goodStartMsgs.groupBy(v => v._1)
        .map(kv => (kv._1, kv._2.map(v => v._2)))
        .toSeq
        .sortBy(p => startEdges(p._1))
        .reverse
      val sortEnds = goodEndMsgs.groupBy(v => v._1)
        .map(kv => (kv._1, kv._2.map(v => v._2)))
        .toSeq
        .sortBy(p => endEdges(p._1))
        .reverse
      
      // now that god is dead,
      // we traverse the list by ascending overlap size and filter nodes
      def filter(ss: Seq[(VertexId, Array[VertexId])]): Set[VertexId] = {
        // where is your god now
        @tailrec def filterFn(sorted: Seq[(VertexId, Array[VertexId])],
                              currVertex: VertexId,
                              currIterator: Iterator[VertexId],
                              vs: Seq[VertexId] = Seq.empty): Seq[VertexId] = {
          if (sorted.isEmpty) {
            currVertex +: vs
          } else {
            val (nextSorted, nextVertex, nextIterator, nextVs) = if (currIterator.hasNext) {
              val searchValue = currIterator.next
              (sorted.filter(kv => kv._1 == searchValue), currVertex, currIterator, vs)
            } else {
              (sorted.tail, sorted.head._1, sorted.head._2.toIterator, currVertex +: vs)
            }
            filterFn(nextSorted, nextVertex, nextIterator, nextVs)
          }
        }
        
        if (ss.isEmpty) {
          Set.empty
        } else {
          filterFn(ss.tail, ss.head._1, ss.head._2.toIterator).toSet
        }
      }

      VertexWithEdges(vertex.v, keepEdges = filter(sortStarts) ++ filter(sortEnds))
    }

    // join in unnecessarily complex manner with graph... graphx!!!
    val graphButFilterEdges = graphWithEdges.joinVertices(aggregatedMsgs)(joinFn)
    graphWithEdges.unpersist()

    // now, filter out edges and map down to original vertex types
    graphButFilterEdges.subgraph(et => {
      et.srcAttr.keepEdge(et.dstId) || et.dstAttr.keepEdge(et.srcId)
    }).mapVertices((vid, v) => v.v)    
  }
}
