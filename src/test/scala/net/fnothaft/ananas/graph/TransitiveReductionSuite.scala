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

import net.fnothaft.ananas.AnanasFunSuite
import net.fnothaft.ananas.overlapping.{ MinHashableSequence, Overlap, Position }
import org.apache.spark.graphx.{ Edge, Graph }

class TransitiveReductionSuite extends AnanasFunSuite {

  def makeRead(id: Long): (Long, MinHashableSequence) = {
    // we don't need the hashes to be populated
    (id, MinHashableSequence(id, Array.empty))
  }

  sparkTest("run transitive reduction with all reads on the same strand") {
    def mkEdge(sid: Long, eid: Long, size: Int): Edge[Overlap] = {
      Edge(sid, eid, Overlap(false, null, null, Position.END, Position.START, size))
    }
    val reads = sc.parallelize(Seq(makeRead(0L), makeRead(1L), makeRead(2L), makeRead(3L), makeRead(4L)))
    val overlaps = sc.parallelize(Seq(mkEdge(0L, 1L, 500), mkEdge(0L, 2L, 300),
                                      mkEdge(1L, 2L, 500), mkEdge(1L, 3L, 300), mkEdge(1L, 4L, 100),
                                      mkEdge(2L, 3L, 500), mkEdge(2L, 4L, 300),
                                      mkEdge(3L, 4L, 500)))
    val graph = TransitiveReduction(Graph(reads, overlaps).cache())
      .cache()
    
    assert(graph.vertices.count === 5)
    val edges = graph.edges
      .map(ed => (ed.srcId, ed.dstId))
      .collect
    assert(edges.length === 4)
    assert(edges.contains((0L, 1L)))
    assert(edges.contains((1L, 2L)))
    assert(edges.contains((2L, 3L)))
    assert(edges.contains((3L, 4L)))
  }

  sparkTest("run transitive reduction with reads on different strands") {
    def mkEdge(sid: Long, eid: Long, switchStrand: Boolean, size: Int): Edge[Overlap] = {
      if (switchStrand) {
        Edge(sid, eid, Overlap(true, null, null, Position.END, Position.END, size))
      } else {
        Edge(sid, eid, Overlap(false, null, null, Position.END, Position.START, size))
      }
    }
    // 0 -> +; 1,2 -> -; 3 -> +; 4 -> -
    val reads = sc.parallelize(Seq(makeRead(0L), makeRead(1L), makeRead(2L), makeRead(3L), makeRead(4L)))
    val overlaps = sc.parallelize(Seq(mkEdge(0L, 1L, true, 500), mkEdge(0L, 2L, true, 300),
                                      mkEdge(1L, 2L, false, 500), mkEdge(1L, 3L, true, 300), mkEdge(1L, 4L, false, 100),
                                      mkEdge(2L, 3L, true, 500), mkEdge(2L, 4L, false, 300),
                                      mkEdge(3L, 4L, false, 500)))
    val graph = TransitiveReduction(Graph(reads, overlaps).cache())
      .cache()
    
    assert(graph.vertices.count === 5)
    val edges = graph.edges
      .map(ed => (ed.srcId, ed.dstId))
      .collect
    assert(edges.length === 4)
    assert(edges.contains((0L, 1L)))
    assert(edges.contains((1L, 2L)))
    assert(edges.contains((2L, 3L)))
    assert(edges.contains((3L, 4L)))
  }
}
