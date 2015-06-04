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
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

class WalkGraphSuite extends AnanasFunSuite {

  sparkTest("we can use the graph walker to count contig lengths") {
    val vertices = sc.parallelize(Seq((0L, (true, false)), (1L, (false, false)), (2L, (false, true)),
                                      (4L, (true, false)), (5L, (false, false)), (6L, (false, false)), (7L, (false, true))))
    val edges = sc.parallelize(Seq(Edge[Unit](0L, 1L), Edge[Unit](1L, 2L),
                                   Edge[Unit](4L, 5L), Edge[Unit](5L, 6L), Edge[Unit](6L, 7L)))

    val graph = Graph(vertices, edges)

    val lengths = WalkGraph(graph,
                            (vid: VertexId, vd: (Boolean, Boolean)) => {
                              if (vd._1) {
                                Iterable((vid + 1L, (vid, 1)))
                              } else {
                                Iterable.empty[(VertexId, (VertexId, Int))]
                              }
                            },
                            (msgs: Iterable[(VertexId, (VertexId, Int))], vid: VertexId, vd: (Boolean, Boolean)) => {
                              if (vd._2) {
                                (vd, Iterable.empty[(VertexId, (VertexId, Int))], msgs.map(v => (v._2._1, v._2._2 + 1)))
                              } else {
                                (vd, msgs.map(v => (vid + 1L, (v._2._1, v._2._2 + 1))), Iterable.empty[(VertexId, Int)])
                              }
                            },
                            (kv: (VertexId, Int)) => kv)
                              .collectAsMap()

    assert(lengths.size === 2)
    assert(lengths(0L) === 3)
    assert(lengths(4L) === 4)
  }

  sparkTest("correctly run a graph walk algorithm that modifies vertex attributes") {
    val vertices = sc.parallelize(Seq((0L, Some(1L)), (1L, Some(2L)), (2L, None),
                                      (3L, Some(2L)),
                                      (4L, Some(5L)), (5L, Some(6L)), (6L, Some(7L)), (7L, None)))
    val edges = sc.parallelize(Seq(Edge[Unit](0L, 1L), Edge[Unit](1L, 2L),
                                   Edge[Unit](3L, 2L),
                                   Edge[Unit](4L, 5L), Edge[Unit](5L, 6L), Edge[Unit](6L, 7L)))

    val graph = Graph(vertices, edges)

    val lengths = WalkGraph[Option[Long], Unit, (VertexId, Int), (VertexId, Int)](graph,
                            (vid: VertexId, vd: Option[VertexId]) => {
                              if (vid == 0L || vid == 3L || vid == 4L) {
                                Iterable((vd.get, (vid, 1)))
                              } else {
                                Iterable.empty[(VertexId, (VertexId, Int))]
                              }
                            },
                            (msgs: Iterable[(VertexId, (VertexId, Int))], vid: VertexId, vd: Option[Long]) => {
                              if (vd.isEmpty) {
                                (vd, Iterable.empty[(VertexId, (VertexId, Int))], msgs.map(v => (v._2._1, v._2._2)))
                              } else {
                                (None, msgs.map(v => (vd.get, (v._2._1, v._2._2 + 1))), Iterable.empty[(VertexId, Int)])
                              }
                            },
                            (kv: (VertexId, Int)) => kv)
                              .collectAsMap()

    assert(lengths.size === 3)
    assert(lengths(0L) === 2)
    assert(lengths(3L) === 1)
    assert(lengths(4L) === 3)
  }
}
