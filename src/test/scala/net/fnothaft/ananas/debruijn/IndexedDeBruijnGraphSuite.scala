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

import java.nio.file.Files
import net.fnothaft.ananas.AnanasFunSuite
import net.fnothaft.ananas.models.{ CanonicalKmer, Fragment, IntMer }

class IndexedDeBruijnGraphSuite extends AnanasFunSuite {

  sparkTest("building a graph from a single read should give a single component") {
    
    val fragment = Fragment(0L, Array(IntMer.fromSequence("ACACTCTTCCTAGTGTCACATGTGTG")
      .map(_.asInstanceOf[CanonicalKmer])))

    val dbg = IndexedDeBruijnGraph.buildFromFragments(sc.parallelize(Seq(fragment)))

    assert(dbg.vertices.count === 11)
    assert(dbg.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }

  sparkTest("building a graph from reads that overlap should give a single component") {
    
    val fragment0 = Fragment(0L, Array(IntMer.fromSequence("ACACTCTTCCTAGTGTCACATGTGTG")
      .map(_.asInstanceOf[CanonicalKmer])))
    val fragment1 = Fragment(1L, Array(IntMer.fromSequence("TCTTCCTAGTGTCACATGTGTGCATG")
      .map(_.asInstanceOf[CanonicalKmer])))
    val fragment2 = Fragment(2L, Array(IntMer.fromSequence("CCTAGTGTCACATGTGTGCATGGGAC")
      .map(_.asInstanceOf[CanonicalKmer])))

    val dbg = IndexedDeBruijnGraph.buildFromFragments(sc.parallelize(Seq(fragment0,
                                                                         fragment1,
                                                                         fragment2)))

    assert(dbg.vertices.count === 19)
    assert(dbg.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }

  sparkTest("building a graph from reads that are disjoint should yield two components") {
    
    val fragment0 = Fragment(0L, Array(IntMer.fromSequence("ACACTCTTCCTAGTGTCACATGTGTG")
      .map(_.asInstanceOf[CanonicalKmer])))
    val fragment1 = Fragment(1L, Array(IntMer.fromSequence("ACGGACATGTGCAACACATTGTGAAC")
      .map(_.asInstanceOf[CanonicalKmer])))

    val dbg = IndexedDeBruijnGraph.buildFromFragments(sc.parallelize(Seq(fragment0,
                                                                         fragment1)))

    assert(dbg.vertices.count === 22)
    assert(dbg.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 2)
  }

  sparkTest("saving and loading a graph should not change the results") {
    
    val fragment0 = Fragment(0L, Array(IntMer.fromSequence("ACACTCTTCCTAGTGTCACATGTGTG")
      .map(_.asInstanceOf[CanonicalKmer])))
    val fragment1 = Fragment(1L, Array(IntMer.fromSequence("TCTTCCTAGTGTCACATGTGTGCATG")
      .map(_.asInstanceOf[CanonicalKmer])))
    val fragment2 = Fragment(2L, Array(IntMer.fromSequence("CCTAGTGTCACATGTGTGCATGGGAC")
      .map(_.asInstanceOf[CanonicalKmer])))

    val dbg = IndexedDeBruijnGraph.buildFromFragments(sc.parallelize(Seq(fragment0,
                                                                         fragment1,
                                                                         fragment2)))

    val tempFile = Files.createTempDirectory("graph")
    IndexedDeBruijnGraph.saveToFile(tempFile.toAbsolutePath.toString + "/graph",
                                    dbg)
    dbg.unpersist()
    val graph = IndexedDeBruijnGraph.loadFromFile(sc, tempFile.toAbsolutePath.toString + "/graph")

    assert(graph.vertices.count === 19)
    assert(graph.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }
}
