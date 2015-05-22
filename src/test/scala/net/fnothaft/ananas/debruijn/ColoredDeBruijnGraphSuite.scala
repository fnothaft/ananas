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
import net.fnothaft.ananas.models.{ CanonicalKmer, ContigFragment, IntMer }
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }

class ColoredDeBruijnGraphSuite extends AnanasFunSuite {

  sparkTest("building a graph from a single NCF should give a single component") {
    val ncf = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(1)
      .setFragmentSequence("ACACTGTGGGTACACTACGAGA")
      .build()

    val frag = ContigFragment.buildFromNCF(ncf)

    val dbg = ColoredDeBruijnGraph.buildFromFragments(sc.parallelize(Seq(frag)))

    assert(dbg.vertices.count === 7)
    assert(dbg.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }

  sparkTest("building a graph from a single contig with split NCFs should give a single component") {
    val ncf0 = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentStartPosition(0L)
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(2)
      .setFragmentSequence("ACACTGTGGGTACACTACGAGA")
      .build()
    val ncf1 = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentStartPosition(6L)
      .setFragmentNumber(1)
      .setNumberOfFragmentsInContig(2)
      .setFragmentSequence("TGGGTACACTACGAGATCACT")
      .build()

    val frags = Seq(ncf0, ncf1).map(ContigFragment.buildFromNCF)

    val dbg = ColoredDeBruijnGraph.buildFromFragments(sc.parallelize(frags))

    assert(dbg.vertices.count === 12)
    assert(dbg.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }

  sparkTest("saving and loading a graph should not change the results") {
    
    val ncf0 = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentStartPosition(0L)
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(2)
      .setFragmentSequence("ACACTGTGGGTACACTACGAGA")
      .build()
    val ncf1 = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentStartPosition(6L)
      .setFragmentNumber(1)
      .setNumberOfFragmentsInContig(2)
      .setFragmentSequence("TGGGTACACTACGAGATCACT")
      .build()

    val frags = Seq(ncf0, ncf1).map(ContigFragment.buildFromNCF)

    val dbg = ColoredDeBruijnGraph.buildFromFragments(sc.parallelize(frags))

    val tempFile = Files.createTempDirectory("graph")
    ColoredDeBruijnGraph.saveToFile(tempFile.toAbsolutePath.toString + "/graph",
                                    dbg)
    dbg.unpersist()
    val graph = ColoredDeBruijnGraph.loadFromFile(sc, tempFile.toAbsolutePath.toString + "/graph")

    assert(graph.vertices.count === 12)
    assert(graph.connectedComponents()
      .vertices
      .map(_._2)
      .distinct
      .count === 1)
  }
}
