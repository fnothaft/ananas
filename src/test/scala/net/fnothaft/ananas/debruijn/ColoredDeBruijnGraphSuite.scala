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
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ Alphabet, Symbol }
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

  def getVertexKmerCounts(graph: Graph[ColoredKmerVertex, Unit]): RDD[(String, Int)] = {
    val km = graph.vertices.flatMap(v => {
      val fwdKmerCount = v._2.forwardTerminals.size + v._2.forwardStronglyConnected.size
      val revKmerCount = v._2.reverseTerminals.size + v._2.reverseStronglyConnected.size

      val ks = Seq((v._2.kmer.toCanonicalString, fwdKmerCount),
          (v._2.kmer.toAntiCanonicalString, revKmerCount))
      ks
    }).cache
    km.filter(_._2 != 0)
  }

  sparkTest("build graph from contig file") {
    val file = ClassLoader.getSystemClassLoader.getResource("contigs.fa").getFile

    val fragments = ContigFragment.loadFromFile(sc, file)
    val dbg = ColoredDeBruijnGraph.buildFromFragments(fragments).cache()

    val kmers = dbg.vertices.map(v => v._2.kmer.toOriginalString).cache
    val textKmers = sc.textFile(file)
      .filter(!_.startsWith(">"))
      .flatMap(_.sliding(16))
      .map(v => {
        val str = Alphabet.dna.reverseComplement(v, (c: Char) => Symbol('N', 'N'))
        if (str.compare(v) > 0) {
          v
        } else {
          str
        }
      })

    val textCount = textKmers.countByValue()
    val graphCount = getVertexKmerCounts(dbg).collectAsMap

    var missing = List[String]()

    textCount.foreach(p => {
      val (kmer, count) = p
      assert(graphCount.contains(kmer))
      assert(graphCount(kmer) === count)
    })
    graphCount.foreach(p => {
      val (kmer, count) = p
      assert(textCount.contains(kmer))
      assert(textCount(kmer) === count)
    })
  }
}
