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
package net.fnothaft.ananas.models

import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }
import org.scalatest.FunSuite

class ContigFragmentSuite extends FunSuite {

  def c(s: String): CanonicalKmer = {
    IntMer(s).asInstanceOf[CanonicalKmer]
  }
  
  test("build and flatten a fragment from a single NCF") {
    val ncf = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(1)
      .setFragmentSequence("ACACTGTGGGTACACTACGAGA")
      .build()

    val frag = ContigFragment.buildFromNCF(ncf)

    assert(frag.id === "ctg")
    assert(frag.isLast)
    assert(frag.startPos === 0)
    assert(frag.sequence.length === 7)
    assert(frag.sequence(0) === c("ACACTGTGGGTACACT"))
    assert(frag.sequence(1) === c("CACTGTGGGTACACTA"))
    assert(frag.sequence(2) === c("ACTGTGGGTACACTAC"))
    assert(frag.sequence(3) === c("CTGTGGGTACACTACG"))
    assert(frag.sequence(4) === c("TGTGGGTACACTACGA"))
    assert(frag.sequence(5) === c("GTGGGTACACTACGAG"))
    assert(frag.sequence(6) === c("TGGGTACACTACGAGA"))
    
    val flat = frag.flattenFragment

    assert(flat.length === 7)
    assert(flat(0)._1 === c("ACACTGTGGGTACACT"))
    assert(flat(0)._2.stronglyConnected(("ctg", 0)) === c("CACTGTGGGTACACTA").longHash)
    assert(flat(1)._1 === c("CACTGTGGGTACACTA"))
    assert(flat(1)._2.stronglyConnected(("ctg", 1)) === c("ACTGTGGGTACACTAC").longHash)
    assert(flat(2)._1 === c("ACTGTGGGTACACTAC"))
    assert(flat(2)._2.stronglyConnected(("ctg", 2)) === c("CTGTGGGTACACTACG").longHash)
    assert(flat(3)._1 === c("CTGTGGGTACACTACG"))
    assert(flat(3)._2.stronglyConnected(("ctg", 3)) === c("TGTGGGTACACTACGA").longHash)
    assert(flat(4)._1 === c("TGTGGGTACACTACGA"))
    assert(flat(4)._2.stronglyConnected(("ctg", 4)) === c("GTGGGTACACTACGAG").longHash)
    assert(flat(5)._1 === c("GTGGGTACACTACGAG"))
    assert(flat(5)._2.stronglyConnected(("ctg", 5)) === c("TGGGTACACTACGAGA").longHash)
    assert(flat(6)._1 === c("TGGGTACACTACGAGA"))
    assert(flat(6)._2.terminals(("ctg", 6)))
  }

  test("build and flatten a fragment from a NCF from the middle of a contig") {
    val ncf = NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
      .setContigName("ctg")
      .build())
      .setFragmentStartPosition(20L)
      .setFragmentNumber(1)
      .setNumberOfFragmentsInContig(6)
      .setFragmentSequence("ACACTGTGGGTACACTACGAGA")
      .build()
    
    val frag = ContigFragment.buildFromNCF(ncf)

    assert(frag.id === "ctg")
    assert(!frag.isLast)
    assert(frag.startPos === 20)
    assert(frag.sequence.length === 7)
    assert(frag.sequence(0) === c("ACACTGTGGGTACACT"))
    assert(frag.sequence(1) === c("CACTGTGGGTACACTA"))
    assert(frag.sequence(2) === c("ACTGTGGGTACACTAC"))
    assert(frag.sequence(3) === c("CTGTGGGTACACTACG"))
    assert(frag.sequence(4) === c("TGTGGGTACACTACGA"))
    assert(frag.sequence(5) === c("GTGGGTACACTACGAG"))
    assert(frag.sequence(6) === c("TGGGTACACTACGAGA"))
    
    val flat = frag.flattenFragment

    assert(flat.length === 6)
    assert(flat(0)._1 === c("ACACTGTGGGTACACT"))
    assert(flat(0)._2.stronglyConnected(("ctg", 20)) === c("CACTGTGGGTACACTA").longHash)
    assert(flat(1)._1 === c("CACTGTGGGTACACTA"))
    assert(flat(1)._2.stronglyConnected(("ctg", 21)) === c("ACTGTGGGTACACTAC").longHash)
    assert(flat(2)._1 === c("ACTGTGGGTACACTAC"))
    assert(flat(2)._2.stronglyConnected(("ctg", 22)) === c("CTGTGGGTACACTACG").longHash)
    assert(flat(3)._1 === c("CTGTGGGTACACTACG"))
    assert(flat(3)._2.stronglyConnected(("ctg", 23)) === c("TGTGGGTACACTACGA").longHash)
    assert(flat(4)._1 === c("TGTGGGTACACTACGA"))
    assert(flat(4)._2.stronglyConnected(("ctg", 24)) === c("GTGGGTACACTACGAG").longHash)
    assert(flat(5)._1 === c("GTGGGTACACTACGAG"))
    assert(flat(5)._2.stronglyConnected(("ctg", 25)) === c("TGGGTACACTACGAGA").longHash)
  }
}
