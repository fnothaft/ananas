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

import org.scalatest.FunSuite

class IntMerSuite extends FunSuite {

  test("cannot create a k-mer with len != 16") {
    intercept[IllegalArgumentException] {
      IntMer("ACACTG")
    }

    intercept[IllegalArgumentException] {
      IntMer("ACACTGACACTGACACTG")
    }
  }

  test("cannot create a k-mer with bad chars") {
    intercept[IllegalArgumentException] {
      IntMer("ACACACACACACACAY")
    }
  }

  test("build a proper k-mer") {
    val kmer = IntMer("ACACACACTGTGTGTG")

    assert(kmer.kmer === 0x1111EEEE)
    assert(kmer.mask === 0x0)
    assert(kmer.rcKmer === 0x4444BBBB)
    assert(kmer.rcMask === 0x0)
    assert(kmer.toString === "ACACACACTGTGTGTG")
  }

  test("build a masked k-mer") {
    val kmer = IntMer("ACACACACTGTGTNTG")

    assert(kmer.kmer === 0x1111EECE)
    assert(kmer.mask === 0x30)
    assert(kmer.rcKmer === 0x4C44BBBB)
    assert(kmer.rcMask === 0xC000000)
    assert(kmer.toString === "ACACACACTGTGTNTG")
  }

  test("compare compatible proper and masked k-mers") {
    val kmer = IntMer("ACACACACTGTGTGTG")
    val mmer = IntMer("ACACACACTGTGTNTG")
    
    assert(kmer.equals(mmer))
  }

  test("compare reverse compliment k-mers") {
    val kmer = IntMer("ACACACACTGTGTGTG")
    val rmer = IntMer("CACACACAGTGTGTGT")

    assert(kmer.isCanonical)
    assert(!rmer.isCanonical)
    assert(kmer.equals(rmer))
  }

  test("build an array of k-mers from a sequence") {
    val kArray = IntMer.fromSequence("ACACACACTGTGTGTGCACA")

    assert(kArray.length === 5)
    assert(kArray(0) === IntMer("ACACACACTGTGTGTG"))
    assert(kArray(1) === IntMer("CACACACTGTGTGTGC"))
    assert(kArray(2) === IntMer("ACACACTGTGTGTGCA"))
    assert(kArray(3) === IntMer("CACACTGTGTGTGCAC"))
    assert(kArray(4) === IntMer("ACACTGTGTGTGCACA"))
  }

  test("round trip from k-mers to a sequence and back") {
    assert(IntMer.toSequence(IntMer.fromSequence("ACACACACTGTGTGTGCACA")) === "ACACACACTGTGTGTGCACA")
  }
}
