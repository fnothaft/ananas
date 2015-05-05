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

import scala.annotation.tailrec

object IntMer {
  
  private def cVal(c: Char): (Int, Int) = c match {
    case 'A' => (0, 0)
    case 'C' => (1, 0)
    case 'G' => (2, 0)
    case 'T' => (3, 0)
    case 'N' => (0, 3)
  }
  
  def apply(str: String): IntMer = {
    // check input length
    require(str.length == 16, "K-mer must have length 16 (%s).".format(str))

    @tailrec def constructKmer(c: Iterator[Char],
                               kmer: Int = 0,
                               mask: Int = 0,
                               rKmer: Int = 0,
                               rMask: Int = 0): (Int, Int, Int, Int) = {
      if (!c.hasNext) {
        // we haven't complimented the reverse compilment kmer yet, so invert
        (kmer, mask, ~rKmer, rMask)
      } else {
        val n = c.next
        val (k, m) = cVal(n)
        
        // shift in from right to get new kmer and mask
        val newKmer = (kmer << 2) | k
        val newMask = (mask << 2) | m

        // shift in from left to get new reverse kmer and reverse mask
        val shift = k << 30
        val newRKmer = (rKmer >>> 2) | (shift)
        val newRMask = (rMask >>> 2) | (m << 30)

        // recurse
        constructKmer(c, newKmer, newMask, newRKmer, newRMask)
      }
    }

    try {
      val (kmer, mask, rcKmer, rcMask) = constructKmer(str.toIterator)
      new IntMer(kmer, mask, rcKmer, rcMask)
    } catch {
      case t : MatchError => {
        throw new IllegalArgumentException("Received illegal k-mer string. %s has bad chars.".format(str))
      }
    }
  }

  def fromSequence(seq: String): Array[IntMer] = {
    // check input length
    require(seq.length >= 16, "Sequence must be at least 16 bases (%s).".format(seq))

    // preallocate k-mer array
    val kArray = new Array[IntMer](seq.length - 15)

    // calculate first k-mer
    kArray(0) = IntMer(seq.take(16))

    @tailrec def extendKmer(c: Iterator[Char],
                            idx: Int = 0) {
      if (c.hasNext) {
        val n = c.next
        val (k, m) = cVal(n)
        
        // shift in from right to get new kmer and mask
        val newKmer = (kArray(idx).kmer << 2) | k
        val newMask = (kArray(idx).mask << 2) | m

        // shift in from left to get new reverse kmer and reverse mask
        val shift = ~(k << 30) & 0xC0000000
        val newRcKmer = (kArray(idx).rcKmer >>> 2) | (shift)
        val newRcMask = (kArray(idx).rcMask >>> 2) | (m << 30)

        // insert k-mer into array
        kArray(idx + 1) = IntMer(newKmer, newMask, newRcKmer, newRcMask)

        // recurse
        extendKmer(c, idx + 1)
      }
    }

    // insert k-mers into array
    extendKmer(seq.toIterator.drop(16))

    kArray
  }
}

case class IntMer(kmer: Int,
                  mask: Int,
                  rcKmer: Int,
                  rcMask: Int) {
  val (canonicalKmer, canonicalMask, isCanonical) = if (kmer > rcKmer) {
    (rcKmer, rcMask, false)
  } else {
    (kmer, mask, true)
  }

  override def hashCode: Int = canonicalKmer | canonicalMask

  def equals(o: IntMer): Boolean = {
    (~(canonicalKmer ^ o.canonicalKmer) | canonicalMask | o.canonicalMask) == 0xFFFFFFFF
  }

  private def getBase(k: Int,
                      m: Int): Char = {
    if ((m & 0x3) != 0) {
      'N'
    } else {
      (k & 0x3) match {
        case 0 => 'A'
        case 1 => 'C'
        case 2 => 'G'
        case _ => 'T'
      }
    }
  }

  override def toString: String = {
    @tailrec def buildString(shiftKmer: Int,
                             shiftMask: Int,
                             a: Array[Char],
                             i: Int = 15): String = {
      if (i < 0) {
        a.mkString
      } else {
        a(i) = getBase(shiftKmer, shiftMask)
        buildString(shiftKmer >>> 2, shiftMask >>> 2, a, i - 1)
      }
    }
    
    buildString(kmer, mask, new Array[Char](16))
  }

  def lastBase: Char = {
    getBase(kmer, mask)
  }
}
