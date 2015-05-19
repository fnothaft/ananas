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

import net.fnothaft.ananas.avro.{ Backing, Kmer }
import scala.annotation.tailrec

object IntMer {
  
  private def cVal(c: Char): (Int, Int) = c match {
    case 'A' => (0, 0)
    case 'C' => (1, 0)
    case 'G' => (2, 0)
    case 'T' => (3, 0)
    case 'N' => (0, 3)
  }
  
  @tailrec private def constructKmer(c: Iterator[Char],
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

  def apply(str: String): IntMer = {
    // check input length
    require(str.length == 16, "K-mer must have length 16 (%s).".format(str))

    try {
      val (kmer, mask, rcKmer, rcMask) = constructKmer(str.toIterator)
      if (kmer < rcKmer) {
        new IntMer(kmer, mask, true)
      } else {
        new IntMer(rcKmer, rcMask, false)
      }
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
    val (kmer, mask, rcKmer, rcMask) = constructKmer(seq.take(16).toIterator)
    kArray(0) = if (kmer < rcKmer) {
      new IntMer(kmer, mask, true)
    } else {
      new IntMer(rcKmer, rcMask, false)
    }

    @tailrec def extendKmer(c: Iterator[Char],
                            kmer: Int = 0,
                            mask: Int = 0,
                            rcKmer: Int = 0,
                            rcMask: Int = 0,
                            idx: Int = 0) {
      if (c.hasNext) {
        val n = c.next
        val (k, m) = cVal(n)
        
        // shift in from right to get new kmer and mask
        val newKmer = (kmer << 2) | k
        val newMask = (mask << 2) | m

        // shift in from left to get new reverse kmer and reverse mask
        val shift = ~(k << 30) & 0xC0000000
        val newRcKmer = (rcKmer >>> 2) | (shift)
        val newRcMask = (rcMask >>> 2) | (m << 30)

        // insert k-mer into array
        kArray(idx + 1) = if (newKmer < newRcKmer) {
          new IntMer(newKmer, newMask, true)
        } else {
          new IntMer(newRcKmer, newRcMask, false)
        }

        // recurse
        extendKmer(c, newKmer, newMask, newRcKmer, newRcMask, idx + 1)
      }
    }

    // insert k-mers into array
    extendKmer(seq.toIterator.drop(16), kmer, mask, rcKmer, rcMask)

    kArray
  }

  def toSequence(array: Array[IntMer]): String = {
    "%s%s".format(array.head.toOriginalString,
                  array.tail
                    .map(_.originalLastBase)
                    .mkString)
  }
}

case class IntMer(kmer: Int,
                  mask: Int,
                  isOriginal: Boolean) extends CanonicalKmer {

  def kmerLength: Int = 16

  def toAvro: Kmer = {
    Kmer.newBuilder()
      .setFormat(Backing.INT)
      .setIsOriginal(isOriginal)
      .setIntKmer(kmer)
      .setIntMask(mask)
      .build()
  }
  
  def flipCanonicality: IntMer = {
    @tailrec def flip(k: Int,
                      m: Int,
                      nk: Int = 0,
                      nm: Int = 0,
                      i: Int = 16): (Int, Int) = {
      if (i <= 0) {
        (~nk, nm)
      } else {
        flip(k >>> 2, m >>> 2, (nk << 2) | (k & 0x3), (nm << 2) | (m & 0x3))
      }
    }

    val (newKmer, newMask) = flip(kmer, mask)
    new IntMer(newKmer, newMask, !isOriginal)
  }

  override def hashCode: Int = kmer | mask

  def longHash: Long = hashCode.toLong

  def equals(o: IntMer): Boolean = {
    (~(kmer ^ o.kmer) | mask | o.mask) == 0xFFFFFFFF
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

  private def getRcBase(k: Int,
                        m: Int): Char = {
    if ((m & 0xC0000000) != 0) {
      'N'
    } else {
      (k & 0xC0000000) match {
        case 0x00000000 => 'T'
        case 0x40000000 => 'G'
        case 0x80000000 => 'C'
        case _          => 'A'
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

  def toOriginalString: String = {
    if (isOriginal) {
      toString
    } else {
      @tailrec def buildRcString(shiftKmer: Int,
                                 shiftMask: Int,
                                 a: Array[Char],
                                 i: Int = 15): String = {
        if (i < 0) {
          a.mkString
        } else {
          a(i) = getRcBase(shiftKmer, shiftMask)
          buildRcString(shiftKmer << 2, shiftMask << 2, a, i - 1)
        }
      }

      buildRcString(kmer, mask, new Array[Char](16))      
    }
  }

  def lastBase: Char = {
    getBase(kmer, mask)
  }

  def originalLastBase: Char = {
    if (isOriginal) {
      lastBase
    } else {
      getRcBase(kmer, mask)
    }
  }
}
