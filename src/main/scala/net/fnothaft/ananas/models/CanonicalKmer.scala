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
import org.apache.spark.rdd.RDD

object CanonicalKmer extends Serializable {

  def apply(kmer: Kmer): CanonicalKmer = kmer.getFormat match {
    case Backing.INT => {
      IntMer(kmer.getIntKmer,
             kmer.getIntMask,
             kmer.getIsOriginal)
    }
    case _ => throw new IllegalArgumentException("Received illegal k-mer: %s.".format(kmer))
  }

  def apply(rdd: RDD[Kmer]): RDD[CanonicalKmer] = {
    rdd.map(CanonicalKmer(_))
  }
}

trait CanonicalKmer {

  val isOriginal: Boolean

  def kmerLength: Int

  def lastBase: Char
  def originalLastBase: Char

  def toOriginalString: String = {
    if (isOriginal) {
      toCanonicalString
    } else {
      toAntiCanonicalString
    }
  }

  def toCanonicalString: String
  def toAntiCanonicalString: String

  def longHash: Long

  def toAvro: Kmer

  def flipCanonicality: CanonicalKmer

  def sameExceptForOrientation(k: CanonicalKmer): Boolean
}
