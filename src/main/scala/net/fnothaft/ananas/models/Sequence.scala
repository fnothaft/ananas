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

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.minhash.MinHashable

object Sequence extends Serializable {

  def apply(readWithId: (AlignmentRecord, Long)): Sequence = {
    Sequence(readWithId._2, IntMer.fromSequence(readWithId._1.getSequence).map(_.asInstanceOf[CanonicalKmer]))
  }
}

case class Sequence(id: Long,
                    sequenceKmers: Array[CanonicalKmer],
                    name: Option[String] = None) extends MinHashable {

  val length = sequenceKmers.length + 15

  /**
   * Create hashes for this read by breaking it into shingles by splitting
   * it into equal length k-mers, then take the hash code of each k-mer string.
   *
   * @return Returns an array containing the hash code of each k-mer in the read.
   */
  def provideHashes(): Array[Int] = {
    sequenceKmers.map(_.hashCode)
  }

  override def toString: String = {
    "%d: %s%s".format(id,
                      sequenceKmers.head.toString,
                      sequenceKmers.map(_.lastBase).mkString)
  }
}
