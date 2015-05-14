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

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FastqRecordConverter
import org.bdgenomics.adam.io.InterleavedFastqInputFormat
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro.AlignmentRecord
import parquet.hadoop.util.ContextUtil

object Fragment extends Serializable {

  private def readToKmers(read: AlignmentRecord): Array[Array[CanonicalKmer]] = {
    Array(IntMer.fromSequence(read.getSequence).map(_.asInstanceOf[CanonicalKmer]))
  }

  private def readPairToKmers(iter: Iterable[AlignmentRecord]): Array[Array[CanonicalKmer]] = {
    require(iter.size == 2, "Read pair passed does not have exactly two reads.")
    val firstOfPair = iter.head
    val secondOfPair = iter.drop(1).head

    Array(IntMer.fromSequence(firstOfPair.getSequence).map(_.asInstanceOf[CanonicalKmer]),
          IntMer.fromSequence(secondOfPair.getSequence).map(_.asInstanceOf[CanonicalKmer]))
  }

  def loadFromFile(sc: SparkContext,
                   filename: String): RDD[Fragment] = {
    if (filename.endsWith(".ifq")) {
      val converter = new FastqRecordConverter
      val job = HadoopUtil.newJob(sc)
      sc.newAPIHadoopFile(
        filename,
        classOf[InterleavedFastqInputFormat],
        classOf[Void],
        classOf[Text],
        ContextUtil.getConfiguration(job)
      ).map(l => {
        readPairToKmers(converter.convertPair(l))
      }).zipWithUniqueId
        .map(vk => {
          new Fragment(vk._2, vk._1)
        })
    } else if (filename.endsWith(".fq") ||
               filename.endsWith(".fastq")) {
      sc.loadUnpairedFastq(filename)
        .map(readToKmers)
        .zipWithUniqueId
        .map(vk => {
          new Fragment(vk._2, vk._1)
        })
    } else {
      throw new IllegalArgumentException("Filename (%s) must end with either .fastq, .fq, or .ifq.".format(filename))
    }
  }
}

case class Fragment(id: Long,
                    sequences: Array[Array[CanonicalKmer]]) {
}
