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
package net.fnothaft.ananas

import net.fnothaft.ananas.graph.{ EmitAssembly, TransitiveReduction }
import net.fnothaft.ananas.overlapping.OverlapGraph
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Ananas extends BDGCommandCompanion {
  val commandName = "ananas"
  val commandDescription = "Find Interesting regulatory Grammar modifying variants"

  def apply(cmdLine: Array[String]) = {
    new Ananas(Args4j[AnanasArgs](cmdLine))
  }
}

class AnanasArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "READS", usage = "The unaligned reads to assemble.", index = 0)
  var reads: String = null

  @Argument(required = true, metaVar = "ASSEMBLY", usage = "The assembly output.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-min_overlap", usage = "The minimum overlap length to consider.")
  var minOverlap: Int = 500

  @Args4jOption(required = false, name = "-signature_length", usage = "The length of MinHash signatures to use.")
  var signatureLength: Int = 512

  @Args4jOption(required = false, name = "-bucket_count", usage = "The number of buckets to use.")
  var buckets: Int = 32
}

class Ananas(protected val args: AnanasArgs) extends BDGSparkCommand[AnanasArgs] {
  val companion = Ananas

  def run(sc: SparkContext) {
    // load reads
    val reads = sc.loadAlignments(args.reads)

    sc.setCheckpointDir("checkpoint")

    // run overlapping
    val overlapGraph = OverlapGraph.overlapReads(reads,
                                                 args.minOverlap,
                                                 args.signatureLength,
                                                 args.buckets,
                                                 None)
    overlapGraph.checkpoint()

    // reduce to a string graph
    val stringGraph = TransitiveReduction(overlapGraph)
    stringGraph.checkpoint()

    // assemble contigs
    val assembly = EmitAssembly(stringGraph)
    assembly.checkpoint()

    // save
    assembly.adamParquetSave(args.outputPath)
  }
}
