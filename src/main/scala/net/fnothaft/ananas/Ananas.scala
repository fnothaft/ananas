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

class AnanasArgs extends Args4jBase {
  @Argument(required = true, metaVar = "READS", usage = "The unaligned reads to assemble.", index = 0)
  var genotypes: String = null

  @Argument(required = true, metaVar = "ASSEMBLY", usage = "The assembly output.", index = 1)
  var assembly: String = null

  @Args4jOption(required = false, name = "-min_overlap", usage = "The minimum overlap length to consider.")
  var minOverlap: Int = 500
}

class Ananas(protected val args: AnanasArgs) extends BDGSparkCommand[AnanasArgs] {
  val companion = Ananas

  def run(sc: SparkContext) {
    ???
  }
}
