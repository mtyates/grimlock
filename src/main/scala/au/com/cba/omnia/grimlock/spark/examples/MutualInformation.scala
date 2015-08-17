// Copyright 2014,2015 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package au.com.cba.omnia.grimlock.spark.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.squash._

import au.com.cba.omnia.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

// Simple bucketing implementation. For numerical values it generates categorical values that are the rounded up
// value. All other values are passed through.
case class CeilingBucketing() extends Transformer[Position2D, Position2D] {
  def present(cell: Cell[Position2D]): TraversableOnce[Cell[Position2D]] = {
    val con = (cell.content.schema.kind.isSpecialisationOf(Type.Numerical), cell.content.value.asDouble) match {
      case (true, Some(d)) => Content(NominalSchema(LongCodex), math.ceil(d).toLong)
      case _ => cell.content
    }

    Some(Cell(cell.position, con))
  }
}

object MutualInformation {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data.
    // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
    // 2/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
    //    (instance x feature).
    // 3/ Bucket all continuous variables by rounding them.
    val data = loadText(s"${path}/exampleMutual.txt",
        Cell.parse3DWithDictionary(Dictionary.load(s"${path}/exampleDictionary.txt"), Second, third = DateCodex()))
      .squash(Third, PreservingMinPosition[Position3D]())
      .transform(CeilingBucketing())

    // Define type for the histogram count map.
    type W = Map[Position1D, Content]

    // Define extractor for extracting count from histogram count map.
    val extractor = ExtractWithDimension[Dimension.First, Position2D, Content](First)
      .andThenPresent(_.value.asDouble)

    // Compute histogram on the data.
    val mhist = data
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise(Along(First), Count[Position3D, Position2D]())

    // Compute count of histogram elements.
    val mcount = mhist
      .summarise(Over(First), Sum[Position2D, Position1D]())
      .toMap()

    // Compute sum of marginal entropy
    // 1/ Compute the marginal entropy over the features (second dimension).
    // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
    val marginal = mhist
      .summariseWithValue(Over(First), Entropy(extractor)
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("marginal")), mcount)
      .pairwise(Over(First), Upper, Plus(Locate.OperatorString[Position1D, Position1D]("%s,%s")))

    // Compute histogram on pairwise data.
    // 1/ Generate pairwise values for all upper triangular values.
    // 2/ Expand with content as an extra dimension.
    // 3/ Aggregate out second dimension to get histogram counts.
    val jhist = data
      .pairwise(Over(Second), Upper, Concatenate(Locate.OperatorString[Position1D, Position1D]("%s,%s")))
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise(Along(Second), Count[Position3D, Position2D]())

    // Compute count of histogram elements.
    val jcount = jhist
      .summarise(Over(First), Sum[Position2D, Position1D]())
      .toMap()

    // Compute joint entropy
    val joint = jhist
      .summariseWithValue(Over(First), Entropy(extractor, negate = true)
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("joint")), jcount)

    // Generate mutual information
    // 1/ Sum marginal and negated joint entropy
    // 2/ Persist mutual information.
    (marginal ++ joint)
      .summarise(Over(First), Sum[Position2D, Position1D]())
      .save(s"./demo.${output}/mi.out")
  }
}

