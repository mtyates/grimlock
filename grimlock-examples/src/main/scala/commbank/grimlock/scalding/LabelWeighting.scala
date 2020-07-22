// Copyright 2014,2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ DoubleCodec, StringCodec, Value }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract.ExtractWithKey
import commbank.grimlock.framework.metadata.ContinuousSchema
import commbank.grimlock.framework.position.{ Along, Coordinates1, Over, Position }
import commbank.grimlock.framework.transform.TransformerWithValue

import commbank.grimlock.library.aggregate.Minimum
import commbank.grimlock.library.transform.Fraction

import commbank.grimlock.scalding.Persist
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.implicits._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

// Simple transformer that adds weight to a label.
case class AddWeight[
  P <: HList,
  Q <: HList
](
)(implicit
  ev1: Value.Box[String],
  ev2: Position.AppendConstraints.Aux[P, Value[String], Q]
) extends TransformerWithValue[P, Q] {
  type V = Map[Position[Coordinates1[String]], Content]

  // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
  // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = List(
    Cell(cell.position.append("label"), cell.content),
    Cell(cell.position.append("weight"), ext(Position(cell.content.value.toShortString)))
  )
}

class LabelWeighting(args: Args) extends Job(args) {
  // Define implicit context.
  implicit val ctx = Context()

  import ctx.encoder

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read labels and melt the date into the instance id to generate a 1D matrix.
  val labels = ctx
    .read(
      s"${path}/exampleLabels.txt",
      Persist.textLoader,
      Cell.shortStringParser(
        StringCodec :: StringCodec :: HNil,
        Content.decoder(DoubleCodec, ContinuousSchema[Double]()),
        "|"
      )
    )
    .data // Keep only the data (ignoring errors).
    .contract(_1, _0, Value.concatenate[Value[String], Value[String]](":"))

  // Compute histogram over the label values.
  val histogram = labels
    .histogram(Along(_0))(Locate.AppendContentString, false)

  // Compute the total number of labels and gather result into a Map.
  val sum = labels
    .measure(_0)
    .gatherByPosition(Over(_0))

  // Define extract object to get data out of sum/min map.
  def extractor[
    P <: HList,
    K <% Value[K]
  ](
    key: K
  ) = ExtractWithKey[P, K, Content](key)
    .andThenPresent(_.value.as[Double])

  // Compute the ratio of (total number of labels) / (count for each label).
  val ratio = histogram
    .transformWithValue(sum, Fraction(extractor(0L), true))

  // Find the minimum ratio, and gather the result into a Map.
  val min = ratio
    .summarise(Along(_0))(Minimum().andThenRelocate(_.position.append("min").toOption))
    .gatherByPosition(Over(_0))

  // Divide the ratio by the minimum ratio, and gather the result into a Map.
  val weights = ratio
    .transformWithValue(min, Fraction(extractor("min")))
    .gatherByPosition(Over(_0))

  // Re-read labels and add the computed weight.
  ctx
    .read(
      s"${path}/exampleLabels.txt",
      Persist.textLoader,
      Cell.shortStringParser(
        StringCodec :: StringCodec :: HNil,
        Content.decoder(DoubleCodec, ContinuousSchema[Double]()),
        "|"
      )
    )
    .data // Keep only the data (ignoring errors).
    .transformWithValue(weights, AddWeight())
    .saveAsText(ctx, s"./demo.${output}/weighted.out", Cell.toShortString(true, "|"))
    .toUnit
}

