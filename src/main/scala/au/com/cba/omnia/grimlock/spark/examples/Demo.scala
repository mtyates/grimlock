// Copyright 2014-2015 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Nameable._
import au.com.cba.omnia.grimlock.spark.Names._
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.Positions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.Types._

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object BasicOperations {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data. This returns a 2D matrix (instance x feature).
    val data = load2D(s"${path}/exampleInput.txt")

    // Get the number of rows.
    data
      .size(First)
      .save(s"./demo.${output}/row_size.out")

    // Get all dimensions of the matrix.
    data
      .shape()
      .save(s"./demo.${output}/matrix_shape.out")

    // Get the column names.
    data
      .names(Over(Second))
      .save(s"./demo.${output}/column_names.out")

    // Get the type of variables of each column.
    data
      .types(Over(Second), true)
      .save(s"./demo.${output}/column_types.txt")

    // Transpose the matrix.
    data
      .permute(Second, First)
      .save(s"./demo.${output}/transposed.out")

    // Construct a simple query
    def simpleQuery(cell: Cell[Position2D]) = (cell.content.value gtr 995) || (cell.content.value equ "F")

    // Find all co-ordinates that match the above simple query.
    val coords = data
      .which(simpleQuery)
      .save(s"./demo.${output}/query.txt")

    // Get the data for the above coordinates.
    data
      .get(coords)
      .save(s"./demo.${output}/values.txt")

    // Keep columns A and B, and remove row 0221707
    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", false)
      .save(s"./demo.${output}/sliced.txt")
  }
}

object DataSciencePipelineWithFiltering {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data. This returns a 2D matrix (instance x feature).
    val data = load2D(s"${path}/exampleInput.txt")

    // Define a custom partition. If the instance is 'iid:0364354' or 'iid:0216406' then assign it to the right (test)
    // partition. In all other cases assing it to the left (train) partition.
    case class CustomPartition(dim: Dimension, left: String, right: String) extends Partitioner[Position2D, String] {
      def assign(cell: Cell[Position2D]): Collection[String] = {
        if (cell.position(dim).toShortString == "iid:0364354" || cell.position(dim).toShortString == "iid:0216406") {
          Collection(right)
        } else {
          Collection(left)
        }
      }
    }

    // Perform a split of the data into a training and test set.
    val parts = data
      .split[String, CustomPartition](CustomPartition(First, "train", "test"))

    // Get the training data
    val train = parts
      .get("train")

    // Define descriptive statistics to be computed on the training data.
    val dstats: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      StandardDeviation().andThenExpand(_.position.append("sd")),
      Skewness().andThenExpand(_.position.append("skewness")),
      Kurtosis().andThenExpand(_.position.append("kurtosis")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    // Compute descriptive statistics on the training data.
    val descriptive = train
      .summarise[Dimension.First, Position2D, List[Aggregator[Position2D, Position1D, Position2D]]](
        Along[Position2D, Dimension.First](First), dstats)

    // Compute histogram on the categorical features in the training data.
    val histogram = train
      .filter(_.content.schema.kind.isSpecialisationOf(Type.Categorical))
      .expand((c: Cell[Position2D]) => c.position.append(
        "%1$s=%2$s".format(c.position(Second).toShortString, c.content.value.toShortString)))
      .summarise[Dimension.First, Position2D, Count[Position3D, Position2D]](Along[Position3D, Dimension.First](First),
        Count[Position3D, Position2D]())

    // Compute the counts for each categorical features.
    val counts = histogram
      .summarise[Dimension.First, Position1D, Sum[Position2D, Position1D]](Over[Position2D, Dimension.First](First),
        Sum[Position2D, Position1D]())
      .toMap()

    // Define type of the counts map.
    type W = Map[Position1D, Content]

    // Define extractor to extract counts from the map.
    val extractCount = ExtractWithDimension[Dimension.First, Position2D, Content](First)
      .andThenPresent(_.value.asDouble)

    // Define shorthand for AggregatorWithValue type.
    type AwV = AggregatorWithValue[Position2D, Position1D, Position2D] { type V >: W }

    // Define summary statisics to compute on the histogram.
    val sstats: List[AwV] = List(
      Count().andThenExpand(_.position.append("num.cat")),
      Entropy(extractCount).andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy")),
      FrequencyRatio().andThenExpand(_.position.append("freq.ratio")))

    // Compute summary statisics on the histogram.
    val summary = histogram
      .summariseWithValue[Dimension.First, Position2D, List[AwV], W](Over[Position2D, Dimension.First](First),
        sstats, counts)

    // Combine all statistics and write result to file
    val stats = (descriptive ++ histogram ++ summary)
      .save(s"./demo.${output}/stats.out")

    // Determine which features to filter based on statistics. In this case remove all features that occur for 2 or
    // fewer instances. These are removed first to prevent indicator features from being created.
    val rem1 = stats
      .which((cell: Cell[Position2D]) => (cell.position(Second) equ "count") && (cell.content.value leq 2))
      .names(Over(First))

    // Also remove constant features (standard deviation is 0, or 1 category). These are removed after indicators have
    // been created.
    val rem2 = stats
      .which((cell: Cell[Position2D]) => ((cell.position(Second) equ "sd") && (cell.content.value equ 0)) ||
          ((cell.position(Second) equ "num.cat") && (cell.content.value equ 1)))
      .names(Over(First))

    // Finally remove categoricals for which an individual category has only 1 value. These are removed after binarized
    // features have been created.
    val rem3 = stats
      .which((cell: Cell[Position2D]) => (cell.position(Second) like ".*=.*".r) && (cell.content.value equ 1))
      .names(Over(Second))

    // Define type of statistics map.
    type S = Map[Position1D, Map[Position1D, Content]]

    // Define extract object to get data out of statistics map.
    def extractStat(key: String): Extract[Position2D, S, Double] = {
      ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second, key)
        .andThenPresent(_.value.asDouble)
    }

    // List of transformations to apply to each partition.
    val transforms = List(
      Clamp[Position2D, S](extractStat("min"), extractStat("max"))
        .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd"))),
      Binarise[Position2D](Second))

    // Define shorthand for TransformerWithValue type.
    type TwV = TransformerWithValue[Position2D, Position2D] { type V >: S }

    // For each partition:
    //  1/  Remove sparse features;
    //  2/  Create indicator features;
    //  3a/ Remove constant features;
    //  3b/ Clamp features to min/max value of the training data and standardise, binarise categorical features;
    //  3c/ Remove sparse category features;
    //  4a/ Combine preprocessed data sets;
    //  4b/ Optionally fill the matrix (note: this is expensive);
    //  4c/ Save the result as pipe separated CSV for use in modelling.
    def prepare(key: String, partition: RDD[Cell[Position2D]]): RDD[Cell[Position2D]] = {
      val d = partition
        .slice(Over(Second), rem1, false)

      val ind = d
        .transform[Position2D, Transformer[Position2D, Position2D]](Indicator()
          .andThenRename(Transformer.rename(Second, "%1$s.ind")))

      val csb = d
        .slice(Over(Second), rem2, false)
        .transformWithValue[Position2D, List[TwV], S](transforms, stats.toMap(Over[Position2D, Dimension.First](First)))
        .slice(Over(Second), rem3, false)

      (ind ++ csb)
        //.fill(Content(ContinuousSchema[Codex.DoubleCodex], 0))
        .saveAsCSV(Over(Second), s"./demo.${output}/${key}.csv")
    }

    // Prepare each partition.
    parts
      .forEach(List("train", "test"), prepare)
  }
}

object Scoring {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data. This returns a 2D matrix (instance x feature).
    val data = load2D(s"${path}/exampleInput.txt")
    // Read the statistics from the above example.
    val stats = load2D(s"./demo.${output}/stats.out").toMap(Over(First))
    // Read externally learned weights.
    val weights = load1D(s"${path}/exampleWeights.txt").toMap(Over(First))

    // Define type of statistics map.
    type S = Map[Position1D, Map[Position1D, Content]]

    // Type of the weights map.
    type W = Map[Position1D, Content]

    // Define extract object to get data out of statistics map.
    def extractStat(key: String): Extract[Position2D, S, Double] = {
      ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second, key)
        .andThenPresent(_.value.asDouble)
    }

    // For the data do:
    //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
    //  2/ Compute the scored (as a weighted sum);
    //  3/ Save the results.
    val transforms = List(
      Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")),
      Binarise[Position2D](Second),
      Clamp[Position2D, S](extractStat("min"), extractStat("max"))
        .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd"))))

    // Define shorthand for TransformerWithValue type.
    type TwV = TransformerWithValue[Position2D, Position2D] { type V >: S }

    // Define extract object to get data out of weights map.
    val extractWeight = ExtractWithDimension[Dimension.Second, Position2D, Content](Second)
      .andThenPresent(_.value.asDouble)

    data
      .transformWithValue[Position2D, List[TwV], S](transforms, stats)
      .summariseWithValue[Dimension.First, Position1D, WeightedSum[Position2D, Position1D, W], W](
        Over[Position2D, Dimension.First](First), WeightedSum[Position2D, Position1D, W](extractWeight), weights)
      .save(s"./demo.${output}/scores.out")
  }
}

object DataQualityAndAnalysis {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data. This returns a 2D matrix (instance x feature).
    val data = load2D(s"${path}/exampleInput.txt")

    // Define shorthand for List[Aggregator] type.
    type LA = List[Aggregator[Position1D, Position0D, Position1D]]

    // Define moments to compute.
    val moments: LA = List(
      Mean().andThenExpand(_.position.append("mean")),
      StandardDeviation().andThenExpand(_.position.append("sd")),
      Skewness().andThenExpand(_.position.append("skewness")),
      Kurtosis().andThenExpand(_.position.append("kurtosis")))

    // For the instances:
    //  1/ Compute the number of features for each instance;
    //  2/ Save the counts;
    //  3/ Compute the moments of the counts;
    //  4/ Save the moments.
    data
      .summarise[Dimension.First, Position1D, Count[Position2D, Position1D]](
        Over[Position2D, Dimension.First](First), Count[Position2D, Position1D]())
      .save(s"./demo.${output}/feature_count.out")
      .summarise[Dimension.First, Position1D, LA](Along[Position1D, Dimension.First](First), moments)
      .save(s"./demo.${output}/feature_density.out")

    // For the features:
    //  1/ Compute the number of instance that have a value for each features;
    //  2/ Save the counts;
    //  3/ Compute the moments of the counts;
    //  4/ Save the moments.
    data
      .summarise[Dimension.Second, Position1D, Count[Position2D, Position1D]](
        Over[Position2D, Dimension.Second](Second), Count[Position2D, Position1D]())
      .save(s"./demo.${output}/instance_count.out")
      .summarise[Dimension.First, Position1D, LA](Along[Position1D, Dimension.First](First), moments)
      .save(s"./demo.${output}/instance_density.out")
  }
}

object LabelWeighting {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read labels and melt the date into the instance id to generate a 1D matrix.
    val labels = load2DWithSchema(s"${path}/exampleLabels.txt", ContinuousSchema[Codex.DoubleCodex]())
      .melt(Second, First, ":")

    // Compute histogram over the label values.
    val histogram = labels
      .expand((c: Cell[Position1D]) => c.position.append(c.content.value.toShortString))
      .summarise[Dimension.First, Position1D, Count[Position2D, Position1D]](Along[Position2D, Dimension.First](First),
        Count[Position2D, Position1D]())

    // Compute the total number of labels and store result in a Map.
    val sum = labels
      .size(First)
      .toMap(Over(First))

    // Define extract object to get data out of sum/min map.
    def extractor(key: String): Extract[Position1D, Map[Position1D, Content], Double] = {
      ExtractWithKey[Position1D, String, Content](key).andThenPresent(_.value.asDouble)
    }

    // Type of value
    type V = Map[Position1D, Content]

    // Compute the ratio of (total number of labels) / (count for each label).
    val ratio = histogram
      .transformWithValue[Position1D, Fraction[Position1D, V], V](Fraction(extractor(First.toString), true), sum)

    // Find the minimum ratio, and store the result as a Map.
    val min = ratio
      .summarise[Dimension.First, Position1D, Aggregator[Position1D, Position0D, Position1D]](
        Along[Position1D, Dimension.First](First),
        Min[Position1D, Position0D]().andThenExpand(_.position.append("min")))
      .toMap(Over(First))

    // Divide the ratio by the minimum ratio, and store the result as a Map.
    val weights = ratio
      .transformWithValue[Position1D, Fraction[Position1D, V], V](Fraction(extractor("min")), min)
      .toMap(Over(First))

    case class AddWeight() extends TransformerWithValue[Position2D, Position3D] {
      type V = Map[Position1D, Content]

      // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
      // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
      def presentWithValue(cell: Cell[Position2D], ext: V): Collection[Cell[Position3D]] = {
        Collection(List(Cell(cell.position.append("label"), cell.content),
          Cell(cell.position.append("weight"), ext(Position1D(cell.content.value.toShortString)))))
      }
    }

    // Re-read labels and add the computed weight.
    load2DWithSchema(s"${path}/exampleLabels.txt", ContinuousSchema[Codex.DoubleCodex]())
      .transformWithValue[Position3D, AddWeight, AddWeight#V](AddWeight(), weights)
      .save(s"./demo.${output}/weighted.out")
  }
}

