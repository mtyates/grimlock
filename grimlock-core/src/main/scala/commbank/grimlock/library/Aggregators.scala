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

package commbank.grimlock.library.aggregate

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue, Multiple, Single }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distribution.{ CountMap, StreamingHistogram, TDigest, Quantiles }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.extract.Extract
import commbank.grimlock.framework.metadata.{
  CategoricalType,
  ContinuousSchema,
  DiscreteSchema,
  NumericType
}
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.statistics.Statistics

import com.twitter.algebird.{ Moments => AlgeMoments, Monoid }

import scala.reflect.classTag

import shapeless.HList

private[aggregate] object Aggregate {
  type O[A] = Single[A]

  val oTag = classTag[O[_]]

  def reduce[T](strict: Boolean, invalid: (T) => Boolean, reduction: (T, T) => T)(lt: T, rt: T): T =
    if (invalid(lt)) { if (strict) lt else rt }
    else if (invalid(rt)) { if (strict) rt else lt }
    else reduction(lt, rt)

  def present[
    S <: HList,
    T
  ](
    nan: Boolean,
    invalid: (T) => Boolean,
    missing: (T) => Boolean,
    asDouble: (T) => Double
  )(
    pos: Position[S],
    t: T
  )(implicit
    ev: Value.Box[Double]
  ): O[Cell[S]] =
    if (missing(t) || (invalid(t) && !nan))
      Single()
    else if (invalid(t))
      Single(Cell(pos, Content(ContinuousSchema[Double](), Double.NaN)))
    else
      Single(Cell(pos, Content(ContinuousSchema[Double](), asDouble(t))))

}

private[aggregate] object AggregateDouble {
  type T = Double

  val tTag = classTag[T]

  def prepare[P <: HList](cell: Cell[P], filter: Boolean): Option[Double] =
    if (filter && !cell.content.classification.isOfType(NumericType))
      None
    else
      Option(cell.content.value.as[Double].getOrElse(Double.NaN))

  def reduce(
    strict: Boolean,
    reduction: (T, T) => T
  )(
    lt: T,
    rt: T
  ): T = Aggregate.reduce(strict, invalid, reduction)(lt, rt)

  def present[
    S <: HList
  ](
    nan: Boolean
  )(
    pos: Position[S],
    t: T
  )(implicit
    ev: Value.Box[Double]
  ): Aggregate.O[Cell[S]] = Aggregate.present(nan, invalid, missing, asDouble)(pos, t)

  private def invalid(t: T): Boolean = t.isNaN
  private def missing(t: T): Boolean = false
  private def asDouble(t: T): Double = t
}

private[aggregate] object AggregateMoments {
  type T = AlgeMoments

  val tTag = classTag[T]

  def prepare[P <: HList](cell: Cell[P], filter: Boolean): Option[T] = AggregateDouble.prepare(cell, filter)
    .map { case d => AlgeMoments(d) }

  def reduce(strict: Boolean)(lt: T, rt: T): T = Aggregate.reduce(strict, invalid, reduction)(lt, rt)

  def present[
    S <: HList
  ](
    nan: Boolean,
    asDouble: (T) => Double
  )(
    pos: Position[S],
    t: T
  )(implicit
    ev: Value.Box[Double]
  ): Aggregate.O[Cell[S]] = Aggregate.present(nan, invalid, missing, asDouble)(pos, t)

  def invalid(t: T): Boolean = t.mean.isNaN
  def missing(t: T): Boolean = false
  def reduction(lt: T, rt: T): T = Monoid.plus(lt, rt)
}

/** Count reductions. */
case class Counts[P <: HList, S <: HList]()(implicit ev: Value.Box[Long]) extends Aggregator[P, S, S] {
  type T = Long
  type O[A] = Single[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = Option(1)
  def reduce(lt: T, rt: T): T = lt + rt
  def present(pos: Position[S], t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/** Distinct count reductions. */
case class DistinctCounts[P <: HList, S <: HList]()(implicit ev: Value.Box[Long]) extends Aggregator[P, S, S] {
  type T = Set[Value[_]]
  type O[A] = Single[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = Option(Set(cell.content.value))
  def reduce(lt: T, rt: T): T = lt ++ rt
  def present(pos: Position[S], t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t.size.toLong)))
}

/**
 * Compute counts of values matching a predicate.
 *
 * @param predicte Function to be applied to content.
 */
case class PredicateCounts[
  P <: HList,
  S <: HList
](
  predicate: (Content) => Boolean
)(implicit
  ev: Value.Box[Long]
) extends Aggregator[P, S, S] {
  type T = Long
  type O[A] = Single[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = if (predicate(cell.content)) Option(1) else None
  def reduce(lt: T, rt: T): T = lt + rt
  def present(pos: Position[S], t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/**
 * Moments of a distribution.
 *
 * @param mean     The name for the mean of the distibution
 * @param sd       The name for the standard deviation of the distibution
 * @param skewness The name for the skewness of the distibution
 * @param kurtosis The name for the kurtosis of the distibution
 * @param biased   Indicates if the biased estimate should be returned.
 * @param excess   Indicates if the kurtosis or excess kurtosis should be returned.
 * @param filter   Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                 filtered prior to aggregation.
 * @param strict   Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *                 If not then non-numeric values are silently ignored.
 * @param nan      Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                 data).
 */
case class Moments[
  P <: HList,
  S <: HList,
  Q <: HList
](
  mean: Locate.FromPosition[S, Q],
  sd: Locate.FromPosition[S, Q],
  skewness: Locate.FromPosition[S, Q],
  kurtosis: Locate.FromPosition[S, Q],
  biased: Boolean = false,
  excess: Boolean = false,
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, Q] {
  type T = AggregateMoments.T
  type O[A] = Multiple[A]

  val tTag = AggregateMoments.tTag
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = AggregateMoments.prepare(cell, filter)

  def reduce(lt: T, rt: T): T = AggregateMoments.reduce(strict)(lt, rt)

  def present(pos: Position[S], t: T): O[Cell[Q]] =
    if (AggregateMoments.invalid(t) && !nan)
      Multiple()
    else if (AggregateMoments.invalid(t))
      Multiple(
        List(
          mean(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) },
          sd(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) },
          skewness(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) },
          kurtosis(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) }
        )
        .flatten
      )
    else
      Multiple(
        List(
          mean(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.mean)) },
          sd(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Statistics.sd(t, biased))) },
          skewness(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.skewness)) },
          kurtosis(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Statistics.kurtosis(t, excess))) }
        )
        .flatten
      )
}

/**
 * Mean of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Mean[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateMoments.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateMoments.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateMoments.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateMoments.reduce(strict)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateMoments.present(nan, asDouble)(pos, t)

  private def asDouble(t: T): Double = t.mean
}

/**
 * Standard deviation of a distribution.
 *
 * @param biased Indicates if the biased estimate should be returned.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class StandardDeviation[
  P <: HList,
  S <: HList
](
  biased: Boolean = false,
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateMoments.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateMoments.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateMoments.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateMoments.reduce(strict)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateMoments.present(nan, asDouble)(pos, t)

  private def asDouble(t: T): Double = Statistics.sd(t, biased)
}

/**
 * Skewness of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Skewness[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateMoments.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateMoments.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateMoments.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateMoments.reduce(strict)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateMoments.present(nan, asDouble)(pos, t)

  private def asDouble(t: T): Double = t.skewness
}

/**
 * Kurtosis of a distribution.
 *
 * @param excess Indicates if the kurtosis or excess kurtosis should be returned.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Kurtosis[
  P <: HList,
  S <: HList
](
  excess: Boolean = false,
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateMoments.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateMoments.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateMoments.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateMoments.reduce(strict)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateMoments.present(nan, asDouble)(pos, t)

  private def asDouble(t: T): Double = Statistics.kurtosis(t, excess)
}

/**
 * Limits (minimum/maximum value) reduction.
 *
 * @param min    The name for the minimum value.
 * @param max    The name for the maximum value.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Limits[
  P <: HList,
  S <: HList,
  Q <: HList
](
  min: Locate.FromPosition[S, Q],
  max: Locate.FromPosition[S, Q],
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, Q] {
  type T = (Double, Double)
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter).map { case d => (d, d) }

  def reduce(lt: T, rt: T): T = Aggregate.reduce(strict, invalid, reduction)(lt, rt)

  def present(pos: Position[S], t: T): O[Cell[Q]] =
    if (invalid(t) && !nan)
      Multiple()
    else if (invalid(t))
      Multiple(
        List(
          min(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) },
          max(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), Double.NaN)) }
        )
        .flatten
      )
    else
      Multiple(
        List(
          min(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t._1)) },
          max(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t._2)) }
        )
        .flatten
      )

  private def invalid(t: T): Boolean = t._1.isNaN || t._2.isNaN
  private def reduction(lt: T, rt: T): T = (math.min(lt._1, rt._1), math.max(lt._2, rt._2))
}

/**
 * Minimum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Minimum[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateDouble.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateDouble.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateDouble.reduce(strict, reduction)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateDouble.present(nan)(pos, t)

  private def reduction(lt: T, rt: T): T = math.min(lt, rt)
}

/**
 * Maximum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Maximum[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateDouble.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateDouble.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateDouble.reduce(strict, reduction)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateDouble.present(nan)(pos, t)

  private def reduction(lt: T, rt: T): T = math.max(lt, rt)
}

/**
 * Maximum absolute value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class MaximumAbsolute[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateDouble.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateDouble.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateDouble.reduce(strict, reduction)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateDouble.present(nan)(pos, t)

  private def reduction(lt: T, rt: T): T = math.max(math.abs(lt), math.abs(rt))
}

/**
 * Sum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Sums[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = AggregateDouble.T
  type O[A] = Aggregate.O[A]

  val tTag = AggregateDouble.tTag
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
  def reduce(lt: T, rt: T): T = AggregateDouble.reduce(strict, reduction)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = AggregateDouble.present(nan)(pos, t)

  private def reduction(lt: T, rt: T): T = lt + rt
}

/**
 * Weighted sum reduction. This is particularly useful for scoring linear models.
 *
 * @param weight Object that will extract, for `cell`, its corresponding weight.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class WeightedSums[
  P <: HList,
  S <: HList,
  W
](
  weight: Extract[P, W, Double],
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends AggregatorWithValue[P, S, S] {
  type T = AggregateDouble.T
  type V = W
  type O[A] = Aggregate.O[A]

  val tTag = AggregateDouble.tTag
  val oTag = Aggregate.oTag

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = AggregateDouble.prepare(cell, filter)
    .map { case d => d * weight.extract(cell, ext).getOrElse(0.0) }

  def reduce(lt: T, rt: T): T = AggregateDouble.reduce(strict, reduction)(lt, rt)

  def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[S]] =
    if (t.isNaN && !nan) Single() else Single(Cell(pos, Content(ContinuousSchema[Double](), t)))

  private def reduction(lt: T, rt: T): T = lt + rt
}

/**
 * Compute entropy.
 *
 * @param count  Object that will extract, for `cell`, its corresponding count.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 * @param negate Indicator if negative entropy should be returned.
 * @param log    The log function to use.
 */
case class Entropy[
  P <: HList,
  S <: HList,
  W
](
  count: Extract[P, W, Double],
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false,
  negate: Boolean = false,
  log: (Double) => Double = (x: Double) => math.log(x) / math.log(2)
)(implicit
  ev: Value.Box[Double]
) extends AggregatorWithValue[P, S, S] {
  type T = (Long, Double)
  type V = W
  type O[A] = Single[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = AggregateDouble.prepare(cell, filter)
    .map(v => (1, count.extract(cell, ext).map(c => (v / c) * log(v / c)).getOrElse(Double.NaN)))

  def reduce(lt: T, rt: T): T = (lt._1 + rt._1,
    if (lt._2.isNaN) { if (strict) lt._2 else rt._2 }
    else if (rt._2.isNaN) { if (strict) rt._2 else lt._2 }
    else lt._2 + rt._2
  )

  def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[S]] =
    if (t._1 == 1 || (t._2.isNaN && !nan))
      Single()
    else
      Single(Cell(pos, Content(ContinuousSchema[Double](), if (negate) t._2 else -t._2)))
}

/**
 * Compute frequency ratio.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class FrequencyRatio[
  P <: HList,
  S <: HList
](
  filter: Boolean = true,
  strict: Boolean = true,
  nan: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, S] {
  type T = (Long, Double, Double)
  type O[A] = Aggregate.O[A]

  val tTag = classTag[T]
  val oTag = Aggregate.oTag

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter).map { case d => (1, d, d) }
  def reduce(lt: T, rt: T): T = Aggregate.reduce(strict, invalid, reduction)(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[S]] = Aggregate.present(nan, invalid, missing, asDouble)(pos, t)

  private def invalid(t: T): Boolean = t._2.isNaN
  private def reduction(lt: T, rt: T): T = {
    val high = math.max(lt._2, rt._2)
    val low = if (math.max(lt._3, rt._3) == high) math.min(lt._3, rt._3) else math.max(lt._3, rt._3)

    (lt._1 + rt._1, high, low)
  }
  private def missing(t: T): Boolean = t._1 == 1
  private def asDouble(t: T): Double = t._2 / t._3
}

/**
 * Compute approximate quantiles using a t-digest.
 *
 * @param probs       The quantile probabilities to compute.
 * @param compression The t-digest compression parameter.
 * @param name        Names each quantile output.
 * @param filter      Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                    filtered prior to aggregation.
 * @param nan         Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                    data).
 *
 * @see https://github.com/tdunning/t-digest
 */
case class TDigestQuantiles[
  P <: HList,
  S <: HList,
  Q <: HList
](
  probs: List[Double],
  compression: Double,
  name: Locate.FromPositionWithValue[S, Double, Q],
  filter: Boolean = true,
  nan: Boolean = false
)(implicit
  ev1: Value.Box[Double],
  ev2: Position.GreaterThanConstraints[Q, S]
) extends Aggregator[P, S, Q] {
  type T = TDigest.T
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
    .flatMap { case d => TDigest.from(d, compression) }

  def reduce(lt: T, rt: T): T = TDigest.reduce(lt, rt)

  def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(TDigest.toCells(t, probs, pos, name, nan))
}

/**
 * Compute quantiles using a count map.
 *
 * @param probs     The quantile probabilities to compute.
 * @param quantiser Function that determines the quantile indices into the order statistics.
 * @param name      Names each quantile output.
 * @param filter    Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                  filtered prior to aggregation.
 * @param nan       Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                  data).
 *
 * @note Only use this if all distinct values and their counts fit in memory.
 */
case class CountMapQuantiles[
  P <: HList,
  S <: HList,
  Q <: HList
](
  probs: List[Double],
  quantiser: Quantiles.Quantiser,
  name: Locate.FromPositionWithValue[S, Double, Q],
  filter: Boolean = true,
  nan: Boolean = false
)(implicit
  ev1: Value.Box[Double],
  ev2: Position.GreaterThanConstraints[Q, S]
) extends Aggregator[P, S, Q] {
  type T = CountMap.T
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter).map { case d => CountMap.from(d) }
  def reduce(lt: T, rt: T): T = CountMap.reduce(lt, rt)
  def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(CountMap.toCells(t, probs, pos, quantiser, name, nan))
}

/**
 * Compute `count` uniformly spaced approximate quantiles using an online streaming parallel histogram.
 *
 * @param count  The number of quantiles to compute.
 * @param name   Names each quantile output.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 *
 * @see http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
sealed case class UniformQuantiles[
  P <: HList,
  S <: HList,
  Q <: HList
](
  count: Long,
  name: Locate.FromPositionWithValue[S, Double, Q],
  filter: Boolean = true,
  nan: Boolean = false
)(implicit
  ev1: Value.Box[Double],
  ev2: Position.GreaterThanConstraints[Q, S]
) extends Aggregator[P, S, Q] {
  type T = StreamingHistogram.T
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = AggregateDouble.prepare(cell, filter)
    .map { case d => StreamingHistogram.from(d, count) }

  def reduce(lt: T, rt: T): T = StreamingHistogram.reduce(lt, rt)

  def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(StreamingHistogram.toCells(t, count, pos, name, nan))
}

/**
 * Compute histograms using a CountMap.
 *
 * @param name   Names each histogram value output.
 * @param filter Indicates if only categorical types should be aggregated. Is set then all numeric values are
 *               filtered prior to aggregation.
 *
 * @note Only use this if all distinct values and their counts fit in memory.
 */
case class CountMapHistogram[
  P <: HList,
  S <: HList,
  Q <: HList
](
  name: Locate.FromPositionWithValue[S, Content, Q],
  filter: Boolean = true
)(implicit
  ev1: Value.Box[Long],
  ev2: Position.GreaterThanConstraints[Q, S]
) extends Aggregator[P, S, Q] {
  type T = Map[Content, Long]
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] =
    if (!filter || cell.content.classification.isOfType(CategoricalType))
      Option(Map(cell.content -> 1L))
    else
      None

  def reduce(lt: T, rt: T): T = {
    val (big, small) = if (lt.size > rt.size) (lt, rt) else (rt, lt)

    big ++ small.map { case (k, v) => k -> big.get(k).map { case l => l + v }.getOrElse(v) }
  }

  def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(
    t
      .flatMap { case (c, s) => name(pos, c).map { case p => Cell(p, Content(DiscreteSchema[Long](), s)) } }
      .toList
  )
}

/**
 * Compute confusion matrix.
 *
 * @param outcome            Returns (actual binary outcome, predicted binary outcome) from a `Cell`.
 * @param accuracy           The name for the accuracy.
 * @param f1Score            The name for the F1-score.
 * @param falseDiscoveryRate The name for the false discovery rate.
 * @param falseNegatives     The name for the number of false negatives.
 * @param falsePositives     The name for the number of false positives.
 * @param precision          The name for the precision.
 * @param recall             The name for the recall.
 * @param trueNegatives      The name for the number of true negatives.
 * @param truePositives      The name for the number of true positives.
 */
case class ConfusionMatrix[
  P <: HList,
  S <: HList,
  Q <: HList
](
  outcome: Cell[P] => Option[(Boolean, Boolean)],
  accuracy: Locate.FromPosition[S, Q],
  f1Score: Locate.FromPosition[S, Q],
  falseDiscoveryRate: Locate.FromPosition[S, Q],
  falseNegatives: Locate.FromPosition[S, Q],
  falsePositives: Locate.FromPosition[S, Q],
  precision: Locate.FromPosition[S, Q],
  recall: Locate.FromPosition[S, Q],
  trueNegatives: Locate.FromPosition[S, Q],
  truePositives: Locate.FromPosition[S, Q]
)(implicit
  ev: Value.Box[Double]
) extends Aggregator[P, S, Q] {
  type T = TableOfConfusion
  type O[A] = Multiple[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = outcome(cell).map {
    case (true, true) => TableOfConfusion(tp = 1)
    case (false, true) => TableOfConfusion(fp = 1)
    case (true, false) => TableOfConfusion(fn = 1)
    case (false, false) => TableOfConfusion(tn = 1)
  }

  def reduce(lt: T, rt: T): T = lt + rt

  def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(
    List(
      accuracy(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.accuracy)) },
      f1Score(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.f1Score)) },
      falseDiscoveryRate(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.fdr)) },
      falseNegatives(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.fn.toDouble)) },
      falsePositives(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.fp.toDouble)) },
      precision(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.precision)) },
      recall(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.recall)) },
      trueNegatives(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.tn.toDouble)) },
      truePositives(pos).map { case p => Cell(p, Content(ContinuousSchema[Double](), t.tp.toDouble)) }
    ).flatten
  )
}

/**
 * Case class for a confusion matrix table.
 *
 * @param tp number of true positives.
 * @param fp number of false positives.
 * @param fn number of false negatives.
 * @param tn number of true negatives.
 */
case class TableOfConfusion(tp: Int = 0, fp: Int = 0, fn: Int = 0, tn: Int = 0) {
  /**
   * Add two confusion tables together.
   *
   * @param that The `TableOfConfusion` to add.
   *
   * @return A `TableOfConfusion` with the metrics (`tp`, `fp`, `fn` and `tn`) added together from this and `that`.
   */
  def +(
    that: TableOfConfusion
  ): TableOfConfusion = TableOfConfusion(tp + that.tp, fp + that.fp, fn + that.fn, tn + that.tn)

  /** Calculate the accuracy. */
  def accuracy: Double = (tp + tn) / total.toDouble

  /** Calculate the F1-score. */
  def f1Score: Double = 2 * tp / (2 * tp + fp + fn).toDouble

  /** Calculate the false discovery rate. */
  def fdr: Double = fp / (fp + tp).toDouble

  /** Calculate the precision. */
  def precision: Double = tp / (tp + fp).toDouble

  /** Calculate the recall. */
  def recall: Double = tp.toDouble / (tp + fn).toDouble

  private def total: Int = tp + fp + fn + tn
}

