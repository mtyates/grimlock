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

package au.com.cba.omnia.grimlock.scalding.partition

import au.com.cba.omnia.grimlock.framework.{ Cell, Default, NoParameters, Reducers, Sequence2, Tuner }
import au.com.cba.omnia.grimlock.framework.partition.{ Partition, Partitions => BasePartitions }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import au.com.cba.omnia.grimlock.scalding._

import cascading.flow.FlowDef
import com.twitter.scalding.Mode
import com.twitter.scalding.typed.{ Grouped, IterablePipe, TypedPipe }

import scala.reflect.ClassTag
import scala.util.Success

/**
 * Rich wrapper around a `TypedPipe[(I, Cell[P])]`.
 *
 * @param data The `TypedPipe[(I, Cell[P])]`.
 */
class Partitions[I: Ordering, P <: Position](val data: TypedPipe[(I, Cell[P])]) extends BasePartitions[I, P]
  with Persist[(I, Cell[P])] {
  type U[A] = TypedPipe[A]

  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  type ForEachTuners = OneOf2[Default[Execution], Default[Sequence2[Execution, Reducers]]]
  def forEach[Q <: Position, T <: Tuner](fn: (I, U[Cell[P]]) => U[Cell[Q]], exclude: List[I], tuner: T)(
    implicit ev1: ClassTag[I], ev2: ForEachTuners#V[T]): U[(I, Cell[Q])] = {
/*
    val (config, mode) = tuner.parameters match {
      case Execution(cfg, md) => (cfg, md)
    }
    val f = (t: I, c: Iterator[Cell[P]]) => {
      fn(t, IterablePipe(c.toIterable))
        .toIterableExecution
        .waitFor(config, mode) match {
          case Success(itr) => itr.toIterator
          case _ => Iterator.empty
        }
      }

    Grouped(data.filter { case (i, _) => !exclude.contains(i) })
      .mapGroup(f)
*/
    val keys = tuner.parameters match {
      case Sequence2(_, r @ Reducers(_)) => ids(Default(r))
      case _ => ids(Default(NoParameters))
    }

    val (config, mode) = tuner.parameters match {
      case Execution(cfg, md) => (cfg, md)
      case Sequence2(Execution(cfg, md), _) => (cfg, md)
    }

    // TODO: This reads the data |{ids}| times. Is there a way to read it only once?
    //       The above bit of code does do things in parallel, but it doesn't support a `save` call withing `fn`.
    //       Also, it executes the graph for each `get` after a `forEach`. The below bit might be parallelised by
    //       using a `forceToDiskExecution` inside `collect` and then zipping together the pipes in `reduce`.
    (keys
      .map { case k => List(k) }
      .sum
      .getExecution
      .waitFor(config, mode) match {
        case Success(list) => list
        case _ => List.empty
      })
      .collect { case k if (!exclude.contains(k)) => fn(k, get(k)).map { case c => (k, c) } }
      .reduce[U[(I, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(id: I): U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  type IdsTuners = OneOf2[Default[NoParameters.type], Default[Reducers]]
  def ids[T <: Tuner](tuner: T = Default())(implicit ev1: ClassTag[I], ev2: IdsTuners#V[T]): U[I] = {
    val keys = Grouped(data.map { case (i, _) => (i, ()) })

    tuner.parameters match {
      case Reducers(reducers) => keys.withReducers(reducers).sum.keys
      case _ => keys.sum.keys
    }
  }

  def merge(ids: List[I]): U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText(file: String, writer: TextWriter = Partition.toString())(implicit flow: FlowDef,
    mode: Mode): U[(I, Cell[P])] = saveText(file, writer)
}

/** Companion object for the Scalding `Partitions` class. */
object Partitions {
  /** Conversion from `TypedPipe[(T, Cell[P])]` to a Scalding `Partitions`. */
  implicit def TPTC2TPP[T: Ordering, P <: Position](data: TypedPipe[(T, Cell[P])]): Partitions[T, P] = {
    new Partitions(data)
  }
}

