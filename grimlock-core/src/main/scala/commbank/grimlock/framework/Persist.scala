// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner

import org.apache.hadoop.io.Writable

import scala.util.Try

/** Trait for persisting data. */
trait Persist[T, C <: Context[C]] extends java.io.Serializable {
  /**
   *   Convenience function for suppressing ‘Discarded non-unit value’ compiler warnings.
   *
   *   These occur when the output of a function is not assigned to a variable (for a non-unit return).
   *   This function ensures that such warnings are suppressed, it does not affect the flow or outcome.
   */
  def toUnit(): Unit = ()
}

/** Companion object to `Persist` with various types, implicits, etc. */
object Persist {
  trait Loader[X, C <: Context[C]] extends java.io.Serializable {
    /**
     * Function to load data.
     *
     * @param context     The operating context
     * @param location    Name of the output directory or file, or `database.table` for tables.
     *
     * @return A `C#U[X]`; that is it returns distributed data of type `X`.
     */
    def load(context: C, location: String): C#U[X]
  }

  /** Type for general parsing. */
  type Parser[X, T] = (X) => TraversableOnce[Try[T]]

  /** Shorthand type for converting a `T` to key value tuple. */
  type SequenceWriter[T, K <: Writable, V <: Writable] = (T) => TraversableOnce[(K, V)]

  /** Type for parsing a string to one or more `T`s or an error string. */
  type TextParser[T] = Parser[String, T]

  /** Shorthand type for converting a `T` to string. */
  type TextWriter[T] = (T) => TraversableOnce[String]

  /**
   * Type for converting pivoted `T`s to string.
   *
   * All data will first be pivoted according to a slice. All `T` belonging to each slice.selected will
   * be grouped into the list.
   */
  type TextWriterByPosition[T] = (List[Option[T]]) => TraversableOnce[String]

  /** Trait for tuners permitted on a call to `saveAsText`. */
  trait SaveAsTextTuner[U[_], T <: Tuner] extends java.io.Serializable
}

/** Trait for writing strings as text. */
trait SaveStringsAsText[C <: Context[C]] extends Persist[String, C] {
  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[String]`; that is it returns `data`.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[String]
}

