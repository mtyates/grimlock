// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock.spark

import commbank.grimlock.framework.{ Persist => FwPersist }

import commbank.grimlock.spark.environment.{ DistributedData, Environment }

/** Trait for peristing a Spark `RDD`. */
trait Persist[T] extends FwPersist[T] with DistributedData with Environment with java.io.Serializable {
  protected def saveText(ctx: C, file: String, writer: TextWriter): U[T] = {
    data
      .flatMap(writer(_))
      .saveAsTextFile(file)

    data
  }
}
