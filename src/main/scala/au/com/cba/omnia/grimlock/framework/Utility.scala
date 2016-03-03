// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.utility

import shapeless._
import shapeless.ops.hlist._

    object Foo {

    import scala.util.Random
    import shapeless._, poly._, ops.hlist._

    trait C[A]
    case class D[A](a: A) extends C[A]

    val r = new Random(0)
    val Seq(b1T, b2T, b3F) = Seq.fill(3)(r.nextBoolean)
    val lst = (b1T, D(1)) :: (b2T, D(2f)) :: (b3F, D(3d)) :: HNil

    object booleanFilter extends Poly2 {
      implicit def hnilFilter[T, A[X] <: C[X]] = at[(Boolean, A[T]), HNil]{ case((b,a), acc) => if(b) a :: acc else acc }

      implicit def filter[T, A[X] <: C[X], ACC <: HList](implicit lub: LUBConstraint[ACC, C[_]]) =
        at[(Boolean, A[T]), ACC]{ case ((b, a), acc) => if (b) a :: acc else acc }
    }

      def filter[L <: HList, F <: HList](l: L)(implicit
                                               lub: LUBConstraint[L, (Boolean, C[_])],
                                               rf: RightFolder.Aux[L, HNil.type, booleanFilter.type, F]): F =
        l.foldRight(HNil)(booleanFilter)
    }
/** Base trait for ecaping special characters in a string. */
trait Escape {
  /** The special character to escape. */
  val special: String

  /**
   * Escape a string.
   *
   * @param str The string to escape.
    * @return The escaped string.
   */
  def escape(str: String): String
}

/**
 * Escape a string by enclosing it in quotes.
 *
 * @param special The special character to quote.
 * @param quote   The quoting character to use.
 * @param all     Indicator if all strings should be quoted.
 */
case class Quote(special: String, quote: String = "\"", all: Boolean = false) extends Escape {
  def escape(str: String): String = {
    if (all || str.contains(special)) { quote + str + quote } else { str }
  }
}

/**
 * Escape a string by replacing the special character.
 *
 * @param special The special character to replace.
 * @param pattern The escape pattern to use. Use `%1$``s` to substitute the special character.
 */
case class Replace(special: String, pattern: String = "\\%1$s") extends Escape {
  def escape(str: String): String = str.replaceAllLiterally(special, pattern.format(special))
}

/** Type class that ensures three types are different. */
trait Distinct[P <: Product] {

}

object Distinct {
  implicit def default[P <: Product](implicit gen: Generic.Aux[P, HNil]) = new Distinct[P] {}

  implicit def isDistinct[P <: Product, L <: HList, C <: Coproduct](implicit
                                                                   gen: Generic.Aux[P, L],
                                                                   toCoproduct: ToCoproduct.Aux[L, C],
                                                                   toSum: ToSum.Aux[L, C]
                                                                  ) = new Distinct[P] {}
}

trait UnionTypes {
  type Not[A] = A => Nothing
  type NotNot[A] = Not[Not[A]]

  trait Disjunction {
    self =>
    type D
    type Or[S] = Disjunction {
      type D = self.D with Not[S]
    }
  }

  type OneOf[T] = {
    type Or[S] = (Disjunction {type D = Not[T]})#Or[S]
  }




  type Contains[S, T <: Disjunction] = NotNot[S] <:< Not[T#D]
  type In[S, T <: Disjunction] = Contains[S, T]
  type Is[S, T] = Contains[S, OneOf[T]#Or[Nothing]]

}

object UnionTypes extends UnionTypes
