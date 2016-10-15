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

package commbank.grimlock

import commbank.grimlock.framework._

class TestMixedType extends TestGrimlock {

  "A Mixed" should "return its short name" in {
    MixedType.toShortString shouldBe "mixed"
  }

  it should "return its name" in {
    MixedType.toString shouldBe "MixedType"
  }

  it should "return correct generalised type" in {
    MixedType.getRootType shouldBe MixedType
  }

  it should "match correct specialisation" in {
    MixedType.isTypeOf(MixedType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    MixedType.isTypeOf(NumericType) shouldBe false
    MixedType.isTypeOf(OrdinalType) shouldBe false
  }
}

class TestNumericType extends TestGrimlock {

  "A Numeric" should "return its short name" in {
    NumericType.toShortString shouldBe "numeric"
  }

  it should "return its name" in {
    NumericType.toString shouldBe "NumericType"
  }

  it should "return correct generalised type" in {
    NumericType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    NumericType.isTypeOf(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    NumericType.isTypeOf(MixedType) shouldBe false
    NumericType.isTypeOf(OrdinalType) shouldBe false
    NumericType.isTypeOf(ContinuousType) shouldBe false
  }
}

class TestContinuousType extends TestGrimlock {

  "A Continuous" should "return its short name" in {
    ContinuousType.toShortString shouldBe "continuous"
  }

  it should "return its name" in {
    ContinuousType.toString shouldBe "ContinuousType"
  }

  it should "return correct generalised type" in {
    ContinuousType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    ContinuousType.isTypeOf(ContinuousType) shouldBe true
    ContinuousType.isTypeOf(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    ContinuousType.isTypeOf(MixedType) shouldBe false
    ContinuousType.isTypeOf(OrdinalType) shouldBe false
  }
}

class TestDiscreteType extends TestGrimlock {

  "A Discrete" should "return its short name" in {
    DiscreteType.toShortString shouldBe "discrete"
  }

  it should "return its name" in {
    DiscreteType.toString shouldBe "DiscreteType"
  }

  it should "return correct generalised type" in {
    DiscreteType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    DiscreteType.isTypeOf(DiscreteType) shouldBe true
    DiscreteType.isTypeOf(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    DiscreteType.isTypeOf(MixedType) shouldBe false
    DiscreteType.isTypeOf(OrdinalType) shouldBe false
  }
}

class TestCategoricalType extends TestGrimlock {

  "A Categorical" should "return its short name" in {
    CategoricalType.toShortString shouldBe "categorical"
  }

  it should "return its name" in {
    CategoricalType.toString shouldBe "CategoricalType"
  }

  it should "return correct generalised type" in {
    CategoricalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    CategoricalType.isTypeOf(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    CategoricalType.isTypeOf(NumericType) shouldBe false
    CategoricalType.isTypeOf(DiscreteType) shouldBe false
    CategoricalType.isTypeOf(OrdinalType) shouldBe false
  }
}

class TestNominalType extends TestGrimlock {

  "A Nominal" should "return its short name" in {
    NominalType.toShortString shouldBe "nominal"
  }

  it should "return its name" in {
    NominalType.toString shouldBe "NominalType"
  }

  it should "return correct generalised type" in {
    NominalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    NominalType.isTypeOf(NominalType) shouldBe true
    NominalType.isTypeOf(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    NominalType.isTypeOf(MixedType) shouldBe false
    NominalType.isTypeOf(DiscreteType) shouldBe false
  }
}

class TestOrdinalType extends TestGrimlock {

  "A Ordinal" should "return its short name" in {
    OrdinalType.toShortString shouldBe "ordinal"
  }

  it should "return its name" in {
    OrdinalType.toString shouldBe "OrdinalType"
  }

  it should "return correct generalised type" in {
    OrdinalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    OrdinalType.isTypeOf(OrdinalType) shouldBe true
    OrdinalType.isTypeOf(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    OrdinalType.isTypeOf(MixedType) shouldBe false
    OrdinalType.isTypeOf(DiscreteType) shouldBe false
  }
}

class TestDateType extends TestGrimlock {

  "A Date" should "return its short name" in {
    DateType.toShortString shouldBe "date"
  }

  it should "return its name" in {
    DateType.toString shouldBe "DateType"
  }

  it should "return correct generalised type" in {
    DateType.getRootType shouldBe DateType
  }

  it should "match correct specialisation" in {
    DateType.isTypeOf(DateType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    DateType.isTypeOf(NumericType) shouldBe false
    DateType.isTypeOf(OrdinalType) shouldBe false
  }
}

class TestEventType extends TestGrimlock {

  "A Structured" should "return its short name" in {
    StructuredType.toShortString shouldBe "structured"
  }

  it should "return its name" in {
    StructuredType.toString shouldBe "StructuredType"
  }

  it should "return correct generalised type" in {
    StructuredType.getRootType shouldBe StructuredType
  }

  it should "match correct specialisation" in {
    StructuredType.isTypeOf(StructuredType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    StructuredType.isTypeOf(NumericType) shouldBe false
    StructuredType.isTypeOf(OrdinalType) shouldBe false
  }
}

