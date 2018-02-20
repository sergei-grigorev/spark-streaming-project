package com.griddynamics.stopbot.model

import cats.kernel.Eq
import cats.kernel.laws.discipline.SemigroupTests
import cats.tests.CatsSuite
import org.scalacheck.Arbitrary

class EventAggregationTest extends CatsSuite {
  import EventAggregationTest._

  checkAll("EventAggregation.SemigroupLaws", SemigroupTests[EventAggregation].semigroup)
}

object EventAggregationTest {
  implicit def eqTree: Eq[EventAggregation] = Eq.fromUniversalEquals

  implicit def arbEventAggregation: Arbitrary[EventAggregation] =
    Arbitrary(
      for {
        clicks <- Arbitrary.arbitrary[Long]
        watches <- Arbitrary.arbitrary[Long]
        first <- Arbitrary.arbitrary[Long]
        last <- Arbitrary.arbitrary[Long]
      } yield EventAggregation(clicks, watches, first, last)
    )
}
