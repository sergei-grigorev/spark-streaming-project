package com.griddynamics.stopbot.model

import java.sql.Timestamp
import java.time.Instant

import cats.kernel.Eq
import cats.kernel.laws.discipline.SemigroupTests
import cats.tests.CatsSuite
import org.scalacheck.{ Arbitrary, Gen }

class EventAggregationTest extends CatsSuite {

  import EventAggregationTest._

  checkAll("EventAggregation.SemigroupLaws", SemigroupTests[EventAggregation].semigroup)
}

object EventAggregationTest {
  implicit def eqTree: Eq[EventAggregation] = Eq.fromUniversalEquals

  implicit val timestampArbitrary: Arbitrary[Timestamp] =
    Arbitrary(Gen.choose(0, Long.MaxValue).map(l => Timestamp.from(Instant.ofEpochMilli(l))))

  implicit def arbEventAggregation: Arbitrary[EventAggregation] =
    Arbitrary(
      for {
        clicks <- Arbitrary.arbitrary[Long]
        watches <- Arbitrary.arbitrary[Long]
        first <- Arbitrary.arbitrary[Timestamp]
        last <- Arbitrary.arbitrary[Timestamp]
      } yield EventAggregation(clicks, watches, first, last))
}
