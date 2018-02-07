package com.griddynamics.stopbot.model

import cats.kernel.laws.SemigroupLaws
import org.scalatest._
import org.scalatest.prop.PropertyChecks

class EventAggregationTest extends PropertyChecks with FunSuiteLike {

  test("semigroupAssociative") {
    forAll { (clicks: Int, watches: Int, from: Long, to: Long) =>

      val rs1 = SemigroupLaws[EventAggregation].semigroupAssociative(
        EventAggregation(clicks, watches, from, to),
        EventAggregation(clicks, watches, from, to),
        EventAggregation(clicks, watches, from, to))
      assert(rs1.lhs == rs1.rhs)
    }
  }
}
