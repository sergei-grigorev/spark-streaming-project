package com.griddynamics.stopbot

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

package object implicits {
  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)
}
