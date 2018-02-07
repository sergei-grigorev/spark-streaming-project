package com.griddynamics.stopbot.model

object EventType extends Enumeration {
  type EventType = Value

  val Watch: EventType = Value("watch")
  val Click: EventType = Value("click")
  val Unknown: EventType = Value("unknown")
}
