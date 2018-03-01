package com.griddynamics.stopbot.model

import java.sql.Timestamp

import com.griddynamics.stopbot.model.EventType.EventType

case class Event(ip: String, action: EventType, eventTime: Timestamp)

case class Event2(ip: String, action: String, eventTime: Timestamp)
