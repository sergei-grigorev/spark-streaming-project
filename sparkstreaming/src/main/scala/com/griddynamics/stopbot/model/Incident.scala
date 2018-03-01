package com.griddynamics.stopbot.model

import java.sql.Timestamp

case class Incident(ip: String, lastEvent: Timestamp, reason: String)
