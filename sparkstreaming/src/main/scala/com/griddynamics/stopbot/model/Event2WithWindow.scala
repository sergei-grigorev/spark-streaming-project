package com.griddynamics.stopbot.model

import java.sql.Timestamp

case class Event2WithWindow(ip: String, window: Event2WithWindow.Window, action: String, eventTime: Timestamp)

object Event2WithWindow {

  case class Window(start: Timestamp, end: Timestamp)

}
