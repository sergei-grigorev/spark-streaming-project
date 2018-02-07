package com.griddynamics.stopbot.model

import com.griddynamics.stopbot.model.EventType.EventType
import io.circe.{Decoder, HCursor}
import cats.syntax.either._
import io.circe.Decoder.Result

/**
  * Input event.
  *
  * @param eventType event type
  * @param ip        user ip-address
  * @param time      event time
  * @param url       ads url
  */
case class Event(eventType: EventType, ip: String, time: Long, url: String)

object Event {
  //noinspection ConvertExpressionToSAM
  implicit val decodeEvent: Decoder[Event] = new Decoder[Event] {
    override def apply(c: HCursor): Result[Event] = {
      for {
        eventType <- c.downField("type").as[String]
        userIp <- c.downField("ip").as[String]
        requestTime <- c.downField("unix_time").as[Long]
        url <- c.downField("url").as[String]
      } yield {
        new Event(EventType.values.find(_.toString == eventType).getOrElse(EventType.Unknown), userIp, requestTime, url)
      }
    }
  }
}