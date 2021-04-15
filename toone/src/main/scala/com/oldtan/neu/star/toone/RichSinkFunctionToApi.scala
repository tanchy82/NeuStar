package com.oldtan.neu.star.toone

import java.util.UUID

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.json4s._
import org.json4s.native.Json
import scalaj.http.Http

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RichSinkFunctionToApi extends RichSinkFunction[Map[String, AnyRef]] {

  override def invoke(d: Map[String, AnyRef]) = {
    Future {
      Http(s"http://10.101.37.126:8888/data/tantest/${UUID.randomUUID.toString}")
        .header("Content-Type", "application/json").put(Json(DefaultFormats).write(d).toString)
        .timeout(500, 300).asString
    }
  }
}


