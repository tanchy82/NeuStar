package com.oldtan.neu.star.toone

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.json4s.DefaultFormats
import org.json4s.native.Json
import scalaj.http.Http

import scala.concurrent.Future
import scala.util.Failure
import scala.concurrent.ExecutionContext.Implicits.global

class RichSinkFunctionToApi extends RichSinkFunction[List[Map[String, Any]]] {

  override def invoke(d: List[Map[String, Any]]) = {
    /*
     *  http://172.22.248.72:8888/data/yichangtest
     *  http://10.101.37.126:8888/data/tantest
     */
    /*try {
      Http(s"http://172.22.248.72:8888/data/_bulk_insert/false/true/false")
        .header("Content-Type", "application/json").postData(Json(DefaultFormats).write(d))
        .timeout(500, 10000).asString
    }catch {
      case e:Exception => println(e.printStackTrace)
    }*/

    Future {
      Http(s"http://172.22.248.72:8888/data/_bulk_insert/false/true/false")
        .header("Content-Type", "application/json").postData(Json(DefaultFormats).write(d))
        .timeout(500, 10000).asString
    }.onComplete {
      case Failure(e) => println(e.printStackTrace)
      case _ =>
    }
  }
}
