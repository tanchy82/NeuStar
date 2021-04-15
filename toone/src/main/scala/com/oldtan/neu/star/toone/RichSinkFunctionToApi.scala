package com.oldtan.neu.star.toone

import java.util
import java.util.UUID

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scalaj.http.Http

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Failure


class RichSinkFunctionToApi extends RichSinkFunction[Map[String, String]] {

  val mapper = new ObjectMapper

  override def invoke(d: Map[String, String]) = {
    /*
     *  http://172.22.248.72:8888/data/yichangtest
     *  http://10.101.37.126:8888/data/tantest
     */
    Future {
      val mm = new util.HashMap[String, String]
      d.toStream.foreach(a => mm.put(a._1, Option(a._2).mkString))
      val uri = s"http://172.22.248.72:8888/data/yichangtest/${UUID.randomUUID.toString}"
      println(uri)
      Http(uri)
        .header("Content-Type", "application/json").put(mapper.writeValueAsString(mm))
        .timeout(300, 500).asString
    }.onComplete { case Failure(e) => println(e.printStackTrace) }
  }
}


