package com.oldtan.neu.star.toone

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new RichSourceFunctionFromSQL).print
    //.addSink(new RichSinkFunctionToSQL)
    env.execute()
  }


}