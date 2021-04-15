package com.oldtan.neu.star.toone

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object StreamingJob extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.addSource(new RichSourceFromSQL).addSink(new RichSinkFunctionToApi)
  env.execute
}



