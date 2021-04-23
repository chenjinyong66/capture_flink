package com.cjy.v1_11.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 *
 * @author chenjinyong
 * @create 2020-03-23
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    env.setParallelism(8)
    //    env.disableOperatorChaining()
    //    com.cjy.wc.StreamWordCount
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")

    val inputStream = env.socketTextStream(host, port)

    val resultDataStream = inputStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    resultDataStream.print().setParallelism(1)

    env.execute("stream wordcount")
  }

}
