package com.cjy.apitest.sinktest

import com.cjy.apitest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


/**
 * flink sink to file
 *
 * @author chenjinyong
 * @create 2020-04-07
 */
object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)


    val inputPath = "E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)


    val wstream = inputStream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })


    wstream.print()
    wstream.addSink(StreamingFileSink.forRowFormat(
      new Path("E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor_sink.txt"),
      new SimpleStringEncoder[Sensor]()
    ).build()
    )

    env.execute("fileSink")


  }
}
