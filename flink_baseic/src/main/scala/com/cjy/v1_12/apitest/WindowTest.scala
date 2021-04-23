package com.cjy.v1_12.apitest

import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 窗口操作
 *
 * @author chenjinyong
 * @create 2020-04-09
 */


// 定义样例类，温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // filnk 1.12 版本中默认的是eventTime

    // 获取数据
    val sourceStream = env.socketTextStream("localhost", 8888)


    val dataStream = sourceStream.map(x => {
      val arr = x.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

      //  TODO 新版水印
      //  .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
      .assignTimestampsAndWatermarks(new WatermarkStrategy[SensorReading] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = {
          return new WatermarkGenerator[SensorReading] {
            override def onEvent(t: SensorReading, l: Long, watermarkOutput: WatermarkOutput): Unit = ???

            override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = ???
          }
        }
      })

    /*      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
            override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
          })*/

    val latetag = new OutputTag[(String, Double, Long)]("late")

    val resDataStream = dataStream.map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1) // 按照tuple 第一个元素分组
      .window(TumblingEventTimeWindows.of(Time.seconds(3))) // 滚动时间窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(5))) // 滑动时间窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口

      .allowedLateness(Time.minutes(1)) // 允许有迟到的数据
      .sideOutputLateData(latetag)

      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))

    resDataStream.getSideOutput(latetag).print("late")

    resDataStream.print()

    env.execute("window test")


  }
}
