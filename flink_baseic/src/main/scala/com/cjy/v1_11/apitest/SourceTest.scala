package com.cjy.v1_11.apitest

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * flink 读取source 从文件、Kafka、自定义source
 *
 * @author chenjinyong
 * @create 2020-03-25
 */

case class Sensor($device_id: String, $app_id: String, $app_version: String, platform_name: String, $is_first_day: Boolean, project: String)

object SourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // 1、从集合中读取数据
    val datalist = List(
      Sensor("ef6fffa168443c75", "com.chenjinyong.intl.wyn", "1.1.0", "玩转小程序", false, "default"),
      Sensor("0b4e4d7f719d659e", "com.chenjinyong.wyn", "1.1.0", "中文版app", false, "production")

    )
    val inputStream1 = env.fromCollection(datalist)

    // 2、从文件中读取数据
    val inputStreamPath = "E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputStreamPath)


    // 3、从kafka中读取
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "sensor")

    val inputStream2 = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props))
    inputStream2.print()

    inputStream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })
    //      .print()

    // 3、自定义source
    val inputStream4 = env.addSource(new MyUDFSource())


    //    inputStream4.print()


    env.execute("sensor")

  }


  class MyUDFSource() extends SourceFunction[Sensor] {
    var running: Boolean = true

    override def cancel(): Unit = false


    override def run(sct: SourceFunction.SourceContext[Sensor]): Unit = {
      val random = new Random()

      var curTemp = 1.to(10).map(i => (random.nextLong().toString, "com.chenjinyong.wyn", "1.1.0", "中文版app", random.nextBoolean(), "projection"))

      // 定义无限循环，不停地产生数据，除非被cancel
      while (running) {
        curTemp.foreach(
          x => sct.collect(Sensor(x._1, x._2, x._3, x._4, x._5, x._6))
        )
      }
      // 间隔500ms
      Thread.sleep(500)

    }
  }

}
