package com.cjy.apitest.sinktest

import java.util.Properties

import com.cjy.apitest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 *
 * @author chenjinyong
 * @create 2020-04-02
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val props = new Properties()
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "sinkKafka")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props))

    val writeStream = stream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5)).toString

    })

    writeStream.print()


    // 写入 kafka
    // todo 自动创建kafka topic
    writeStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "kafkasink", new SimpleStringSchema()))


    env.execute("sink to kafka")


  }

}
