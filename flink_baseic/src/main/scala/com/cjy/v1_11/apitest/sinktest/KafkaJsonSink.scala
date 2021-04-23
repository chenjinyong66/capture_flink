package com.cjy.v1_11.apitest.sinktest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

/**
 * flink operator json
 *
 * @author chenjinyong
 * @create 2020-04-07
 */
object KafkaJsonSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.put("group.id", "sinkKafka")

    //如果想读多个主题，必须放在java的list中
    import scala.collection.JavaConverters._
    val topics = List[String]("test", "flinktest").asJava

    val consumer = new FlinkKafkaConsumer011(topics, new JSONKeyValueDeserializationSchema(true), props)
    // 从指定的时间戳开始读取 consumer.setStartFromTimestamp(1659801580000l)
    // 从指定的offsets 开始读取 consumer.setStartFromSpecificOffsets()

    val datasource = env.addSource(consumer)

    val resultStream = datasource.map(obj => {
      // 获取到的值中是包含 " 的
      val event = obj.get("value").get("event")
      val appVersion = obj.get("value").get("properties").get("$app_version")
      // TODO 空指针异常
      val project = obj.get("value").get("project").toString.replace("\"", "")
      val projects = obj.get("value").get("projects")
      val offset = obj.get("metadata").get("offset")
      val topic = obj.get("metadata").get("topic")
      val partition = obj.get("metadata").get("partition")


      //      if (project.toString.equals("production")) {

      (event, appVersion, project, projects, s"消费的主题是：$topic,分区是：$partition,当前偏移量是：$offset").toString()

    })


    resultStream.addSink(StreamingFileSink.forRowFormat(
      new Path("E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor_json.txt"),
      new SimpleStringEncoder[String]()
    ).build()
    )

    resultStream.print()
    env.execute("sinkJson")
  }

}

