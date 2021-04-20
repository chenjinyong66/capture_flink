package com.cjy.apitest.sinktest

import java.util
import java.util.Properties

import com.cjy.apitest.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 *
 * @author chenjinyong
 * @create 2020-04-02
 */
object ESSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "essink")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props))

    val wstream = stream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })

    val httphosts = new util.ArrayList[HttpHost]()
    httphosts.add(new HttpHost("localhost", 9200))

    val esSinkFunc = new ElasticsearchSinkFunction[Sensor] {
      override def process(t: Sensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

        //
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.$app_id)
        dataSource.put("temperature", t.$is_first_day.toString)
        dataSource.put("ts", t.platform_name.toString)

        // 创建index request，用于发送http请求

        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("test")
          .source(dataSource)

        requestIndexer.add(indexRequest)
      }
    }

    wstream.addSink(new ElasticsearchSink
    .Builder[Sensor](httphosts, esSinkFunc)
      .build()
    )

    env.execute("es sink")
  }

}
