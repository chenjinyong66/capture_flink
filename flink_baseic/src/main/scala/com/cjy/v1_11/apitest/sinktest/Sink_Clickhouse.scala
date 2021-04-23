package com.cjy.v1_11.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.{Date, Properties}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

/**
 * flink 解析 kafka 中 的 json 数据 写入clickhouse
 *
 * clickhouse支持直接消费 kafka
 *
 * @author chenjinyong
 * @create 2021-04-22
 */

case class userInfo(userid: String, name: String, age: Int, items: String, action: String, gender: Int, ctime: Long, utime: String)

object DataWrite {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.12.x 版本中默认的是事件时间 TimeCharacteristic.EventTime
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // 创建表环境
    //    val tabEnv = StreamTableEnvironment.create(env)

    // read data from kafka

    import org.apache.flink.api.scala._

    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "clickhouse")

    // 获取到kafka中的json数据
    val inputStream = env.addSource(new FlinkKafkaConsumer011("test", new JSONKeyValueDeserializationSchema(true), props))
    inputStream.print()

    val sql = "insert into test.userinfo(userid,name,age,gender,items,action,ctime,utime)values(?,?,?,?,?,?,?,?)"

    val result = inputStream.map(obj => {
      val value = obj.get("value")
      val items = value.get("item").toString.replaceAll("\"", "")
      val userid = value.get("userid").toString.replaceAll("\"", "")
      val action = value.get("action").toString.replaceAll("\"", "")
      val name = value.get("userinfo").get("name").toString.replaceAll("\"", "")
      val age = value.get("userinfo").get("age").toString.replaceAll("\"", "").toInt
      val gender = value.get("userinfo").get("gender").toString.replaceAll("\"", "").toInt
      val ctime = value.get("times").toString.replaceAll("\"", "").trim
      val utime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

      val ctimes = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(ctime).getTime

      userInfo(userid, name, age, items, action, gender, ctimes, utime)
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[userInfo]() {
          override def extractTimestamp(element: userInfo, recordTimestamp: Long): Long = {
            element.ctime * 1000L
          }
        }).withIdleness(Duration.ofMinutes(1))
      )

    result.print()


    //    result.addSink(JdbcSink.sink())

    // link-connector-jdbc
    /*
        result.addSink(JdbcSink.sink[userInfo](sql, new CkSinkBuilder, new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
          new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:clickhouse://master:8123")
            .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
            .build()
        ))

    */

    // flink--jdbc
    result.addSink(new clickhouseSinkFunc)


    env.execute("clickhouse")
  }


}

class CkSinkBuilder extends JdbcStatementBuilder[userInfo] {
  def accept(ps: PreparedStatement, v: userInfo): Unit = {
    ps.setString(1, v.userid)
    ps.setString(2, v.name)
    ps.setInt(3, v.age)
    ps.setInt(4, v.gender)
    ps.setString(5, v.items)
    ps.setString(6, v.action)
    ps.setLong(7, v.ctime)
    ps.setString(8, v.utime)
  }
}


class clickhouseSinkFunc() extends RichSinkFunction[userInfo] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    val ckDriver = "ru.yandex.clickhouse.ClickHouseDriver"

    conn = DriverManager.getConnection("jdbc:clickhouse://master:8123/test", "", "")
    insertStmt = conn.prepareStatement("insert into test.userinfo(userid,name,age,gender,items,action,ctime,utime)values(?,?,?,?,?,?,?,?)"
    )

  }


  override def close(): Unit = {
    insertStmt.close()
    conn.close()
  }

  override def invoke(value: userInfo, context: SinkFunction.Context): Unit = {
    insertStmt.setString(1, value.userid)
    insertStmt.setString(2, value.name)
    insertStmt.setInt(3, value.age)
    insertStmt.setInt(4, value.gender)
    insertStmt.setString(5, value.items)
    insertStmt.setString(6, value.action)
    insertStmt.setLong(7, value.ctime)
    insertStmt.setString(8, value.utime)
    insertStmt.execute()

  }
}


