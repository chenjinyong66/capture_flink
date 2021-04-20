package com.cjy.apitest.sinktest

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}
import java.util.Properties

import com.cjy.apitest.Sensor
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 *
 * @author chenjinyong
 * @create 2020-04-07
 */
object JdbcSink {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "jdbcSink")

    val sourceStream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props))
    val wstream = sourceStream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })


    wstream.addSink(new mysqlSinkFunc())
    env.execute("jdbc sink")
  }

}

class mysqlSinkFunc() extends RichSinkFunction[Sensor] {

  // 定义连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: Sensor, context: SinkFunction.Context): Unit = {
    // 先执行更新操作，查到就更新
    updateStmt.setDouble(1, value.platform_name.toDouble)
    updateStmt.setString(2, value.$app_id)
    updateStmt.execute()
    // 如果更新没有查到数据，那么就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.$app_id)
      insertStmt.setDouble(2, value.platform_name.toDouble)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}