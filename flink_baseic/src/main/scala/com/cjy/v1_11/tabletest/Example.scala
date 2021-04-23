package com.cjy.v1_11.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
 *
 * @author chenjinyong
 * @create 2020-04-13
 */
case class Sensor($device_id: String, $app_id: String, $app_version: String, platform_name: String, $is_first_day: Boolean, project: String)


object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2、从文件中读取数据
    val inputStreamPath = "E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputStreamPath)


    // 3、从kafka中读取

    val resourceDataStream = inputStream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })

    // 创建表执行环境
    val tableENV = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable = tableENV.fromDataStream(resourceDataStream)
    dataTable.select('td, 'tf)

    // 调用tableapi转换
    /* dataTable.select("$device_id,project")
       .filter("project= 'default' ")
      */
    val resTable = dataTable
      .select($("$device_id"), $"project", $("$is_first_day"))
      .filter($"project" === "production")


    // 直接用sql实现
    tableENV.createTemporaryView("dataTable", dataTable)
    val sql = "select $device_id,$is_first_day,project from dataTable where $device_id ='b4f4686a0e2fcbd7'"
    val resultSqlTable = tableENV.sqlQuery(sql)

    resTable.getQueryOperation
    resTable.execute().print()
    resultSqlTable.execute().print()
    resultSqlTable.printSchema()

    env.execute("example zum")


  }

}
