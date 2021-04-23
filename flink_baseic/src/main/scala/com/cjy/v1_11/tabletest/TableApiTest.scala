package com.cjy.v1_11.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * table sql api
 *
 * @author chenjinyong
 * @create 2020-10-14
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val inputStreamPath = "E:\\WorkSpace\\workspace_self\\flink_tutorial\\flink_baseic\\src\\main\\resources\\sensor.txt"


    val createTable = String.format(
      //"CREATE TABLE UserScores (type STRING, orderNo STRING,productName STRING,money FLOAT,name STRING,zoneCode STRING,zoneName STRING)\n" +
      "CREATE TABLE UserScores (name STRING)\n" +
        "WITH (\n" +
        "  'connector' = 'kafka',\n" +
        "  'topic' = 'nima',\n" +
        "  'properties.bootstrap.servers' = '192.168.120.130:9092',\n" +
        "  'properties.group.id' = 'testGroup',\n" +
        "  'format' = 'json',\n" +
        //"  'scan.startup.timestamp-millis' = '1605147648000',\n" +
        // "  'csv.field-delimiter' = '\t',\n" +
        "  'scan.startup.mode' = 'group-offsets'\n" +
        ")")
    tableEnv.executeSql(createTable)
    val table = tableEnv.sqlQuery("select * from UserScores")
    table.execute().print()


    val schema = new Schema().field("id", DataTypes.STRING()).field("name", DataTypes.STRING())
    tableEnv.connect(new FileSystem().path(inputStreamPath))
      .withFormat(new Csv().fieldDelimiter(','))
      .withSchema(schema)
      .createTemporaryTable("sensorTmp")
    // 调用 tableApi
    val sensorTable = tableEnv.from("sencorTmp")
      .select($("$device_id"), $"project", $("$is_first_day"))
      .filter($"project" === "production")

    // use sql
    val sqltable = tableEnv.sqlQuery(
      """
        |select * from sensorTmp
        |""".stripMargin)

    sensorTable.toAppendStream[(String, String)].print("table api")

    sqltable.execute().print()

  }
}

