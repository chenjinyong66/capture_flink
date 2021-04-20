package com.cjy.apitest.sinktest

import java.util.Properties

import com.cjy.apitest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 *
 * @author chenjinyong
 * @create 2020-04-02
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "redissink")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props))

    val datastream = stream.map(x => {
      val arr = x.split(",")
      Sensor(arr(0), arr(1), arr(2), arr(3), arr(4).toBoolean, arr(5))
    })


    // 写入redis
    val redisConf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    datastream.addSink(new RedisSink[Sensor](redisConf, new redisMapper ))

    env.execute("redis sink")
  }

}

class redisMapper extends RedisMapper[Sensor] {
  // 定义保存数据写入redis的命令，HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_tmp")
  }

  // 将id指定为key
  // todo  写入redis 的方法
  override def getKeyFromData(t: Sensor): String = t.platform_name.toString

  // 将温度值指定为value
  override def getValueFromData(t: Sensor): String = t.$app_id.toString
}