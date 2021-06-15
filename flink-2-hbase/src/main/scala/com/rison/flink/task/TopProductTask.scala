package com.rison.flink.task

import java.util.Properties

import com.rison.flink.agg.CountAggregate
import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.domain.{LogEntity, TopProductEntity}
import com.rison.flink.top.TopNHotItems
import com.rison.flink.util.PropertiesUtil
import com.rison.flink.window.WindowResultFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
 * @author : Rison 2021/6/15 上午8:38
 *         热门产品-> redis
 */
object TopProductTask {
  private val topSize = 5

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //开启EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val groupId = "top_product_groupId"
    val topic = "top_product_topic"
    val topics = new java.util.ArrayList[String]
    val dataStream: DataStream[KafkaDStream] = environment.addSource(KafkaClient.createKafkaSource(topics, groupId))

    val logEntityDataStream: DataStream[LogEntity] = dataStream
      .map(
        kafkaDStream => {
          val logs: Array[String] = kafkaDStream.message.split(",")
          LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
        }
      )

    val topProduct: DataStream[TopProductEntity] = logEntityDataStream
      //抽取时间戳做watermark 单位毫秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEntity](Time.milliseconds(30)) {
        override def extractTimestamp(t: LogEntity) = t.time * 1000L
      })
      //按照productId 活动窗口
      .keyBy(_.productId)
      .timeWindow(Time.seconds(60), Time.seconds(5))
      .aggregate(CountAggregate(), WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(TopNHotItems(topSize))
      .flatMap(
        data => {
          data
        }
      )
    val prop: Properties = PropertiesUtil.load("conf.properties")
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost(prop.getProperty("redis.host"))
      .setPort(prop.getProperty("redis.port").toInt)
      .setDatabase(prop.getProperty("redis.db").toInt)
      .build()

    topProduct.addSink(new RedisSink[TopProductEntity](conf, TopRedisSink()))


    environment.execute(this.getClass.getSimpleName.stripPrefix("$") + " TOP N")
  }
}
