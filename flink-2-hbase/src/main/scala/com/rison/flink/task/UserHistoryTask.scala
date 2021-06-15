package com.rison.flink.task

import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.map.UserHistoryMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/15 上午8:22
 * 用户-产品 、 产品-用户 ——> Hbase
 */
object UserHistoryTask {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val groupId = "user_product_groupId"
    val topic = "user_product_topic"
    val topics = new java.util.ArrayList[String]
    val dataStream: DataStream[KafkaDStream] = environment.addSource(KafkaClient.createKafkaSource(topics, groupId))
    dataStream.map(new UserHistoryMapFunction())
    topics.add(topic)
    environment.execute(this.getClass.getSimpleName.stripPrefix("$"))
  }
}
