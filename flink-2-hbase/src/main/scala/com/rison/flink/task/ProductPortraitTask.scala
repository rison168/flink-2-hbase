package com.rison.flink.task

import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.map.ProductPortraitMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/13 下午2:36
 *         产品画像 -> Hbase
 */
object ProductPortraitTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val groupId = "product_groupId"
    val topic = "product_topic"
    val topics = new java.util.ArrayList[String]
    topics.add(topic)
    val dataStream: DataStream[KafkaDStream] = env.addSource(KafkaClient.createKafkaSource(topics, groupId))
    dataStream.map(ProductPortraitMapFunction())
    env.execute(this.getClass.getSimpleName.stripPrefix("$"))
  }

}
