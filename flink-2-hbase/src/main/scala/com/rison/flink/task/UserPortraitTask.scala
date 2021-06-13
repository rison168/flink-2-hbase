package com.rison.flink.task

import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.map.UserPortraitMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/13 下午3:01
 *用户画像 -> Hbase
 */
object UserPortraitTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val groupId = "user_groupId"
    val topic = "user_topic"
    val topics = new java.util.ArrayList[String]
    topics.add(topic)
    val dataStream: DataStream[KafkaDStream] = env.addSource(KafkaClient.createKafkaSource(topics, groupId))
    dataStream
      .map(UserPortraitMapFunction())
    env.execute(this.getClass.getSimpleName.stripPrefix("$"))
  }

}
