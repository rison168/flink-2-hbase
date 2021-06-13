package com.rison.flink.task

import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.domain.LogEntity
import com.rison.flink.map.{ProductPortraitMapFunction, UserInterestFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/13 下午3:18
 *用户兴趣 -> Hbase
 */
object UserInterestTask{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val groupId = "user_interest_groupId"
  val topic = "user_interest_topic"
  val topics = new java.util.ArrayList[String]
  topics.add(topic)
  val dataStream: DataStream[KafkaDStream] = env.addSource(KafkaClient.createKafkaSource(topics, groupId))
  dataStream
    .map(
      str => {
        val logs: Array[String] = str.message.split(",")
        LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
      }
    )
    .keyBy(_.userId)
    .map(UserInterestFunction())

  env.execute(this.getClass.getSimpleName.stripPrefix("$"))

}

