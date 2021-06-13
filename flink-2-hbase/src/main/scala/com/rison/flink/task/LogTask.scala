package com.rison.flink.task

import com.rison.flink.client.{KafkaClient, KafkaDStream}
import com.rison.flink.map.LogMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/13 下午1:49
 * 日志 -> Hbase
 */
object LogTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val groupId = "log_groupId"
    val topic = "log_topic"
    val topics = new java.util.ArrayList[String]
    topics.add(topic)
    val dataStream: DataStream[KafkaDStream] = env.addSource(
      KafkaClient.createKafkaSource(topics, groupId)
    )
    dataStream.map(LogMapFunction())
    env.execute(this.getClass.getSimpleName.stripPrefix("$") + "message receive")
  }
}
