package com.rison.flink.map

import com.rison.flink.client.{HbaseClient, KafkaDStream}
import com.rison.flink.domain.LogEntity
import com.rison.flink.util.LogHelper
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/6/15 上午8:25
 *
 */
case class UserHistoryMapFunction() extends MapFunction[KafkaDStream, LogEntity] with LogHelper{
  /**
   * 将 用户-产品 产品-用户 分别储存到Hbase表
   * @param t
   * @return
   */
  override def map(t: KafkaDStream): LogEntity = {
    log.info(this.getClass.getSimpleName.stripPrefix("$") + ":" + t)
    val logs: Array[String] = t.message.split(",")
    val logEntity: LogEntity = LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
    if (logEntity != null){
      HbaseClient.incrementColumn("user_history", logEntity.userId.toString, "product", logEntity.productId.toString)
      HbaseClient.incrementColumn("product_history", logEntity.productId.toString, "p", logEntity.userId.toString)

    }
    logEntity
  }
}
