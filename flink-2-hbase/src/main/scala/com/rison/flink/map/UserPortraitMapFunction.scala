package com.rison.flink.map

import java.sql.ResultSet

import com.rison.flink.client.{HbaseClient, KafkaDStream, MysqlClient}
import com.rison.flink.domain.LogEntity
import com.rison.flink.util.LogHelper
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/6/13 下午3:03
 *
 */
case class UserPortraitMapFunction() extends MapFunction[KafkaDStream, LogEntity] with LogHelper {
  override def map(t: KafkaDStream): LogEntity = {
    log.info("UserPortraitMapFunction参数：" + t)
    val logs: Array[String] = t.message.split(",")
    val logEntity: LogEntity = LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
    val resultSet: ResultSet = MysqlClient.selectById(logEntity.productId)
    if (resultSet != null){
      while (resultSet.next()){
        val userId = logEntity.userId.toString
        val country = resultSet.getString("country")
        HbaseClient.incrementColumn("user", userId, "country", country)
        val color = resultSet.getString("color")
        HbaseClient.incrementColumn("user", userId, "color", color)
        val style = resultSet.getString("style")
        HbaseClient.incrementColumn("user", userId, "style", style)
      }
    }
    logEntity
  }
}
