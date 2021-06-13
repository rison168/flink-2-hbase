package com.rison.flink.map

import java.sql.ResultSet

import com.rison.flink.client.{HbaseClient, KafkaDStream, MysqlClient}
import com.rison.flink.domain.LogEntity
import com.rison.flink.util.{AgeUtil, LogHelper}
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/6/13 下午2:41
 *
 */
case class ProductPortraitMapFunction() extends MapFunction[KafkaDStream, LogEntity] with LogHelper {
  override def map(t: KafkaDStream): LogEntity = {
    log.info("ProductPortraitMapFunction参数： " + t)
    val logs: Array[String] = t.message.split(",")
    val logEntity: LogEntity = LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
    val resultSet: ResultSet = MysqlClient.selectUserById(logEntity.userId)
    if (resultSet != null) {
      while (resultSet.next()) {
        val productId = logEntity.productId
        val sex = resultSet.getString("sex")
        HbaseClient.incrementColumn("prod", productId.toString, "sex", sex)
        val age = resultSet.getString("age")
        HbaseClient.incrementColumn("prod", productId.toString, "age", AgeUtil.getAgeType(age))
      }
    }
    logEntity
  }
}
