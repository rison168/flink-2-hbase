package com.rison.flink.map

import cn.hutool.core.util.StrUtil
import com.rison.flink.client.{HbaseClient, KafkaDStream}
import com.rison.flink.domain.LogEntity
import com.rison.flink.util.LogHelper
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/6/13 下午2:00
 *
 */
case class LogMapFunction() extends MapFunction[KafkaDStream, LogEntity] with LogHelper {
  override def map(t: KafkaDStream): LogEntity = {
    log.info("logMapFunction参数：" + t)
    val logs: Array[String] = t.message.split(",")
    val logEntity: LogEntity = LogEntity(logs(0).toInt, logs(1).toInt, logs(2).toLong, logs(3))
    if (logEntity != null) {
      val rowKey = logEntity.userId + "_" + logEntity.productId + "_" + logEntity.time
      HbaseClient.putData("con", rowKey, "log", "userid", logEntity.userId.toString);
      HbaseClient.putData("con", rowKey, "log", "productid", logEntity.productId.toString);
      HbaseClient.putData("con", rowKey, "log", "time", logEntity.time.toString);
      HbaseClient.putData("con", rowKey, "log", "action", logEntity.action);
    }
    logEntity
  }
}
