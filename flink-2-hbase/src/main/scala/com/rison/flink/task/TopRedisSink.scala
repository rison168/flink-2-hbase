package com.rison.flink.task

import com.rison.flink.domain.TopProductEntity
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author : Rison 2021/6/15 上午11:20
 *
 */
case class TopRedisSink() extends RedisMapper[TopProductEntity]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  override def getKeyFromData(t: TopProductEntity): String = {
    t.rankName
  }

  override def getValueFromData(t: TopProductEntity): String = {
    t.productId.toString
  }
}
