package com.rison.flink.client

import java.util.Properties

import com.rison.flink.util.PropertiesUtil
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/12 上午11:30
 *         Redis 客户端工具类
 */
object RedisClient {
  //定义一个连接池对象
  private var jedisPool:JedisPool = null

  /**
   * 创建JedisPool连接池对象
   */
  def build():Unit = {
    val prop = PropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")

    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100)  //最大连接数
    jedisPoolConfig.setMaxIdle(20)   //最大空闲
    jedisPoolConfig.setMinIdle(20)     //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
  }

  /**
   * 获取Jedis客户端
   * @return
   */
  def getJedisClient():Jedis ={
    if(jedisPool == null){
      build()
    }
    jedisPool.getResource
  }

  /**
   * 获取value
   * @param key
   * @return
   */
  def getData(key: String) = {
    val jedis: Jedis = getJedisClient()
    val str: String = jedis.get(key)
    jedis.close()
    str

  }

  /**
   * 获取top value
   * @param topRange
   */
  def getTopList(topRange: Int) = {
    val res = new ListBuffer[String]()
    for (i <- 0 until topRange) {
      res.append(getData(i.toString))
    }
    res
  }


}
