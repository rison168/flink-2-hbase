package com.rison.flink.client

import java.util.Properties

import cn.hutool.core.util.StrUtil
import com.rison.flink.util.{LogHelper, PropertiesUtil}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer


/**
 * @author : Rison 2021/6/12 下午12:54
 *         kafka工具类
 */
object KafkaClient extends LogHelper {
  private val properties: Properties = PropertiesUtil.load("conf.properties")
  val broker_list = properties.getProperty("kafka.bootstrap.servers")
  val zookeeper_connect = properties.getProperty("kafka.zookeeper.connect")

  // kafka消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "zookeeper.connect" -> zookeeper_connect,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "log_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
   * kafkaProps
   *
   * @param groupId
   * @return
   */
  def getKafkaProperties(groupId: String): Properties = {
    kafkaParam.put("group.id", groupId)
    val kafkaProps: Properties = new Properties()
    kafkaParam.empty.foreach(
      param => {
        kafkaProps.setProperty(param._1, param._2.toString)
      }
    )
    kafkaProps
  }

  /**
   * kafkaProps
   *
   * @return
   */
  def getKafkaProperties(): Properties = {
    val kafkaProps: Properties = new Properties()
    kafkaParam.empty.foreach(
      param => {
        kafkaProps.setProperty(param._1, param._2.toString)
      }
    )
    kafkaProps
  }

  /**
   * 组建kafka信息
   *
   * @param topic
   * @param groupId
   * @return
   */
  def createKafkaSource(topic: java.util.List[String], groupId: String): FlinkKafkaConsumer011[KafkaDStream] = {

    // kafka消费者配置
    //KeyedDeserializationSchema太旧了，用KafkaDeserializationSchema
    val dataStream = new FlinkKafkaConsumer011[KafkaDStream](topic: java.util.List[String], new KafkaDeserializationSchema[KafkaDStream]() {
      override def getProducedType: TypeInformation[KafkaDStream] = TypeInformation.of(new TypeHint[KafkaDStream]() {})

      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaDStream = {
        var key: String = null
        var value: String = null
        if (record.key != null) {
          key = new String(record.key())
        }
        if (record.value != null) {
          value = new String(record.value())
        }
        val kafkaSource = KafkaDStream(record.topic(), record.partition(), record.offset(), record.timestamp().toString, value)
        kafkaSource
      }

      override def isEndOfStream(s: KafkaDStream) = false
    },
      getKafkaProperties(groupId)
    )

    //是否自动提交offset
    dataStream.setCommitOffsetsOnCheckpoints(true)
    dataStream
  }


  /**
   * 从redis中获取kafka的offset
   *
   * @param topics
   * @return
   */
  def getSpecificOffsets(topics: java.util.ArrayList[String]): java.util.Map[KafkaTopicPartition, java.lang.Long] = {
    import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
    val specificStartOffsets: java.util.Map[KafkaTopicPartition, java.lang.Long] = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    import scala.collection.JavaConversions._
    for (topic <- topics) {
      val jedis = RedisClient.getJedisClient()
      val key = s"my_flink_$topic"
      val partitions = jedis.hgetAll(key).toList
      for (partition <- partitions) {
        if (!StrUtil.isEmpty(topic) && !StrUtil.isEmpty(partition._1) && !StrUtil.isEmpty(partition._2)) {
          log.warn("topic:" + topic.trim, partition._1.trim.toInt, partition._2.trim.toLong)
          specificStartOffsets.put(new KafkaTopicPartition(topic.trim, partition._1.trim.toInt), partition._2.trim.toLong)
        }
      }
      jedis.close()
    }
    specificStartOffsets
  }

  /**
   * 保存偏移量到 Redis
   *
   * @param topic
   * @param partition
   * @param offset
   */
  def setOffset(topic: String, partition: Int, offset: Long): Unit = {
    val jedis = RedisClient.getJedisClient()
    val gtKey = s"my_flink_$topic"
    jedis.hset(gtKey, partition.toString, offset.toString)
    jedis.close()
  }

}


case class KafkaDStream(topic: String, partition: Int, offset: Long, keyMessage: String, message: String) {}
