package com.rison.flink.scheduler

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Date, Timer, TimerTask}

import com.rison.flink.client.HbaseClient
import com.rison.flink.coeff.{ItemCfCoeff, ProductCoeff}
import com.rison.flink.util.LogHelper

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/15 下午2:17
 *
 *
 *         每12小时定时调度一次 基于两个推荐策略的 产品评分计算
 *         策略1 ：协同过滤
 *
 *         数据写入Hbase表  px
 *
 *         策略2 ： 基于产品标签 计算产品的余弦相似度
 *
 *         数据写入Hbase表 ps
 *
 */
object SchedulerJob {

  def main(args: Array[String]): Unit = {
    val timer = new Timer()
    //定时每15分钟
    timer.scheduleAtFixedRate(RefreshTask(), 0, 15 * 1000)
  }
}

case class RefreshTask() extends TimerTask with LogHelper {
  lazy val executorService: ExecutorService = Executors.newFixedThreadPool(10)

  override def run(): Unit = {
    log.info(new Date() + " -- 开始执行任务！")
    val listBuffer = new ListBuffer[String]()
    val allProductId: List[String] = HbaseClient.getAllKey("product_history").toList
    log.info("获取到历史的产品！")
    for (id <- allProductId) {
      //每12小时调度一次
      executorService.execute(Task(id, allProductId))
    }
  }
}

case class Task(id: String, allProductId: List[String]) extends Runnable {
  var pid: String = _
  var others: List[String] = _

  override def run(): Unit = {
    ItemCfCoeff.getSingleItemCfCoeff(id, allProductId)
    ProductCoeff.getSingleProductCoeff(id, allProductId)
  }
}
