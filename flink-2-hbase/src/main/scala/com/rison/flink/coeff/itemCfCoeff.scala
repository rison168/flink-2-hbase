package com.rison.flink.coeff

import com.rison.flink.client.HbaseClient

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/15 上午11:38
 *         基于协同过滤的产品相关度计算
 *         策略1 ：协同过滤
 *               abs( i ∩ j)
 *         w = ——————————————
 *               sqrt(i || j)
 *
 */
object itemCfCoeff {

  def twoItemCfCoeff(id: String, other: String): Double = {
    val p1: ListBuffer[(String, Double)] = HbaseClient.getRow("product_history", id)
    val p2: ListBuffer[(String, Double)] = HbaseClient.getRow("product_history", other)
    val n: Int = p1.size
    val m: Int = p2.size
    var sum: Int = 0
    val total: Double = Math.sqrt(n * m)
    for (data1 <- p1) {
      for (data2 <- p2) {
        if (data2._1.equals(data1._1)) sum ++
      }
    }
    if (total == 0) {
      0.0
    }
    sum / total
  }

  /**
   * 计算一个产品和其他产品的评分，将计算结果放到Hbase
   *
   * @param id
   * @param others
   */
  def getSingleItemCfCoeff(id: String, others: List[String]) = {
    others.foreach(
      other => {
        if (!id.equals(other)) {
          val score: Double = twoItemCfCoeff(id, other)
          HbaseClient.putData("px", id, "p", other, score.toString)
        }
      }
    )
  }
}
