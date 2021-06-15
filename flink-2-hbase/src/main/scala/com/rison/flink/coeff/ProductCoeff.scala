package com.rison.flink.coeff

import com.rison.flink.client.HbaseClient
import com.rison.flink.domain.ProductPortraitEntity
import com.rison.flink.util.Constants

/**
 * @author : Rison 2021/6/15 上午11:59
 *         * 策略2 ： 基于产品标签 计算产品的余弦相似度
 *         *
 *         *     w = sqrt( pow((tag{i,a} - tag{j,a}),2)  + pow((tag{i,b} - tag{j,b}),2) )
 */
object ProductCoeff {

  /**
   * 获取一个产品的所有标签数据
   *
   * @param proId 产品id
   * @return 产品标签entity
   */
  def singleProduct(proId: String): ProductPortraitEntity = {
    var entity: ProductPortraitEntity = _
    try {
      val woman: String = HbaseClient.getData("prod", proId, "sex", Constants.SEX_WOMAN)
      val man: String = HbaseClient.getData("prod", proId, "sex", Constants.SEX_MAN)
      val age_10: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_10)
      val age_20: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_20)
      val age_30: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_30)
      val age_40: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_40)
      val age_50: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_50)
      val age_60: String = HbaseClient.getData("prod", proId, "age", Constants.AGE_60)
      entity = ProductPortraitEntity(man.toInt, woman.toInt, age_10.toInt, age_20.toInt, age_30.toInt, age_40.toInt, age_50.toInt, age_60.toInt)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    entity
  }

  /**
   * 根据标签计算两个产品之间的相关度
   *
   * @param product 产品
   * @param target  相关产品
   * @retur
   * */
  def getScore(product: ProductPortraitEntity, target: ProductPortraitEntity): Double = {
    val sqrt: Double = Math.sqrt(product.getTotal() + target.getTotal())
    if (sqrt == 0) {
      0.0
    }
    val total: Double = product.man * target.man + product.age_10 * target.age_10 + product.age_20 * target.age_20 + product.age_30 * target.age_30 +
      product.age_40 * target.age_40 + product.age_50 * target.age_50 + product.age_60 * target.age_60
    math.sqrt(total) / sqrt
  }

  /**
   * 计算一个产品和其他相关产品的评分,并将计算结果放入Hbase
   *
   * @param id     产品id
   * @param others 其他产品的id
   */
  def getSingleProductCoeff(id: String, others: List[String]) = {
    val product: ProductPortraitEntity = singleProduct(id)
    for (productId <- others) {
      if (!productId.equals(id)) {
        val entity: ProductPortraitEntity = singleProduct(productId)
        val score: Double = getScore(product, entity)
      }
    }
  }

}
