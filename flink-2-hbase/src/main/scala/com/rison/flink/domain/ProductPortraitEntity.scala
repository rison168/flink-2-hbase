package com.rison.flink.domain

/**
 * @author : Rison 2021/6/12 下午12:33
 *
 */
case class ProductPortraitEntity(man: Int, woman: Int, age_10: Int, age_20: Int, age_30: Int, age_40: Int, age_50: Int, age_60: Int) {
  def getTotal(): Int = {
    man * man + woman * woman + age_10 * age_10 + age_20 * age_20 + age_30 * age_30 + age_40 * age_40 + age_50 * age_50 + age_60 * age_60
  }
}
