package com.rison.flink.util

/**
 * @author : Rison 2021/6/12 下午12:40
 *
 */
object AgeUtil {
  def getAgeType(age: String): String = {
    val number: Int = Integer.valueOf(age)
    if (10 <= number && number < 20) "10s"
    else if (20 <= number && number < 30) "20s"
    else if (30 <= number && number < 40) "30s"
    else if (40 <= number && number < 50) "40s"
    else if (50 <= number && number < 60) "50s"
    else "0s"
  }
}
