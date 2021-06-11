package com.rison.flink.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author : Rison 2021/6/11 下午3:07
 * properties工具类
 */
object PropertiesUtil {
  /**
   * 加载prop文件
   * @param propertiesName
   * @return
   */
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    //加载指定的配置文件
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8
    ))
    prop
  }

}
