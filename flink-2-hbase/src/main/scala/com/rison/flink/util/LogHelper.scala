package com.rison.flink.util

/**
 * @author : Rison 2021/6/11 下午4:31
 *日志工具类
 */
import org.apache.log4j.Logger
trait LogHelper {
  lazy val log = Logger.getLogger(this.getClass.getName)
}
