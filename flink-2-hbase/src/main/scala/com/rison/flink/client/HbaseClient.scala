package com.rison.flink.client

import org.apache.hadoop.hbase.client.{Admin, Connection}

/**
 * @author : Rison 2021/6/11 下午3:04
 * 连接Hbase客户端工具类
 */
object HbaseClient {
  var admin: Admin = _
  var conn: Connection = _



}
