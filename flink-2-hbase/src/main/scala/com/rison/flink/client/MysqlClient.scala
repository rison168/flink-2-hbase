package com.rison.flink.client

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Properties

import com.rison.flink.util.PropertiesUtil

/**
 * @author : Rison 2021/6/12 下午12:09
 *         mysql客户端工具类
 */
object MysqlClient {
  val prop: Properties = PropertiesUtil.load("conf.properties")
  val url = prop.getProperty("mysql.url")
  val name = prop.getProperty("mysql.name")
  val pass = prop.getProperty("mysql.pass")
  var stmt: Statement = _
  var conn: Connection = _

  /**
   * 创建statement
   */
  def build(): Unit = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(url, name, pass)
      stmt = conn.createStatement()
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: SQLException => e.printStackTrace()
    }
  }

  /**
   * 根据Id筛选产品
   *
   * @param id
   * @return
   */
  def selectById(id: Int) = {
    val sql =
      s"""
         |select *
         |from product
         |where product_id = ${id}
         |""".stripMargin
    if (stmt == null) {
      build()
    }
    val result = stmt.executeQuery(sql)
    conn.close()
    stmt.close()
    result
  }

  /**
   * 根据userId查询
   *
   * @param userId
   * @return
   */
  def selectUserById(userId: Int) = {
    val sql =
      s"""
         |select *
         |from user
         |where user_id = ${userId}
         |""".stripMargin
    if (stmt == null) {
      build()
    }
    val result = stmt.executeQuery(sql)
    conn.close()
    stmt.close()
    result
  }

}
