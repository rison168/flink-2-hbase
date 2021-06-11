package com.rison.flink.client

import java.io.IOException
import java.util
import java.util.{Collections, Properties}

import com.rison.flink.util.{LogHelper, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/11 下午3:04
 *         连接Hbase客户端工具类
 */
object HbaseClient extends LogHelper {
  var admin: Admin = _
  var conn: Connection = _
  val prop: Properties = PropertiesUtil.load("conf.properties")

  /**
   * 初始化
   */
  def build() = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.rootdir", prop.getProperty("hbase.rootdir"))
    conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"))
    conf.set("hbase.client.scanner.timeout.period", prop.getProperty("hbase.client.scanner.timeout.period"))
    conf.set("hbase.rpc.timeout", prop.getProperty("hbase.rpc.timeout"))
    try {
      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin()
    } catch {
      case e: IOException => e.printStackTrace()
    }

    /**
     * 创建Hbase表
     *
     * @param tableName      表名
     * @param columnFamilies 簇字段
     */
    @throws[IOException]
    def createTable(tableName: String, columnFamilies: String*)  = {
      if (conn == null) {
        this.build()
      }
      if (admin.tableExists(TableName.valueOf(tableName))) {
        log.info("table exists !")
      } else {
        log.info("start create table...")
        val tableDescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
        columnFamilies.foreach(
          column => {
            tableDescriptor.addFamily(new HColumnDescriptor(column))
          }
        )
        admin.createTable(tableDescriptor)
        log.info("create table success !")
      }
    }
  }

  /**
   * 获取一列获取一行数据
   * @param tableName
   * @param rowKey
   * @param familyName
   * @param column
   * @return
   */
  @throws[IOException]
  def getData(tableName: String, rowKey: String, familyName: String, column: String): String = {
    if (conn == null){
      this.build()
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val rows: Array[Byte] = Bytes.toBytes(rowKey)
    val get = new Get(rows)
    val result: Result = table.get(get)
    val resultValues: Array[Byte] = result.getValue(familyName.getBytes(), column.getBytes())
    resultValues.toString
  }

  /**
   * 获取一行的所有数据，并且排序
   * @param tableName
   * @param rowKey
   * @throws
   * @return
   */
  @throws[IOException]
  def getRow(tableName: String, rowKey: String): ListBuffer[(String, Double)] = {
    if (conn == null){
      this.build()
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val rows: Array[Byte] = Bytes.toBytes(rowKey)
    val get = new Get(rows)
    val result: Result = table.get(get)
    val list = ListBuffer[(String, Double)]()
    result.listCells().forEach(
      cell => {
        val key: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        list.append((key, value.toDouble))
      }
    )
    list.sortWith(
      (data1: (String, Double), data2: (String, Double)) =>
        data1._2 < data2._2
    )
  }


}
