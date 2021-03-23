package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable

class RichSourceFunctionFromSQL extends RichSourceFunction[Map[String,AnyRef]] {

  var isRUNNING: Boolean = true
  var conn: Connection = null

  def getConnection(): Connection = {
    var conn: Connection = null
    val DB_URL = "jdbc:mysql://127.0.0.1:13306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai"
    val USER = "root"
    val PASS = "oldtan"
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(DB_URL, USER, PASS)
    } catch {
      case _: Throwable => println("Due to the connect error then exit!")
    }
    conn
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = this.getConnection()
  }

  override def cancel() = isRUNNING = false

  override def close() = {
    if (conn != null) conn.close()
  }

  override def run(sourceContext: SourceFunction.SourceContext[Map[String,AnyRef]]) = {
    val lastKeySet:mutable.Set[String] = mutable.Set()
    val currentSet:mutable.Set[String] = mutable.Set()

    while (isRUNNING) {
      val sql = "SELECT * FROM user "
      // s"UNIX_TIMESTAMP(create_time) > UNIX_TIMESTAMP('${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now().minusMinutes(6))}')"
      println(if(conn == null) "Mysql database connect fail ****")
      val ps = this.conn.prepareStatement(sql)
      val resSet = ps.executeQuery()
      while (resSet.next() && !lastKeySet(resSet.getString("id"))){
        currentSet += resSet.getString("id")
        sourceContext.collect((1 to resSet.getMetaData.getColumnCount)
          .toIterator.map(i => (resSet.getMetaData.getColumnName(i),resSet.getObject(i))).toMap)
      }
      lastKeySet.clear
      lastKeySet++=currentSet
      currentSet.clear
      ps.close()
      println(s"****************************${LocalDateTime.now()}")
      isRUNNING = false
      //Thread.sleep(5000)
    }
  }

}
