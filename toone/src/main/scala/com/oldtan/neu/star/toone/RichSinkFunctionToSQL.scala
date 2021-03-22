package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.time.LocalDateTime

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction


class RichSinkFunctionToSQL extends RichSinkFunction[Map[String,AnyRef]]{
  var ps: PreparedStatement = null
  var conn: Connection = null
  val SQL = "insert into user_2(id,name,age,create_time) values(?,?,?,?)"

  def getConnection(): Connection = {
    var conn: Connection = null
    val DB_URL = "jdbc:mysql://localhost:13306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai"
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
    ps = conn.prepareStatement(SQL)
  }

  override def close() = {
    if (conn != null) conn.close()
    if (ps != null) ps.close()
  }

  override def invoke(d:Map[String,AnyRef]) {
    ps.setObject(1, d.get("id").orNull)
    ps.setObject(2, d.get("name").orNull)
    ps.setObject(3, d.get("age").orNull)
    ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
    ps.executeUpdate()
  }

}
