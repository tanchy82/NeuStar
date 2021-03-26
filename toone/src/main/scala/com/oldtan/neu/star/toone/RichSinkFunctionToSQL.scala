package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.time.LocalDateTime

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction


class RichSinkFunctionToSQL extends RichSinkFunction[Map[String,AnyRef]]{
  var conn: Option[Connection] = None
  var ps: Option[PreparedStatement] = None
  val SQL = "insert into user_2(id,name,age,create_time) values(?,?,?,?)"

  @throws("Due to the connect error then exit!")
  def getConnection(): Option[Connection] = {
    val DB_URL = "jdbc:mysql://huaweioldtan:13306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai"
    val USER = "root"
    val PASS = "oldtan"
    Class.forName("com.mysql.cj.jdbc.Driver")
    Option(DriverManager.getConnection(DB_URL, USER, PASS))
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = this.getConnection()
    ps = Option(conn.get.prepareStatement(SQL))
  }

  override def close() = {
    conn.foreach(_ close)
    ps.foreach(_ close())
  }

  override def invoke(d:Map[String,AnyRef]) {
    ps.foreach(p => {
      p.setObject(1, d.get("id").orNull)
      p.setObject(2, d.get("name").orNull)
      p.setObject(3, d.get("age").orNull)
      p.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
      p.executeUpdate
    })
  }

}
