package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDateTime

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable

class RichSourceFunctionFromSQL extends RichSourceFunction[Map[String,AnyRef]] {

  var isRUNNING: Boolean = true
  var conn: Option[Connection] = None

  @throws("Due to the connect error then exit!")
  def getConnection: Option[Connection] = {
    val DB_URL = "jdbc:mysql://huaweioldtan:13306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai"
    val USER = "root"
    val PASS = "oldtan"
    Class.forName("com.mysql.cj.jdbc.Driver")
    Option(DriverManager.getConnection(DB_URL, USER, PASS))
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = this.getConnection
  }

  override def cancel() = isRUNNING = false

  override def close() = {
    conn.foreach(_ close)
  }

  override def run(sourceContext: SourceFunction.SourceContext[Map[String,AnyRef]]) = {
    val lastKeySet:mutable.Set[String] = mutable.Set()
    val currentSet:mutable.Set[String] = mutable.Set()

    while (isRUNNING) {
      val sql = "SELECT * FROM user "
      // s"UNIX_TIMESTAMP(create_time) > UNIX_TIMESTAMP('${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now().minusMinutes(6))}')"
      conn.foreach(c => {
        val ps = c.prepareStatement(sql)
        val resSet = ps.executeQuery
        new Iterator[ResultSet] {
          def hasNext = resSet.next()
          def next = resSet
        }.toStream.filter(r => !lastKeySet(r.getString("id"))).foreach(r => {
          currentSet += r.getString("id")
          sourceContext.collect((1 to r.getMetaData.getColumnCount).toIterator.map(i => (r.getMetaData.getColumnName(i),r.getObject(i))).toMap)
        })
        lastKeySet.clear
        lastKeySet++=currentSet
        currentSet.clear
        ps.close()
      })

      println(s"****************************${LocalDateTime.now()}")
      isRUNNING = false
      //Thread.sleep(5000)
    }
  }

}
