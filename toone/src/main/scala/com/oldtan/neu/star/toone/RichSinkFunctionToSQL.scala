package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class RichSinkFunctionToSQL extends RichSinkFunction[Map[Int, AnyRef]] {
  var conn: Option[Connection] = None
  var ps: Option[PreparedStatement] = None
  val SQL = "insert into emr_episode_summary_diagnosis(ORG_CODE, ORG_NAME, DISCHARGE_DIAG_ID, EPISODE_SUMMARY_ID," +
    "EPI_SUMM_TYPE_CODE,INPATIENT_VISIT_NO, MEDICAL_RECORD_NO,ADMIT_TIMES, DIAG_TYPE_CODE, DIAG_TYPE_NAME," +
    "DIAG_SQUENCE_NO,TCM_WM_MARK,IS_PRIMARY_DIAG,WM_DIAG_CODE_RAW,WM_DIAG_NAME_RAW,WM_DIAG_CODE, " +
    "WM_DIAG_NAME,CM_BING_CODE_RAW, CM_BING_NAME_RAW,CM_BING_CODE,CM_BING_NAME, CM_ZHENG_CODE_RAW, " +
    "CM_ZHENG_NAME_RAW,CM_ZHENG_CODE,CM_ZHENG_NAME, ADMIT_DIAG_STAT_CODE,ADMIT_DIAG_STAT_NAME," +
    "DIAG_CONFIRM_DATE,  CLINI_OUTCOME_CODE,CLINI_OUTCOME_NAME, CONFIDENTIALITY_CODE,LAST_UPDATE_DTIME, RESOURCE_ID, ROWKEY) " +
    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

  @throws("Due to the connect error then exit!")
  def getConnection: Option[Connection] = {
    //val DB_URL = "jdbc:oracle:thin:@10.101.37.65:1521:orcl19c"
    //val DB_URL = "jdbc:oracle:thin:@10.34.14.1:1521:ythorcl"
    val DB_URL = """jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.34.14.1)(PORT=1521))(LOAD_BALANCE=yes))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ythorcl)))"""
    val USER = "mdm"
    val PASS = "mdm"
    Class.forName("oracle.jdbc.driver.OracleDriver")
    Option(DriverManager.getConnection(DB_URL, USER, PASS))
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = this.getConnection
    ps = Option(conn.get.prepareStatement(SQL))
  }

  override def close() = {
    conn.foreach(_ close)
    ps.foreach(_ close)
  }

  override def invoke(d: Map[Int, AnyRef]) {
    ps.foreach(p => {
      (1 to 33).foreach(i => p.setObject(i, d.get(i).orNull))
      p.setString(34, UUID.randomUUID().toString)
      p.executeUpdate
    })
  }

}
