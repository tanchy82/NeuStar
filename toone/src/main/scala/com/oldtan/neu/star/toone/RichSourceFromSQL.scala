package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RichSourceFromSQL extends RichSourceFunction[List[Map[String, Any]]] {

  var isRUNNING: Boolean = true
  var conn: Option[Connection] = None

  @throws("Due to the connect error then exit!")
  def getConnection: Option[Connection] = {
    //val DB_URL = "jdbc:oracle:thin:@10.101.37.65:1521:orcl19c"
    val DB_URL = """jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.22.248.135)(PORT=1521))(LOAD_BALANCE=yes))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=jkorcl)))"""
    val USER = "ehr"
    val PASS = "neusoft"
    Class forName "oracle.jdbc.driver.OracleDriver"
    Option(DriverManager.getConnection(DB_URL, USER, PASS))
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = getConnection
  }

  override def cancel() = {
    isRUNNING = false
  }

  override def close() = {
    conn.foreach(_ close)
  }

  override def run(sourceContext: SourceFunction.SourceContext[List[Map[String, Any]]]) = {
    var initHistoryStartDay = LocalDate.of(2020, 1, 1)
    val endHistoryStartDay = LocalDate.of(2021, 4, 16)
    val psFun = (con: Connection) => {
      con prepareStatement "SELECT a.organization_code AS ORG_CODE, " +
        "a.organization_name AS ORG_NAME," +
        "'' AS DISCHARGE_DIAG_ID, " +
        "a.Ws01_00_004_01 AS EPISODE_SUMMARY_ID," +
        "b.WS01_00_905_01 AS EPI_SUMM_TYPE_CODE," +
        "a.ws01_00_014_01 AS INPATIENT_VISIT_NO, " +
        "a.Ws01_00_004_01 AS MEDICAL_RECORD_NO, " +
        "b.Ws02_10_090_01 AS ADMIT_TIMES, " +
        "a.WS05_01_900_01 AS DIAG_TYPE_CODE, " +
        "a.CT05_01_900_01 AS DIAG_TYPE_NAME, " +
        "a.Ws05_01_080_01 AS DIAG_SQUENCE_NO," +
        "a.WS05_01_901_01 AS TCM_WM_MARK," +
        "a.WS05_01_901_02 AS IS_PRIMARY_DIAG, " +
        "a.WS05_01_024_01 AS WM_DIAG_CODE_RAW," +
        "a.CT05_01_024_01 AS WM_DIAG_NAME_RAW," +
        "a.WS05_01_024_01 AS WM_DIAG_CODE, " +
        "a.CT05_01_024_01 AS WM_DIAG_NAME, " +
        "a.WS05_10_130_01 AS CM_BING_CODE_RAW, " +
        "a.CT05_10_130_01 AS CM_BING_NAME_RAW, " +
        "a.WS05_10_130_01 AS CM_BING_CODE, " +
        "a.CT05_10_130_01 AS CM_BING_NAME, " +
        "a.WS05_10_130_02 AS CM_ZHENG_CODE_RAW, " +
        "a.CT05_10_130_02 AS CM_ZHENG_NAME_RAW, " +
        "a.WS05_10_130_02 AS CM_ZHENG_CODE, " +
        "a.CT05_10_130_02 AS CM_ZHENG_NAME, " +
        "a.WS09_00_104_01 AS ADMIT_DIAG_STAT_CODE, " +
        "a.CT09_00_104_01 AS ADMIT_DIAG_STAT_NAME, " +
        "a.WS05_01_058_01 AS DIAG_CONFIRM_DATE," +
        "a.WS05_10_113_01 AS CLINI_OUTCOME_CODE, " +
        "a.CT05_10_113_01 AS CLINI_OUTCOME_NAME, " +
        "'N' AS CONFIDENTIALITY_CODE, " +
        "a.WS06_00_913_01 AS LAST_UPDATE_DTIME, " +
        "'' AS RESOURCE_ID " +
        "FROM HAI_DIAREC_INFO a, hai_aprnot_info b" +
        " where a.business_id = b.business_id and a.organization_code = b.organization_code" +
        " and a.domain_code = b.domain_code and a.datagenerate_date like ?"
    }
    val ps = psFun(conn.get)
    while (initHistoryStartDay.isBefore(endHistoryStartDay)) {
      ps.setString(1, DateTimeFormatter.ofPattern("yyyyMMdd%").format(initHistoryStartDay))
      val resSet = ps.executeQuery
      var resultData:ListBuffer[Map[String, Any]] = mutable.ListBuffer.empty
      new Iterator[ResultSet] {
        def hasNext = resSet next

        def next = resSet
      }.toStream
        .foreach(r => {
          if(resultData.length >= 2000){
            sourceContext collect resultData.toList
            resultData clear
          }
          // yichangtest tantest
          resultData += (Map("_meta" -> Map("dataset" -> "yichangtest", "rowkey" -> UUID.randomUUID.toString)) ++
            (1 to r.getMetaData.getColumnCount).toIterator.map(i => (r.getMetaData.getColumnName(i), r getString i)).toMap)
          //sourceContext.collect((1 to r.getMetaData.getColumnCount).toIterator.map(i => (r.getMetaData.getColumnName(i), r getString i)).toMap)
      })
      if (resultData.nonEmpty) sourceContext collect resultData.toList
      println(initHistoryStartDay)
      initHistoryStartDay = initHistoryStartDay plusDays 1
    }
    ps close
  }
}
