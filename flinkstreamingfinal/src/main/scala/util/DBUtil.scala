package util

import java.util.Date
import java.sql.{Statement, Timestamp, Connection => jsc}

import extract.MysqlRecord
import org.apache.commons.codec.digest.DigestUtils

import scala.collection.mutable.HashMap


object DBUtil {

  def insertIntoMysqlByJdbc(record: MysqlRecord): Unit = {
      var connection:jsc = null
      var statement:Statement = null
      try{
        val date = new Date()
        val time: Long = date.getTime
        val timestamp = new Timestamp(time)
        val hour_md5: String = DigestUtils.md5Hex(Util.getTime(time, "yyyyMMddHH"))
        val scanDay: Int = Util.getTime(time, "yyyyMMdd").toInt
        val scanHour: Int = Util.getTime(time, "HH").toInt
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        val sql =
          s"""
             |insert into z_report_stream_extract
             |(host, scan, filtered, extract, empty_context, no_match_xpath, scan_day, scan_hour, scan_time, hour_md5)
             |values('${record.host}', ${record.scan}, ${record.filtered}, ${record.extract}, ${record.emptyContext}, ${record.noMatchXpath}, $scanDay, $scanHour, '$timestamp','$hour_md5')
             |on DUPLICATE KEY UPDATE
             |scan=scan+${record.scan},
             |filtered=filtered+${record.filtered},
             |extract=extract+${record.extract},
             |empty_context=empty_context+${record.emptyContext},
             |no_match_xpath=no_match_xpath+${record.noMatchXpath};
            """.stripMargin
        statement.execute(sql)

        connection.commit()
      }catch {
        case e:Exception => {
          e.printStackTrace()
          try{
            connection.rollback()
          }catch{
            case e:Exception => e.printStackTrace()
          }
        }
      }finally {
        try{
          if(statement != null) statement.close()
          if(connection != null) connection.close()
        }catch{
          case e:Exception => e.printStackTrace()
        }
      }

    }

  //  def insertIntoMysqlByJdbc(scanNum: Long, filteredNum: Long, extractNum: Long, emptyNum: Long, noMatchNum: Long): Unit = {
  //    if (scanNum != 0 || filteredNum != 0 || extractNum != 0 || emptyNum != 0 || noMatchNum != 0) {
  //      val connection = JDBCUtil.getConnection
  //      val statement = connection.createStatement()
  //      val time: Long = new Date().getTime
  //      val sql =
  //        s"""
  //           |insert into report_stream_extract
  //           |(scan, filtered, extract, `empty_context`, no_match_xpath, scan_time, minute_md5)
  //           |values($scanNum, $filteredNum, $extractNum, $emptyNum, $noMatchNum,
  //           |'${Util.getTime(time, "yyyy-MM-dd HH:mm:ss")}',
  //           |'${DigestUtils.md5Hex(Util.getTime(time, "yyyyMMddHHmm"))}')
  //           |on DUPLICATE KEY UPDATE
  //           |scan=scan+$scanNum, filtered=filtered+$filteredNum, extract=extract+$extractNum,
  //           |empty_context=empty_context+$emptyNum, no_match_xpath=no_match_xpath+$noMatchNum;
  //      """.stripMargin
  //
  //      statement.execute(sql)
  //      statement.close()
  //      connection.close()
  //    }
  //  }

}
