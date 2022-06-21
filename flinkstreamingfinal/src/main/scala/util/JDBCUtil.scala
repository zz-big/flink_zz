package util

import java.sql.{Connection, DriverManager}
import config.MyConfig.MYSQL_CONFIG

object JDBCUtil {
  classOf[com.mysql.jdbc.Driver]

  def getConnection: Connection = {
    DriverManager.getConnection(MYSQL_CONFIG("url"), MYSQL_CONFIG("username"),MYSQL_CONFIG("password"))
  }
}
