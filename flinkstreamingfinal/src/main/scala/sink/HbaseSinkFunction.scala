package sink

import java.util.Date

import extract.{HBaseRecord, Record}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import util.{JavaUtil, Util}

class HbaseSinkFunction extends RichSinkFunction[Record]{

  var table: HTable =_
  var connection:Connection = _

  override def invoke(value: Record, context: SinkFunction.Context[_]): Unit = {

    val record: HBaseRecord = value.asInstanceOf[HBaseRecord]

    val put = new Put(Bytes.toBytes(s"${record.host}_${Util.getTime(record.eventTime, "yyyyMMddHHmmssSSS")}_${record.urlMd5}"))
    if (JavaUtil.isNotEmpty(record.url)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("url"), Bytes.toBytes(record.url))
    if (JavaUtil.isNotEmpty(record.host)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("host"), Bytes.toBytes(record.host))
    if (JavaUtil.isNotEmpty(record.domain)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("domain"), Bytes.toBytes(record.domain))
    if (JavaUtil.isNotEmpty(record.content)) put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("context"), Bytes.toBytes(record.content))
    if (JavaUtil.isNotEmpty(record.html)) put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("html"), Bytes.toBytes(record.html))

    table.put(put)

  }

  override def close(): Unit = {
    table.close()
    connection.close()
  }

  override def open(parameters: Configuration): Unit = {

     val hbaseConf: conf.Configuration = HBaseConfiguration.create()
    connection = ConnectionFactory.createConnection(hbaseConf)
    table = connection.getTable(TableName.valueOf(config.MyConfig.HBASE_TABLE_NAME)).asInstanceOf[HTable]

  }
}
