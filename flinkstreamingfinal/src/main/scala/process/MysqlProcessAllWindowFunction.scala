package process

import java.lang

import extract.{MysqlRecord, Record}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class MysqlProcessAllWindowFunction extends ProcessAllWindowFunction[Record,MysqlRecord,TimeWindow]{
  override def process(context: Context, elements: Iterable[Record], out: Collector[MysqlRecord]): Unit = {
    val hostScan = new mutable.HashMap[String, Long]()
    val hostFilter = new mutable.HashMap[String, Long]()
    val hostExtract = new mutable.HashMap[String, Long]()
    val hostEmpty = new mutable.HashMap[String, Long]()
    val hostNoMatch = new mutable.HashMap[String, Long]()

    elements.foreach(f=>{

      val record: MysqlRecord = f.asInstanceOf[MysqlRecord]
      val scan: Long = hostScan.getOrElse(record.host, 0L)
      val filter: Long = hostFilter.getOrElse(record.host, 0L)
      val extract: Long = hostExtract.getOrElse(record.host, 0L)
      val empty: Long = hostEmpty.getOrElse(record.host, 0L)
      val noMatch: Long = hostNoMatch.getOrElse(record.host, 0L)
      hostScan += record.host -> (scan + record.scan)
      hostFilter += record.host -> (filter + record.filtered)
      hostExtract += record.host -> (extract + record.extract)
      hostEmpty += record.host -> (empty + record.emptyContext)
      hostNoMatch += record.host -> (noMatch + record.noMatchXpath)

    })

    for ((host, scan) <- hostScan) {
      val filtered: Long = hostFilter.getOrElse(host, 0L)
      val extract: Long = hostExtract.getOrElse(host, 0L)
      val empty: Long = hostEmpty.getOrElse(host, 0L)
      val noMatch: Long = hostNoMatch.getOrElse(host, 0L)
      out.collect(MysqlRecord(host, filtered, extract, empty, noMatch, scan))
    }



  }
}
