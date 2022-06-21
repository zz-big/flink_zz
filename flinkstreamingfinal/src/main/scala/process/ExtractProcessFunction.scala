package process

import java.net.URL
import java.util.{Date, Map}
import java.{util => jutil}

import extract._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import util.{JavaUtil, Util}
import util.extractor.HtmlContentExtractor
import scala.util.control.Breaks._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExtractProcessFunction extends BroadcastProcessFunction[HtmlEvent, mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]], Record] {
  val mapBroadCast = new MapStateDescriptor[String, mutable.HashMap[String, mutable.HashSet[String]]](
    "configBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[mutable.HashMap[String, mutable.HashSet[String]]]() {})
  )


  override def processElement(value: HtmlEvent,
                              context: BroadcastProcessFunction[HtmlEvent, mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]], Record]#ReadOnlyContext,
                              out: Collector[Record]): Unit = {

    val xpathMap: ReadOnlyBroadcastState[String, mutable.HashMap[String, mutable.HashSet[String]]] = context.getBroadcastState(mapBroadCast)


    val strings: Array[String] = value.message.split("\001")
    var isCorrect: Boolean = true

    breakable {
      //接收的数据MD5\001url\001html,长度为3
      if (strings.length == 3) {
        val md5 = strings(0)
        val url = strings(1)
        val html = strings(2)
        val urlT = new URL(url)
        val host: String = urlT.getHost
        //检验MD5是否正确匹配
        val md5Check: String = DigestUtils.md5Hex(s"${url}\001${html}")
        out.collect(MysqlRecord(host))

        if (!md5Check.equals(md5)) {
          out.collect(MysqlRecord(host,filtered = 1,scan = 0))
          break()
        }
      } else if (strings.length == 2) {
        val url = strings(1)
        val urlT = new URL(url)
        val host: String = urlT.getHost
        out.collect(MysqlRecord(host,filtered = 1))
        break()

      } else {
        out.collect(MysqlRecord("HOST_NULL",filtered = 1))
        break()
      }

      //接收的数据MD5\001url\001html
      val url: String = strings(1)
      val urlT = new URL(url)
      val host: String = urlT.getHost
      val domain: String = JavaUtil.getDomainName(urlT)
      val html: String = strings(2)

      //正文抽取的结果
      // xpathMapT = ((xpath+"/"+e.tagName()+"[@id='"+e.id()+"']",	negativeWords),(xpath+"/"+e.tagName()+"[@id='"+e.id()+"']",正文))
      val xpathMapT: jutil.Map[String, String] = HtmlContentExtractor.generateXpath(html)
      val faileRule = new ArrayBuffer[String]
      var trueRule = ""
      if (JavaUtil.isNotEmpty(xpathMapT)) {
        val value: jutil.Iterator[Map.Entry[String, String]] = xpathMapT.entrySet().iterator()
        while (value.hasNext) {
          val entry: Map.Entry[String, String] = value.next()
          val key: String = entry.getKey
          if (key != null && !key.trim.equals("")) {
            if (entry.getValue.equals(HtmlContentExtractor.CONTENT)) {
              trueRule = key
            } else {
              faileRule += key
            }
          }
        }

      }

      out.collect(RedisRecord(host, faileRule, trueRule))

      //正文抽取，拿到host对应的正反规则
      if (xpathMap.contains(host)) {
        val hostXpathInfo: mutable.HashMap[String, mutable.HashSet[String]] = xpathMap.get(host)
        val hostPositiveXpathInfo: mutable.HashSet[String] = hostXpathInfo.getOrElse("true", new mutable.HashSet[String]())
        val hostNegativeXpathInfo: mutable.HashSet[String] = hostXpathInfo.getOrElse("false", new mutable.HashSet[String]())

        //抽取正文
        val doc: Document = Jsoup.parse(html)
        //直接使用java代码, 这里需要导入java和scala集合的隐式转换
        //              import scala.collection.JavaConversions._
        //              val context: String = JavaUtil.getcontext(doc,hostPositiveXpathInfo.toList,hostNegativeXpathInfo.toList)

        //使用scala代码
        val content: String = Util.getContext(doc, hostPositiveXpathInfo, hostNegativeXpathInfo)

        if (content.trim.length >= 10) {
          val time: Long = new Date().getTime
          val urlMd5: String = DigestUtils.md5Hex(url)

          //ES记录 url host 正文 html domain  urlMd5  System.currentTimeMillis()
          out.collect(ESRecord(url, host, content, domain, urlMd5, value.eventTime))
          //hbase 记录
          out.collect(HBaseRecord(host, urlMd5, url, domain, content, html, value.eventTime))
          //抽取成功
          out.collect(MysqlRecord(host,extract = 1,scan = 0))
        } else {
          // 根据xpath匹配到的正文长度小于10, 说明这个xpath很可能不正确
          out.collect(MysqlRecord(host,emptyContext = 1,scan = 0))
        }
      } else {
        //没有host匹配
        out.collect(MysqlRecord(host,noMatchXpath = 1,scan = 0))
      }
    }
  }

  override def processBroadcastElement(value: mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]],
                                       context: BroadcastProcessFunction[HtmlEvent, mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]],
                                         Record]#Context, collector: Collector[Record]): Unit = {

    val bc: BroadcastState[String, mutable.HashMap[String, mutable.HashSet[String]]] = context.getBroadcastState(mapBroadCast)
    value.foreach(f => bc.put(f._1, f._2))

  }
}