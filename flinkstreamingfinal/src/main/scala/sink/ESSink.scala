package sink

import java.net.{InetAddress, InetSocketAddress}

import extract.{ESRecord, Record}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * es的存储逻辑
  */
object ESSink {
  def apply(): ElasticsearchSink[Record] = {
    val esClusterName = "zz-es"
    val esPort = 9300
    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    val esConfig = new java.util.HashMap[String, String]
    esConfig.put("cluster.name", esClusterName)
    // This instructs the sink to emit after every element, otherwise they would be buffered
    esConfig.put("bulk.flush.max.actions", "100")

    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s1.hadoop"), esPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s2.hadoop"), esPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("s3.hadoop"), esPort))

    val esSink = new ElasticsearchSink[Record](esConfig, transportAddresses, new ElasticsearchSinkFunction[Record] {

      def createIndexRequest(element: Record): IndexRequest = {
        val record: ESRecord = element.asInstanceOf[ESRecord]
        val map = new java.util.HashMap[String, String]
        map.put("url", record.url)
        map.put("url_md5", record.urlMd5)
        map.put("host", record.host)
        map.put("domain", record.domain)
        map.put("date", record.eventTime.toString)
        map.put("content", record.content)

        Requests.indexRequest()
          .index("z_spark")
          .`type`("news")
          .source(map)
      }

      override def process(t: Record,
                           runtimeContext: RuntimeContext,
                           requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndexRequest(t))
      }
    })
    esSink
  }
}