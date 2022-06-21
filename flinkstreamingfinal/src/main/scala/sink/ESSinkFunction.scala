package sink

import java.net.InetAddress

import extract.{ESRecord, Record}
import org.apache.flink.configuration.Configuration
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

class ESSinkFunction extends RichSinkFunction[Record] {

  var client: TransportClient = null

  override def invoke(value: Record, context: SinkFunction.Context[_]): Unit = {
    val record: ESRecord = value.asInstanceOf[ESRecord]

    client.prepareIndex("z_spark", "news", "1").
      setSource(jsonBuilder.startObject.field("cluster.name", "zz").
        field("url", record.url).
        field("url_md5", record.urlMd5).
        field("domain", record.domain).
        field("host", record.host).
        field("date", record.eventTime.toString).
        field("content", record.content).endObject).get


  }


  override def open(parameters: Configuration): Unit = {
    //设置集群的名字
    val settings: Settings = Settings.builder().put("cluster.name", "zz_es").put("client.transport.sniff", true).build()
    new PreBuiltTransportClient(settings).addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("s1.hadoop"), 9300),
      new InetSocketTransportAddress(InetAddress.getByName("s2.hadoop"), 9300),
      new InetSocketTransportAddress(InetAddress.getByName("s3.hadoop"), 9300))


  }

  override def close(): Unit = {

    client.close()

  }
}
