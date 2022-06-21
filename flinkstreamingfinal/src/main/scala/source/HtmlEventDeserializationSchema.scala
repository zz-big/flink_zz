package source

import extract.HtmlEvent
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

class HtmlEventDeserializationSchema extends KeyedDeserializationSchema[HtmlEvent]{
  override def deserialize(messagekey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): HtmlEvent = {
    //这里应该让数据源发送事件时间，目前先用System.currentTimeMillis()代替
    HtmlEvent(new String(message),System.currentTimeMillis())

  }
  //设置了无界流
  override def isEndOfStream(t: HtmlEvent): Boolean = false
  //得到自定义序列化类型
  override def getProducedType: TypeInformation[HtmlEvent] = TypeInformation.of(new TypeHint[HtmlEvent]() {})
}
