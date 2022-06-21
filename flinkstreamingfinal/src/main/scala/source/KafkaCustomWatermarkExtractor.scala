package source

import extract.HtmlEvent
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class KafkaCustomWatermarkExtractor(maxOutOfOrderness:Time) extends BoundedOutOfOrdernessTimestampExtractor[HtmlEvent](maxOutOfOrderness){
  override def extractTimestamp(t: HtmlEvent): Long = {
    t.eventTime
  }
}
