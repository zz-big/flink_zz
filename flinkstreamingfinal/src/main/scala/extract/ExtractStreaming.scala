package extract

import java.util.Properties
import java.util.concurrent.TimeUnit

import config.MyConfig
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.datastream.{BroadcastStream, DataStreamUtils}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import process.{ExtractProcessFunction, MysqlProcessAllWindowFunction}
import sink.{ESSink, HbaseSinkFunction, RedisSinkFunction}
import source.{HtmlEventDeserializationSchema, KafkaCustomWatermarkExtractor, XpathConfigSource}
import util.{DBUtil, Util}

import scala.collection.convert.wrapAll._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class ExtractStreaming

object ExtractStreaming {
  def main(args: Array[String]): Unit = {
    //生成配置对象
    val conf = new Configuration()
    //开启flink-webui
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    //配置webui的日志文件，否则打印日志到控制台
    conf.setString("web.log.path", "E:\\idea_workspaces\\logs")
    //配置taskManager的日志文件，否则打印日志到控制台
    conf.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "E:\\idea_workspaces\\logs")
    //配置tm有多少个slot
    conf.setString("taskmanager.numberOfTaskSlots", "48")

    //获取local运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    //正式
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //使用Process time因为下面的窗口是固定时间执行的
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //设置为1方便调试，实际线上这行代码应该去掉
        env.setParallelism(1)
    //隔多长时间执行一次ck
    env.enableCheckpointing(MyConfig.CHECKPOINT_INTERVAL)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    //保存EXACTLY_ONCE
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //每次ck之间的间隔，不会重叠
    checkpointConfig.setMinPauseBetweenCheckpoints(MyConfig.CHECKPOINT_BETWEEN)
    //每次ck的超时时间
    checkpointConfig.setCheckpointTimeout(MyConfig.CHECKPOINT_TIMEOUT)
    //如果ck执行失败，程序是否停止
    checkpointConfig.setFailOnCheckpointingErrors(true)
    //job在执行CANCE的时候是否删除ck数据
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //指定保存ck的存储模式，因为kafka的offset比较小，
    //所以kafkaSource推荐使用MemoryStateBackend来保存offset，
    //这样速度快也不会占用过多内存
    val stateBackend = new MemoryStateBackend(MyConfig.MEMORY_STATE_BACKEND_SIZE,false)

    env.setStateBackend(stateBackend)
    //恢复策略
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        MyConfig.RESTART_NUM, // number of restart attempts
        org.apache.flink.api.common.time.Time.of(MyConfig.RESTART_DELAY_TIME, TimeUnit.SECONDS) // delay
      )
    )

    /* Kafka consumer */
    val kafkaConsumerProps = new Properties()
    kafkaConsumerProps.setProperty("bootstrap.servers",MyConfig.KAFKA_BROKER)
    kafkaConsumerProps.setProperty("group.id", MyConfig.KAFKA_GROUP)
    val kafkaSource = new FlinkKafkaConsumer010[HtmlEvent](MyConfig.KAFKA_TOPIC,new HtmlEventDeserializationSchema,kafkaConsumerProps)

    //正式
    //    kafkaSource.setStartFromGroupOffsets()
    //测试
    //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    kafkaSource.setStartFromEarliest()

    //指定kafkaSource为flink的流的source
    val htmlInput: DataStream[HtmlEvent] = env.addSource(kafkaSource)
      //指定数据水位线标识
      .assignTimestampsAndWatermarks(new KafkaCustomWatermarkExtractor(Time.hours(24)))

    //读取配置文件并生成广播变量
    val broadCast = new MapStateDescriptor[String, mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]](
      "configBroadcastState",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]] {})
    )
    val xpathInput = env.addSource(new XpathConfigSource(MyConfig.XPATH_INFO_DIR))
    val xpathBroadCastInput = xpathInput.broadcast(broadCast)

    //连接Kafka的数据流和广播流
    val connectInput: BroadcastConnectedStream[HtmlEvent, mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]] = htmlInput.connect(xpathBroadCastInput)

    //抽取正文
    val unit: DataStream[Record] = connectInput.process(new ExtractProcessFunction)



    unit.filter(f => {
      f match {
        case a: RedisRecord => true
        case _ => false
      }
    }).addSink(new RedisSinkFunction())

    unit.filter(f => {
      f match {
        case a: HBaseRecord => true
        case _ => false
      }
    }).addSink(new HbaseSinkFunction())

    unit.filter(f => {
      f match {
        case a: ESRecord => true
        case _ => false
      }
    }).addSink(ESSink())

    //mysql的存储
    val countResult: DataStream[MysqlRecord] = unit.filter(f => {
      f match {
        case a: MysqlRecord => true
        case _ => false
      }
    }).timeWindowAll(Time.seconds(MyConfig.MYSQL_SAVA_INTERVAL))
      .process(new MysqlProcessAllWindowFunction)

    //获取统计结果
    //mysql并发插入可能会有事务和连接数过多的问题，并且在企业的实际工作中可还能存在集群节点没有访问mysql权限的问题
    //由于本次业务只是把少量的统计数据插入到mysql中，所以选择用client单机端插入最为合适
    for (c<- DataStreamUtils.collect(countResult.javaStream)){

      val record: MysqlRecord = c.asInstanceOf[MysqlRecord]
      // 打印本批次的统计信息, 可以在client上看到
      println(s"last modify time : ${Util.getCurrentTime}")
      println(
        s"""host:${record.host}
           |scan:${record.scan}
           |filtered:${record.filtered}
           |extract:${record.extract}
           |empty:${record.emptyContext}
           |noMatchXpath:${record.noMatchXpath}""".stripMargin)
      DBUtil.insertIntoMysqlByJdbc(record)
    }



    env.execute("NewsExtract")
  }

}

//kafka原始数据类型
case class HtmlEvent(message: String, eventTime: Long)

class Record

case class RedisRecord(var host: String, var faileRule: ArrayBuffer[String], var trueRule: String) extends Record

case class HBaseRecord(var host: String, var urlMd5: String, var url: String, var domain: String, var content: String, var html: String,eventTime: Long) extends Record

case class ESRecord(var url: String, var host: String, var content: String, var domain: String, var urlMd5: String, var eventTime: Long) extends Record

case class MysqlRecord(host: String, filtered: Long = 0L, extract: Long = 0L, emptyContext: Long = 0L, noMatchXpath: Long = 0L, scan: Long = 1L) extends Record