package cdc

import com.starrocks.connector.flink.StarRocksSink
import com.starrocks.connector.flink.table.StarRocksSinkOptions
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Description:
 *
 * @author zz
 * @date 2022/4/20
 */
object MysqlCdcJob {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    setEnvParameters(env)
    //    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    val mysqlSource: MySqlSource[String] = MySqlSource.builder()
      .hostname("gz-cdb-k9rd3k11.sql.tencentcdb.com")
      .port(58184)
      .databaseList("shop-service-test")
      .tableList("test2")
      .username("zengqingyong")
      .password("p*&B!%zDs!D#S7t&NF$7#jDn!dq3pR")
      .deserializer(new JsonDebeziumDeserializationSchema())
      .build()


    val srSink: SinkFunction[String] = StarRocksSink.sink( // the sink options
      StarRocksSinkOptions.builder.withProperty("jdbc-url", "jdbc:mysql://81.71.12.50:8000")
        .withProperty("load-url", "192.168.0.17:8030;192.168.0.15:8030;192.168.0.13:8030")
        .withProperty("username", "root").withProperty("password", "pdroot21").
        withProperty("table-name", "test4").withProperty("database-name", "test")
        .withProperty("sink.properties.format", "json")
        .withProperty("sink.properties.strip_outer_array", "true")
        .withProperty("sink.parallelism", "1")
        .build)

    // Updates from clients
    //    val clients: Table = tableEnv.sqlQuery("SELECT * FROM test3_src")

    // Clients to change stream
    //    val clientsDataStream: DataStream[Row] = tableEnv.toChangelogStream(clients)

    // Send Clients to Elasticsearch for monitoring purposes
    //    val clientsStream: DataStream[Client] = clientsDataStream.map(row =>
    //      Client(
    //        row.getFieldAs[Integer]("id"),
    //        row.getFieldAs[String]("age"),
    //        row.getFieldAs[String]("name")
    //      ))

//    env.addSource(mysqlSource)
    env.execute()

  }

  private def setEnvParameters(env: StreamExecutionEnvironment): Unit = {
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage(4000000))
  }

  case class Client(id: Integer, age: String, name: String)

}
