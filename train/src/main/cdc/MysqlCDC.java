/**
 * Description:
 *
 * @author zz
 * @date 2022/4/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.scala.*;
import org.checkerframework.checker.units.qual.C;

public class MysqlCDC {
    public static void main(String[] args) throws Exception {
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("gz-cdb-k9rd3k11.sql.tencentcdb.com")
//                .port(58184)
////                .serverId("213")
////                .includeSchemaChanges(true)
//                .databaseList("shop-service-test") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
//                .tableList("shop-service-test.test2") // set captured table
//                .username("zengqingyong")
//                .password("p*&B!%zDs!D#S7t&NF$7#jDn!dq3pR")
//                .deserializer(new MyDeserialization()) // converts SourceRecord to JSON String
//                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.setParallelism(3);
        // note: 增量同步需要开启CK
        env.enableCheckpointing(10000);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, envSettings);


//        DataStreamSink<String> stringDataStreamSink = env
//                //2> {"id":1,"age":222}
//                //1> {"id":2,"age":1}
//                //16> {"name":"sdfsdf","id":3,"age":34}
//                //15> {"id":4,"age":333}
//                //14> {"id":5,"age":5555555}
//                //13> {"name":"abcd","id":6,"age":22}
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
////                .setParallelism(4).print();
//                //14> {"id": 99, "age": 11,"name":"aaa"}
////        env.fromElements(new String[]{
////                        "{\"id\":98, \"age\":11,\"name\":\"\"}",
////                        "{\"id\":97, \"age\":112,\"name\":\"aaa\"}"
////                }).print();
//                .addSink(
//                        StarRocksSink.sink(
//                                // the sink options
//                                StarRocksSinkOptions.builder()
//                                        .withProperty("jdbc-url", "jdbc:mysql://81.71.12.50:8000")
//                                        .withProperty("load-url", "192-168-0-17:8040")
//                                        .withProperty("username", "root")
//                                        .withProperty("password", "pdroot21")
//                                        .withProperty("table-name", "test3")
//                                        .withProperty("database-name", "test")
//                                        .withProperty("sink.properties.format", "json")
//                                        .withProperty("sink.properties.strip_outer_array", "true")
//                                        .withProperty("sink.parallelism", "1")
//                                        .build()
//
//                        )
//                );



        TableResult tableSrouce = streamTableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS test3_src (" +
                "  `id` INT NOT NULL," +
                "  `age` int," +
                "  `name`  varchar(100)," +
                "  PRIMARY KEY(`id`)" +
                " NOT ENFORCED" +
                ") with (" +
                "  'table-name' = 'test2'," +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'gz-cdb-k9rd3k11.sql.tencentcdb.com'," +
                "  'server-time-zone' = 'Asia/Shanghai' ," +
                "  'scan.startup.mode' = 'initial',"+
                "  'port' = '58184'," +
                "  'username' = 'zengqingyong'," +
                "  'password' = 'p*&B!%zDs!D#S7t&NF$7#jDn!dq3pR'," +
                "  'database-name' = 'shop-service-test'" +
                ")");


        TableResult tableSink = streamTableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS test3_sink (" +
                "  `id` INT NOT NULL," +
                "  `age` int," +
                "  `name`  varchar(100)," +
                "  PRIMARY KEY(`id`)" +
                " NOT ENFORCED" +
                ")" +
                " with (" +
                "  'sink.buffer-flush.interval-ms' = '15000'," +
                "  'jdbc-url' = 'jdbc:mysql://81.71.12.50:8000'," +
                "  'load-url' = '192-168-0-17:8040'," +
                "  'sink.properties.column_separator' = '\\x01'," +
                "  'connector' = 'starrocks'," +
                "  'username' = 'root'," +
                "  'password' = 'pdroot21'," +
                "  'sink.properties.row_delimiter' = '\\x02'," +
                "  'database-name' = 'test'," +
                "  'table-name' = 'test3'" +
                ")");

        // enable checkpoint
//        env.enableCheckpointing(3000);
//       streamTableEnvironment.executeSql(" select * from test3_src").print();
//
        streamTableEnvironment.executeSql(" insert into test3_sink select * from test3_src");
//        rowDataStream.print();
//        env.execute("Print MySQL Snapshot + Binlog");
    }

    public static class RowData {
        public int id;
        public int age;
        public String name;

        public RowData(int score, int age, String name) {

        }
    }

    /**
     * {
     *     "before":{
     *         "id":2,
     *         "age":1,
     *         "name":null,
     *         "aa":null
     *     },
     *     "after":{
     *         "id":2,
     *         "age":1,
     *         "name":null,
     *         "aa":"sadf"
     *     },
     *     "source":{
     *         "version":"1.5.4.Final",
     *         "connector":"mysql",
     *         "name":"mysql_binlog_source",
     *         "ts_ms":1650370401000,
     *         "snapshot":"false",
     *         "db":"shop-service-test",
     *         "sequence":null,
     *         "table":"test2",
     *         "server_id":112827,
     *         "gtid":"54150d5b-0a0e-11ec-9412-b8599fe78558:889739",
     *         "file":"mysql-bin.000034",
     *         "pos":275308,
     *         "row":0,
     *         "thread":null,
     *         "query":null
     *     },
     *     "op":"u",
     *     "ts_ms":1650370401103,
     *     "transaction":null
     * }
     *
     *
     * {
     *     "source":{
     *         "version":"1.5.4.Final",
     *         "connector":"mysql",
     *         "name":"mysql_binlog_source",
     *         "ts_ms":1650370415606,
     *         "snapshot":"false",
     *         "db":"shop-service-test",
     *         "sequence":null,
     *         "table":"test2",
     *         "server_id":112827,
     *         "gtid":"54150d5b-0a0e-11ec-9412-b8599fe78558:889740",
     *         "file":"mysql-bin.000034",
     *         "pos":275478,
     *         "row":0,
     *         "thread":null,
     *         "query":null
     *     },
     *     "historyRecord":"{
     *         \"source\":{
     *             \"file\":\"mysql-bin.000034\",
     *             \"pos\":275478,
     *             \"server_id\":112827
     *         },
     *         \"position\":{
     *             \"transaction_id\":null,
     *             \"ts_sec\":1650370415,
     *             \"file\":\"mysql-bin.000034\",
     *             \"pos\":275687,
     *             \"gtids\":\"54150d5b-0a0e-11ec-9412-b8599fe78558:1-889739\",
     *             \"server_id\":112827
     *         },
     *         \"databaseName\":\"shop-service-test\",
     *         \"ddl\":\"ALTER TABLE `shop-service-test`.test2 DROP COLUMN aa\",
     *         \"tableChanges\":[
     *             {
     *                 \"type\":\"ALTER\",
     *                 \"id\":\"\\\"shop-service-test\\\".\\\"test2\\\"\",
     *                 \"table\":{
     *                     \"defaultCharsetName\":\"utf8mb4\",
     *                     \"primaryKeyColumnNames\":[
     *                         \"id\"
     *                     ],
     *                     \"columns\":[
     *                         {
     *                             \"name\":\"id\",
     *                             \"jdbcType\":4,
     *                             \"typeName\":\"INT\",
     *                             \"typeExpression\":\"INT\",
     *                             \"charsetName\":null,
     *                             \"position\":1,
     *                             \"optional\":false,
     *                             \"autoIncremented\":false,
     *                             \"generated\":false
     *                         },
     *                         {
     *                             \"name\":\"age\",
     *                             \"jdbcType\":4,
     *                             \"typeName\":\"INT\",
     *                             \"typeExpression\":\"INT\",
     *                             \"charsetName\":null,
     *                             \"position\":2,
     *                             \"optional\":true,
     *                             \"autoIncremented\":false,
     *                             \"generated\":false
     *                         },
     *                         {
     *                             \"name\":\"name\",
     *                             \"jdbcType\":12,
     *                             \"typeName\":\"VARCHAR\",
     *                             \"typeExpression\":\"VARCHAR\",
     *                             \"charsetName\":\"utf8mb4\",
     *                             \"length\":100,
     *                             \"position\":3,
     *                             \"optional\":true,
     *                             \"autoIncremented\":false,
     *                             \"generated\":false
     *                         }
     *                     ]
     *                 }
     *             }
     *         ]
     *     }"
     * }
     */
}


