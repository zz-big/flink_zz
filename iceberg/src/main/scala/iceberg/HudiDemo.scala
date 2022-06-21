package iceberg

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Description:  
 *
 * @author zz  
 * @date 2022/6/9
 */
object HudiDemo {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setInteger(RestOptions.PORT, 9001)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE)

    env.setParallelism(1)
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    tenv.executeSql("CREATE TABLE IF NOT EXISTS test3_src ("
      + "  `id` INT NOT NULL,"
      + "  `age` int,"
      + "  `name`  varchar(100),"
      + "  PRIMARY KEY(`id`)"
      + " NOT ENFORCED"
      + ") with ("
      + "  'table-name' = 'test2',"
      + "  'connector' = 'mysql-cdc',"
      + "  'hostname' = 'gz-cdb-k9rd3k11.sql.tencentcdb.com',"
      + "  'server-time-zone' = 'Asia/Shanghai' ,"
      + "  'scan.startup.mode' = 'initial',"
      + "  'port' = '58184',"
      + "  'username' = 'zengqingyong',"
      + "  'password' = 'p*&B!%zDs!D#S7t&NF$7#jDn!dq3pR',"
      + "  'database-name' = 'shop-service-test'"
      + ")")


    tenv.executeSql("create table if not exists hudi_test ("
      + "  `id` INT NOT NULL,"
      + "  `age` int,"
      + "  `name`  varchar(100),"
      + "  PRIMARY KEY(`id`)  NOT ENFORCED" +
      ")with(\n " +
      "'table.type' = 'MERGE_ON_READ'," +
      "\n 'connector'='hudi'," +
      "\n 'path' = 'hdfs://ns1/hive/warehouse/hudi_db/test1'" +
      "\n)"
    )


    //        tenv.executeSql("select *  from hudi_catalog.iceberg_hadoop_db3.ice_test").print()
    tenv.executeSql("insert into hudi_test select * from test3_src")


  }

}
