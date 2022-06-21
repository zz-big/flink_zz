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
object IcebergDemo2 {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setInteger(RestOptions.PORT, 9002)

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


    //    tenv.executeSql("create table ice_test (" +
    //      "id int,\n" +
    //      "name string, \n" +
    //      "PRIMARY KEY (id) NOT ENFORCED \n" +
    //      ") with (" +
    //      "'connector'='iceberg',\n" +
    //      "'catalog-name'='iceberg_catalog3',\n" +
    //      "'catalog-type'='hadoop',\n" +
    //      "'warehouse'='hdfs://ns1/hive/warehouse/iceberg_hadoop_db3/test1',\n" +
    //      "'format-version'='1', \n" +
    //      "'write.metadata.delete-after-commit.enabled'='true'\n" +
    //      ")" )

    tenv.executeSql("create CATALOG iceberg_catalog3 with (" +
      "'type'='iceberg',\n" +
      "'catalog-type'='hive',\n" +
      "'uri'='thrift://192.168.80.52:9083',\n" +
      "'clients'='2',\n" +
      "'warehouse'='hdfs://ns1/hive/warehouse/iceberg_hadoop_db3/test3',\n" +
      "'format-version'='2' \n" +
      ")")
    //    tenv.executeSql("use  catalog iceberg_catalog3")
    // 创建库
    tenv.executeSql("CREATE DATABASE if not exists iceberg_catalog3.iceberg_hadoop_db3")
//    tenv.useDatabase("drop table iceberg_catalog3.iceberg_hadoop_db3.ice_test")
    tenv.executeSql("create table if not exists iceberg_catalog3.iceberg_hadoop_db3.ice_test ("
      + "  `id` INT NOT NULL,"
      + "  `age` int,"
      + "  `name`  varchar(100),"
      + "  PRIMARY KEY(`id`)  NOT ENFORCED" +
      ")with(\n " +
      "'write.metadata.delete-after-commit.enabled'='true'," +
      "\n 'write.metadata.previous-versions-max'='5'," +
      "\n 'format-version'='2'" +
      "\n)"
    )


        tenv.executeSql("select * from iceberg_catalog3.iceberg_hadoop_db3.ice_test").print()
//        tenv.executeSql("insert into iceberg_catalog3.iceberg_hadoop_db3.ice_test select * from test3_src")
    //    tenv.executeSql("select *  from iceberg_catalog3.iceberg_hadoop_db3.ice_test").print()

//    tenv.executeSql("update iceberg_catalog3.iceberg_hadoop_db3.ice_test set name='vvvv' where id =1")
    //    tenv.executeSql(" select *  from ice_test").print()


    //    tenv.executeSql("show databases").print()
    //    tenv.executeSql("create  table if not EXISTS iceberg_catalog3.iceberg_hadoop_db3.test (id int, name string)").print()
    //    tenv.executeSql(" select id,name  from  test3_src").print()
    //    tenv.executeSql("insert into  iceberg_catalog3.iceberg_hadoop_db3.test  select id,name  from  test3_src").print()

    //    tenv.executeSql("insert into  iceberg_catalog3.iceberg_hadoop_db3.test values (2, 'bb')")


  }

}
