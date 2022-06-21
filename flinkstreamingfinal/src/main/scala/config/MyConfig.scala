package config

object MyConfig {
  // xpath配置文件存放地址
  final val XPATH_INFO_DIR: String = "E:/python_workspaces/data/xpath"
//  final val XPATH_INFO_DIR: String = "hdfs://ns1/user/zengqingyong17/spark/xpath_cache_file"

  // 更新xpath配置文件所在的地址
  final val UPDATE_XPATH_INTERVAL: Long = 10000L

  // hbase 表名
  final val HBASE_TABLE_NAME: String = "z17:context_extract"

  // mysql 配置
  final val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://nn2.hadoop/zzcralwer", "username" -> "zz", "password" -> "12345678")
//  final val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://localhost/spark", "username" -> "root", "password" -> "root1")

  // redis 配置
  final val REDIS_CONFIG: Map[String, String] = Map("host" -> "nn1.hadoop", "port" -> "6379", "timeout" -> "10000")
//  final val REDIS_CONFIG: Map[String, String] = Map("host" -> "nn1.hadoop,nn2.hadoop,s1.hadoop,s2.hadoop,s3.hadoop,s4.hadoop", "port" -> "6379", "timeout" -> "10000")

  //KAFKA的组
  final val KAFKA_GROUP:String = "z11"

  //KAFKA的topic
  final val KAFKA_TOPIC:String = "zz_html"
//  final val KAFKA_TOPIC:String = "c17_test"

  //KAFKA的broker
//  final val KAFKA_BROKER:String = "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
  final val KAFKA_BROKER:String = "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"

  //ES的host
  final val ES_HOST:String = "s1.hadoop"

  //ES的端口
  final val ES_PORT:String = "9200"

  //ES的index和type
  final val ES_INDEX_TYPE:String = "z_spark/news"

  // spark的多目录输入和输出
  //  final val URL_FILE_DIR: String = "/Users/leohe/Data/input/zz_cralwer/*/*"  （"/Users/leohe/Data/input/zz_cralwer/201710/31"）
  //  多目录输出，例子：https://blog.csdn.net/bitcarmanlee/article/details/72934449


  //checkpoint周期
  final val CHECKPOINT_INTERVAL = 1000L

  //checkpoint间隔
  final val CHECKPOINT_BETWEEN = 2000L

  //checkpoint超时
  final val CHECKPOINT_TIMEOUT = 10L

  //容错次数
  final val RESTART_NUM = 3

  //容错延时时间
  final val RESTART_DELAY_TIME = 0

  //mysql的入库间隔
  final val MYSQL_SAVA_INTERVAL = 5

  //jm存储状态的内存大小
  final val MEMORY_STATE_BACKEND_SIZE = 10 * 1024 * 1024

}
