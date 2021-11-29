package com.zz.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Description:  
 *
 * @author zz  
 * @date 2021/11/27 12:37 
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port","8888")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val text: DataStream[String] = env.socketTextStream("localhost", 6666)
    import org.apache.flink.api.scala._
    val wordCount: DataStream[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)

    wordCount.print()
    env.execute("socketWordCount")
  }

}
