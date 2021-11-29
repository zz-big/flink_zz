package com.zz.flink.source

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Description:  
 *
 * @author zz  
 * @date 2021/11/27 14:35 
 */
object ParallelSourceFunction {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    conf.setString("taskmanager.numberOfTaskSlots", "1")
    conf.setString("rest.port", "8887")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, conf)
    // 使用自定义的source

    val text = env.addSource(new ParallelSourceZ)

    val word: DataStream[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)
    word.print()

    //    println(env.getExecutionPlan)
    env.execute("ParallelSourceFunction")


  }

}

class ParallelSourceZ extends ParallelSourceFunction[String] {
  var num = 0
  var isCancel = true

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (isCancel) {
      sourceContext.collect(s"zz_${num}")
      Thread.sleep(1000)
      num += 1
    }

  }

  override def cancel(): Unit = {
    println("canceling")

    isCancel = false
  }
}