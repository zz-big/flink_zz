package com.zz.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * Description:  
 *
 * @author zz  
 * @date 2021/11/27 12:37 
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = env.readTextFile("E:\\00idea_workspaces\\flink_zz\\train\\src\\main\\resources\\data.txt")

    import org.apache.flink.api.scala._
    val wordCount: DataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1).setParallelism(1)

    wordCount.print()

//    env.execute("batch")
  }

}
