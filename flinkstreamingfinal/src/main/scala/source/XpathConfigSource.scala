package source

import java.io.BufferedReader

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import util.FileUtil

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.util.control.Breaks.{break, breakable}

class XpathConfigSource(var xpath:String) extends SourceFunction[mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]]{

  var lastUpdateTime = 0L
  // 存储xpath配置的数据结构
  val xpathInfoUpdate = new mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]()
  var isCancle = true

  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]]): Unit = {
    while (isCancle) {
      //先更新广播变量
      if (System.currentTimeMillis() - lastUpdateTime >= config.MyConfig.UPDATE_XPATH_INTERVAL || xpathInfoUpdate.isEmpty) {
        // 存储xpath配置的数据结构
        val xpathInfoUpdate: mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]] = new HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]()
        val fileSystem: FileSystem = FileSystem.get(new Configuration())
        val fileStatuses: Array[FileStatus] = fileSystem.listStatus(new Path(xpath))

        // 读取配置文件, 将xpath信息存入到xpathInfoUpdate
        for (f <- fileStatuses) {

          breakable {
            val path = f.getPath
            if (path.toString.endsWith("_COPYING_")) {
              break()
            }

            val reader: BufferedReader = FileUtil.getBufferedReader(path)
            //              val stream: FSDataInputStream = fileSystem.open(path)
            //              val reader = new BufferedReader(new InputStreamReader(stream))
            var line: String = reader.readLine()
            while (null != line) {
              //js.people.com.cn	//html/body/div[@class='clearfix w1000_320 text_con']/div[@class='fl text_con_left']/div[@class='box_con']/p	true	0
              val strings: Array[String] = line.split("\t")
              if (strings.length >= 3) {
                val host = strings(0)
                val xpath = strings(1)
                val xpathType = strings(2)

                val hostXpathInfo = xpathInfoUpdate.getOrElseUpdate(host, new HashMap[String, mutable.HashSet[String]]())
                //上面这行代码与下面两行代码作用相同
                //val hostXpathInfo = xpathInfoUpdate.getOrElse(host, new HashMap[String, HashSet[String]]())
                //xpathInfoUpdate += host -> hostXpathInfo

                val hostTpyeXpathInfo = hostXpathInfo.getOrElseUpdate(xpathType, new HashSet[String]())
                hostTpyeXpathInfo.add(xpath)

              }
              line = reader.readLine()
            }
            reader.close()
          }
        }

        sourceContext.collect(xpathInfoUpdate)
        // 更新xpath配置更新的时间广播变量
        lastUpdateTime = System.currentTimeMillis()
      }
    }


  }

  override def cancel(): Unit = {
    isCancle = false

  }
}
