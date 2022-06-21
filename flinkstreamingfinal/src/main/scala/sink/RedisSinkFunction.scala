package sink

import db.JedisConnectionPool
import extract.{Record, RedisRecord}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{Jedis, Transaction}

class RedisSinkFunction extends RichSinkFunction[Record] {
  var reids: Jedis = _

  override def open(parameters: Configuration): Unit = {
    reids = JedisConnectionPool.getConnection()
  }
  override def invoke(value: Record, context: SinkFunction.Context[_]): Unit = {

    val redisRecord: RedisRecord = value.asInstanceOf[RedisRecord]

    //把正反规则存到redis中,使用redis事务
    if (!redisRecord.trueRule.equals("") || !redisRecord.faileRule.isEmpty) {
      val transaction: Transaction = reids.multi()
      //切换数据库
      transaction.select(6)

      if (!redisRecord.trueRule.equals("")|| !redisRecord.faileRule.isEmpty) {
        transaction.incr(s"total_z:${redisRecord.host}")
      }

      if (!redisRecord.trueRule.equals("")) {
        transaction.zincrby(s"txpath_z:${redisRecord.host}", 1, redisRecord.trueRule)
      }
      if (!redisRecord.faileRule.isEmpty) {
        transaction.sadd(s"fxpath_z:${redisRecord.host}", redisRecord.faileRule: _*)
      }
      transaction.exec()
    }


  }

  override def close(): Unit = {
    reids.close()
  }


}
