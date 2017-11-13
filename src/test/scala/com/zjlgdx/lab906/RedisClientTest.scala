package com.zjlgdx.lab906

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable{
  val redisHost = "127.0.0.1"
  val redisport = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(),redisHost,redisport,redisTimeout)
  //val pool = new JedisPool(config,redisHost,redisport,redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread:" + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}

object RedisClientTest {
  def main(args: Array[String]): Unit = {
    val jedis = RedisClient.pool.getResource
    jedis.select(1)
    val t = jedis.get("2017-10-25 22:07:49")
    print(t)
    jedis.close()
  }
}
