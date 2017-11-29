package com.zjlgdx.lab906.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import java.io.File
import com.typesafe.config.ConfigFactory

object RedisClient extends Serializable{

  //read config
  //var projectDir = new File("").getCanonicalPath
  //val config = ConfigFactory.parseFile(new File(projectDir+"/conf/conf/kafka_spark_streaming_redis_mysql.properties"))
  //val config = ConfigFactory.parseFile(new File(args(0)))
  //val redisHost = "192.168.1.245"
  //val redisport = 6399
  //val redisHost = config.getString("redis.host")
  //val redisport = config.getInt("redis.port")
  def getPool(redisHost:String,redisPort:Int,redisTimeout:Int):JedisPool ={
    lazy val pool = new JedisPool(new GenericObjectPoolConfig(),redisHost,redisPort,redisTimeout)
    //val pool = new JedisPool(config,redisHost,redisport,redisTimeout)
    lazy val hook = new Thread {
      override def run = {
        println("Execute hook thread:" + this)
        pool.destroy()
      }
    }
    sys.addShutdownHook(hook.run)
    pool
  }
}
