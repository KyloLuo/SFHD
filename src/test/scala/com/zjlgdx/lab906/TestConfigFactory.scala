package com.zjlgdx.lab906

import java.io.File
import com.typesafe.config.ConfigFactory

object TestConfigFactory {
  def main(args: Array[String]): Unit = {
    // var projectDir = new File("").getCanonicalPath
    // val config = ConfigFactory.parseFile(new File(projectDir+"/conf/conf/kafka_spark_streaming_redis_mysql.properties"))
    val config = ConfigFactory.parseFile(new File("conf/kafka_spark_streaming_redis_mysql.properties"))
    println(config)
    println(config.getString("app.name"))

    //遍历config测试
    println(config.entrySet())
    val iter = config.entrySet().iterator()
    var redisconfig:Map[String,String] = Map()
    while (iter.hasNext){
      val line = iter.next()
      if (line.getKey.startsWith("redis.")){
        redisconfig += (line.getKey ->line.getValue.unwrapped().toString)
      }
    }
    println(redisconfig)
    println(redisconfig.getOrElse("redis.db",2))
  }
}
