package com.zjlgdx.lab906

import java.io.File
import com.typesafe.config.ConfigFactory

object TestConfigFactory {
  def main(args: Array[String]): Unit = {
    var projectDir = new File("").getCanonicalPath
    val config = ConfigFactory.parseFile(new File(projectDir+"/conf/kafka_spark_streaming_redis.properties"))
    //print(config.getString("kafka.metadata.broker.list"))
    print(config.getString("app.name"))

  }
}
