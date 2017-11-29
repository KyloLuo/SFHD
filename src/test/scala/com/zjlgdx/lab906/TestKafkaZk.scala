package com.zjlgdx.lab906

import java.io.File
import com.typesafe.config.ConfigFactory
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

object TestKafkaZk {
  def main(args: Array[String]): Unit = {

    //读取配置
    val config = ConfigFactory.parseFile(new File("conf/conf/kafka_spark_streaming_redis_mysql.properties"))

    //Kafka配置
    val topic = config.getString("topic.name")
    val props = new Properties()
    //"bootstrap.servers"--新版kafka，"metadata.broker.list"--旧版
    props.put("bootstrap.servers", config.getString("kafka.metadata.broker.list"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[Nothing,String](props)


    val record = new ProducerRecord(topic,"sfhd")
    producer.send(record)
    producer.close()
  }
}
