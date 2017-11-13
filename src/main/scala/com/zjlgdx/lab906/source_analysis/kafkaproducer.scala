package com.zjlgdx.lab906.source_analysis

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.util.Random
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringSerializer
object kafkaproducer {
  def main(args: Array[String]): Unit = {

    //read config
    //var projectDir = new File("").getCanonicalPath
    // config = ConfigFactory.parseFile(new File(projectDir+"/conf/kafka_spark_streaming_redis.properties"))

    val topic = "test3"
    val brokers = "192.168.1.245:9292"
    val props = new Properties()
    props.put("bootstrap.servers",brokers)
    props.put("client.id","KafkaProducer")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
    val data = new ProducerRecord[String,String](topic,"2017-10-26 21:55:50","{'HFHDII':92}")
    producer.send(data)
    producer.close()
  }
}
