package com.zjlgdx.lab906.source_analysis

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.zjlgdx.lab906.util.RedisConnPool1
import java.util.Date
import java.text.SimpleDateFormat
import java.io.File
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import redis.clients.jedis.Jedis

object KafkaSparkstreamingRedis1 {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)

    //本地测试
    //val config = ConfigFactory.parseFile(new File("conf/conf/kafka_spark_streaming_redis_mysql.properties"))


    //读取配置文件(spark-submit jar)
    val config = ConfigFactory.parseFile(new File(args(0)))

    //Create a StreamingContext
    val conf = new SparkConf().setAppName(config.getString("app.name")).setMaster(config.getString("master"))
    val ssc = new StreamingContext(conf,Seconds(config.getInt("streaming.seconds")))

    //获取redis相关配置,包装成broadcast
    var redisConfig:Map[String,String] = Map()
    var iter = config.entrySet().iterator()
    while (iter.hasNext) {
      val itemConfig = iter.next()
      if(itemConfig.getKey.startsWith("redis.")){
        redisConfig += (itemConfig.getKey -> itemConfig.getValue.unwrapped().toString)
      }
    }
    val redisConfigBroadcast = ssc.sparkContext.broadcast(redisConfig)

    //Kafka相关配置

    val topic = Set(config.getString("topic.name"))
    val brokers = config.getString("kafka.metadata.broker.list")
    val kafkaParams = Map[String,String](
      "metadata.broker.list" ->brokers,"serializer.class"->"kafka.serializer.StringEncoder")

    //记录消费者zk的路径
    val topicDirs = new ZKGroupTopicDirs(config.getString("kafka.consumer.group.id"),config.getString("topic.name"))
    val zKClient = new ZkClient(config.getString("kafka.zk.list"),Integer.MAX_VALUE,Integer.MAX_VALUE,ZKStringSerializer)
    val children = zKClient.countChildren(topicDirs.consumerOffsetDir)

    //topicDirs.consumerOffsetDir:/consumers/test-consumer-group/offsets/test

    var messages:InputDStream[(String,String)] = null


    if(children>0){
      var fromOffsets:Map[TopicAndPartition,Long] = Map()
      for(i <- 0 until children){
        val partitionOffset = zKClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(config.getString("topic.name"),i)
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      val messageHandler = (mmd:MessageAndMetadata[String,String]) => (mmd.topic,mmd.message())
      messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
        ssc,kafkaParams,fromOffsets,messageHandler
      )
    }
    else {
      //使用最新的kafka topic中的offset
      messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topic)
    }

    //保存offset到zk
    var offsetRanges = Array[OffsetRange]()
    messages.transform {
      rdd => offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zKClient, zkPath, o.untilOffset.toString)

      }
    }

    //开始业务逻辑
    messages.foreachRDD(rdd=>{
      val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val now = dateFormat.format(new Date())
      printf("====================%s===================",now)
      println()
      rdd.foreachPartition(records=>{
        RedisConnPool1.makePool(redisConfigBroadcast.value)
        var jedis:Jedis = null
        try{
          jedis = RedisConnPool1.getPool.getResource
          val pipline = jedis.pipelined()

          for(item <- records){
            val onedata = JSON.parseObject(item._2)
            val time = onedata.getString("time")
            val key:Date = dateFormat.parse(time)
            onedata.put("time",key.getTime/1000)
            val value = onedata
            pipline.set("sfhd_origin_".concat((key.getTime/1000).toString),value.toString)
            pipline.expire("sfhd_origin_".concat((key.getTime/1000).toString),config.getInt("redis.timeexist"))
            println("write redis sceessfully!")
            //logger.info("write successfully!")
            //println(key.getTime/1000,value.toString) //"{"T1AMMSTTMP.AV":601.5,"AM23SIG0206.AV":390.4,"AM17CCS06A701.AV":912}"
          }
          pipline.sync()
        }finally {
          if(jedis!=null){
            jedis.close()
            //logger.info("fail to write redis!!!!!!")
          }
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
