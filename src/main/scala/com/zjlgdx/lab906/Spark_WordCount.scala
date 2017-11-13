package com.zjlgdx.lab906
import org.apache.spark.{SparkConf,SparkContext}

object Spark_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4,5))
    rdd.foreachPartition(line=>line.foreach(num=>println(num)))
  }
}
