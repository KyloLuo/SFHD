package com.zjlgdx.lab906.util

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}

import com.typesafe.config.ConfigFactory

import scala.util.parsing.json.{JSON, JSONArray, JSONFormat, JSONObject}
object MysqlClient {

  //var projectDir = new File("").getCanonicalPath
  //val config = ConfigFactory.parseFile(new File(projectDir+"/conf/kafka_spark_streaming_redis.properties"))
  //val config = ConfigFactory.parseFile(new File(args(0)))
  val driver = "com.mysql.jdbc.Driver"

  def getConnection(url:String):Connection={
    Class.forName(driver)
    return DriverManager.getConnection(url)
  }

  def judgeNoTable(tablename:String,conn:Connection):Boolean={
    val rs:ResultSet = conn.getMetaData().getTables(null,null,tablename,null)
    if(rs.next()){
      return false
    }
    return true
}

  def insertJson(jsonStr:String,tablename:String,conn:Connection):Unit={
    var sqlStr:String = "insert ignore into "+tablename +"("
    var sqlStr_tail:String = "("
    val json:Option[Any] = JSON.parseFull(jsonStr)
    val kvmap:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    kvmap.foreach{x=>
      sqlStr = sqlStr+x._1+","
      sqlStr_tail = sqlStr_tail+x._2+","
    }
    val sql:String = sqlStr.substring(0,sqlStr.length-1)+") values "+ sqlStr_tail.substring(0,sqlStr_tail.length-1)+")"
    val statement = conn.createStatement()
    val Res:Int  = statement.executeUpdate(sql)
  }

  def createTable(jsonStr:String,tableName:String,conn:Connection):Unit ={
    val statement = conn.createStatement()
    var sqlStr = "create table if not exists "+tableName +"("
    val json:Option[Any] = JSON.parseFull(jsonStr)
    val kvmap:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    for (x <- kvmap.keys){
      if(x.equals("time")){
        sqlStr += x+" bigint primary key,"
      }else{
        sqlStr += x+" double(10,3),"
      }
    }
    sqlStr = sqlStr.substring(0,sqlStr.length-1)+") ENGINE=MYISAM;"
    var i = statement.executeUpdate(sqlStr)
    if(i!=1){
      println("success create table")
    }
  }
}