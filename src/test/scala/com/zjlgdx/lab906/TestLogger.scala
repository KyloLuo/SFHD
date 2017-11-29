package com.zjlgdx.lab906

import org.apache.log4j.Logger

object TestLogger {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("LoggerTest")
    if(5>3){
      logger.info("5 > 3")
    }
  }
}
