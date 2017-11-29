package com.zjlgdx.lab906.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisConnPool1 {
  @transient @volatile private var pool: JedisPool = null
  //lazy var pool: JedisPool = null
  //val logger = Logger("RedisClient")

  def makePool(redisConfig: Map[String, String]): Unit = {
    val poolConfig = new JedisPoolConfig()
    // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
    // pool中最多jedis实例数，如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)
    poolConfig.setMaxTotal(redisConfig.getOrElse("redis.max_active", 2).toString.toInt)
    // pool中最多有多少个状态为idle(空闲的)的jedis实例
    poolConfig.setMaxIdle(redisConfig.getOrElse("redis.max_idle", 5).toString.toInt)
    // 最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException
    poolConfig.setMaxWaitMillis(redisConfig.getOrElse("redis.max_wait", 100000).toString.toInt)
    // 在引入一个jedis实例时，是否提前进行测试；如果为true，则保证jedis实例均可用
    poolConfig.setTestOnBorrow(redisConfig.getOrElse("redis.test_on_borrow", false).toString.toBoolean)

    val hosts = redisConfig.getOrElse("redis.ip", "localhost")
    val port = redisConfig.getOrElse("redis.port", 6379).toString.toInt
    val dbNum = redisConfig.getOrElse("redis.db", 0).toString.toInt
    val timeOut = redisConfig.getOrElse("redis.timeout", 2000).toString.toInt


    if (pool == null) {
      synchronized {
        if (pool == null) {
          pool = new JedisPool(poolConfig, hosts, port, timeOut, null, dbNum)

          val hook = new Thread {
            override def run = pool.destroy()
          }
          sys.addShutdownHook(hook.run)
        }
      }
    }
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }
}
