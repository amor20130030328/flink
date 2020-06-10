package com.gy.utils

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {


  val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop101").setPort(6379).build()

  def getRedisSink(): RedisSink[(String,String)] ={
    new RedisSink[(String,String)](conf,new MyRedisMapper)
  }

  class MyRedisMapper extends RedisMapper[(String,String)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "channel_count")
      // new RedisCommandDescription(RedisCommand.SET  )
    }

    override def getValueFromData(t: (String, String)): String = t._2

    override def getKeyFromData(t: (String, String)): String = t._1
  }


}
