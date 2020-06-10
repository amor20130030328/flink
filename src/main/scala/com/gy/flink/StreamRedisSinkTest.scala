package com.gy.flink

import com.alibaba.fastjson.JSON
import com.gy.bean.StartUpLog
import com.gy.utils.{MyKafkaUtil, MyRedisUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object StreamRedisSinkTest {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer  =MyKafkaUtil.getKafkaSource("GMALL_STARTUP")

    val dstream: DataStream[String] = environment.addSource(kafkaConsumer)

    val startUplogDstream: DataStream[StartUpLog] = dstream.map((startUplog:String) =>JSON.parseObject(startUplog,classOf[StartUpLog]))


    val chwordCount: DataStream[(String, Int)] = startUplogDstream.map(_.ch).map((_,1)).keyBy(0).sum(1)

    chwordCount.map(x => (x._1,x._2+"")).addSink(MyRedisUtil.getRedisSink())

    environment.execute()
  }
}
