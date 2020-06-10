package com.gy.flink

import com.alibaba.fastjson.JSON
import com.gy.bean.StartUpLog
import com.gy.utils.{MyESUtil, MyKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.api.scala._

object StreamESSinkTest {


  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer  =MyKafkaUtil.getKafkaSource("GMALL_STARTUP")

    val dstream: DataStream[String] = environment.addSource(kafkaConsumer)

    val startUplogDstream: DataStream[StartUpLog] = dstream.map((startUplog:String) =>JSON.parseObject(startUplog,classOf[StartUpLog]))


   // 明细发送到es 中
    val esSink: ElasticsearchSink[String] = MyESUtil.getElasticSearchSink("essink")


    dstream.addSink(esSink)

    startUplogDstream.print()
    environment.execute()

  }
}
