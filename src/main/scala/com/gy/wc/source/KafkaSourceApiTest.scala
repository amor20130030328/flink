package com.gy.wc.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object KafkaSourceApiTest {

  def main(args: Array[String]): Unit = {


    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //从自定义集合中读取数据

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id","flink")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream = environment.addSource(new FlinkKafkaConsumer011[String]("flink" ,new SimpleStringSchema(),properties))
    stream.print("kafka")

    environment.execute("SourceApiTest")


  }

}
