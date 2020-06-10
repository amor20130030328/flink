package com.gy.flink.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object SourceKafka {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties();

    props.setProperty("bootstrap.servers","hadoop102:9092")
    props.setProperty("group.id","consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset","latest")

    val stream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),props))

    stream.print("kafka")

    environment.execute("SourceApiTest")


  }

}
