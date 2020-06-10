package com.gy.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object MyKafkaUtil {

  val prop = new Properties()

  prop.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
  prop.setProperty("group.id","flink")

  def getKafkaSource(topic :String) ={
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    kafkaConsumer
  }

  def getKafkaSink(topic :String) ={
    val kafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](prop.getProperty("bootstrap.servers"),topic,new SimpleStringSchema())
    kafkaProducer
  }

}
