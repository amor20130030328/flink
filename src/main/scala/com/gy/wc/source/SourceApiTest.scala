package com.gy.wc.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class SensorReading(id:String,timestamp:Long,temperature:Double)


object SourceApiTest {

  def main(args: Array[String]): Unit = {


    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //从自定义集合中读取数据
    val stream = environment.fromCollection(List(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)))
     stream.print().setParallelism(6)

    //从文件中读取
    val stream2 = environment.readTextFile("input/textFile")

    stream2.print()

    environment.execute("SourceApiTest")


  }

}
