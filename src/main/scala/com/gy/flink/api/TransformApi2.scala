package com.gy.flink.api

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object TransformApi2 {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.readTextFile("input\\sensor.txt")
    environment.setParallelism(1)


    val keyStream: KeyedStream[SensorReading, Tuple] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).keyBy(0)

    val dataStream: DataStream[SensorReading] = keyStream.sum(2)
    dataStream.print()

    /*
       val value: KeyedStream[SensorReading, Tuple] = stream.map(data => {
         val dataArray = data.split(",")
         SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
       }).keyBy(0)

       */

     /*
      val value: KeyedStream[SensorReading, String] = stream.map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }).keyBy(_.id)
     */

  /*  stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).keyBy(_.id).sum("temperature").print()
  */


      environment.execute("transform")

  }

}
