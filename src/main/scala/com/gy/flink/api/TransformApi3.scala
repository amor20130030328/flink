package com.gy.flink.api

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object TransformApi3 {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.readTextFile("input\\sensor2.txt")
    environment.setParallelism(1)


    val keyStream: KeyedStream[SensorReading, Tuple] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).keyBy(0)

    /**
       sensor_1, 1547718199, 35.80018327300259     1547718199     45.80018327300259
       sensor_1, 1547718206, 35.1                 1547718200     45.1
       sensor_1, 1547718207, 35.6                  1547718207              45.6
      */
    //输出当前传感器最新的温度 + 10 ,而时间是上一次数据的 时间 + 1
    keyStream.reduce((x,y)=>SensorReading(y.id,x.timestamp + 1 ,y.temperature + 10)).print()




      environment.execute("transform")

  }

}
