package com.gy.flink.api

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object TransformApi4 {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.readTextFile("input\\sensor.txt")
    environment.setParallelism(1)

    //多流转换算子  SplitStream   side output 侧输出流
    val splitStream : SplitStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).split(t => {
      if (t.temperature > 35) Seq("high") else Seq("low")
    })

    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    val allStream = splitStream.select("high","low")

/*    allStream.print("all")
    highStream.print("high")
    lowStream.print("low")*/

    //合并

    val warning = highStream.map(data => (data.id,data.temperature))
    val connectSTream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(lowStream)


    val dataStream: DataStream[Product with Serializable] = connectSTream.map(
      warn => (warn._1, warn._2, "高温预警"),
      low => (low.id, "温度正常")
    )
    dataStream.print()

      environment.execute("transform")

  }

}
