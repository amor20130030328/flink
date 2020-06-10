package com.gy.flink.api

import org.apache.flink.streaming.api.scala._

object TransformApi {

  def main(args: Array[String]): Unit = {

   //基本转换算子 + 简单聚合算子
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("hadoop102",7777)
    stream.map{
      word =>
        val line = word.split(" ")
        (line(0),line(1))
    }.keyBy(0).max(1).print()
    environment.execute("transform")




  }

}
