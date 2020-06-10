package com.gy.wc.transform

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCountAuto {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    //创建流处理的执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //接收一个socket文本流
    val dataStream: DataStream[String] = environment.socketTextStream(host,port)

    //对每条数据进行处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print().setParallelism(2)

    environment.execute("wordcount ")

  }
}
