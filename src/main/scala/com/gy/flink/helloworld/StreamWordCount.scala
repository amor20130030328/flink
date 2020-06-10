package com.gy.flink.helloworld

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("hadoop102",7777)
    env.disableOperatorChaining()

    dataStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    env.execute("流式计算单词数")



  }

}
