package com.gy.flink.helloworld

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 批处理
  */
object WordCount {

  def main(args: Array[String]): Unit = {


    val environment = ExecutionEnvironment.getExecutionEnvironment
    val wordStream = environment.readTextFile("input/hello.txt")
    //flatMap 和 Map需要引用的隐式转换
    import org.apache.flink.api.scala._
    wordStream.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()


  }

}
