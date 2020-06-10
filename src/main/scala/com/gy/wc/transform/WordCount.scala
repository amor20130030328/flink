package com.gy.wc.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object WordCount {

  def main(args: Array[String]): Unit = {

    //从文件中读取数据
    val inpath = "input/hello.txt"

    val environment = ExecutionEnvironment.getExecutionEnvironment
    val inputDataSet = environment.readTextFile(inpath)

    val wordcountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordcountDataSet.print()


  }
}
