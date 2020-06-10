package com.gy.flink

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetTest {

  def main(args: Array[String]): Unit = {


    val source = ExecutionEnvironment.getExecutionEnvironment.readTextFile("input/hello.txt")

    import org.apache.flink.api.scala.createTypeInformation
    source.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()

  }
}
