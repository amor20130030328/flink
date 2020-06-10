package com.gy.flink.api

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object UDSource {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[SensorReading] = env.addSource(new MySensorSource())

    stream.print()
    env.execute("UDSource")

  }

}


class MySensorSource extends SourceFunction[SensorReading]{

  //flag : 表示数据源是否还在正常运行
  var running : Boolean = true


  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val random = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + random.nextGaussian() * 20)
    )

    while(running){
      //更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )

      //虎丘当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      )

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}