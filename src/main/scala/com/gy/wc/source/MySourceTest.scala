package com.gy.wc.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import scala.util.Random

object MySourceTest {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[SensorReading] = env.addSource(new SenserSource)

    stream.print().setParallelism(2)

    env.execute("mysource")
  }
}


class SenserSource() extends SourceFunction[SensorReading]{


  var running:Boolean = true

  //正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
   //初始化一个随机数发生器
    val rand = new Random()
    //初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i => ("sensor" + i, rand.nextGaussian() * 20)
    )

    while (running){
      //在前一次温度的基础上更新温度值
       curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )

      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      )

      Thread.sleep(500)
    }
  }

  //定义一个flag ,表示数据源是否正常运行
  override def cancel(): Unit = {
    running = false
  }
}