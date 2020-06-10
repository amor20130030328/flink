package com.gy.flink

import com.alibaba.fastjson.JSON
import com.gy.bean.StartUpLog
import com.gy.utils.MyKafkaUtil
import org.apache.flink.streaming.api.scala._

object StreamKafkaSink {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer  =MyKafkaUtil.getKafkaSource("GMALL_STARTUP")

    val dstream: DataStream[String] = environment.addSource(kafkaConsumer)

    val startUplogDstream: DataStream[StartUpLog] = dstream.map((startUplog:String) =>JSON.parseObject(startUplog,classOf[StartUpLog]))


    val splitStream: SplitStream[StartUpLog] = startUplogDstream.split { startuplog =>
      var flag: List[String] = null
      if (startuplog.ch == "appstore") {
        flag = List("apple", "usa")
      } else if (startuplog.ch == "huawei") {
        flag = List("android", "china")
      } else {
        flag = List("android", "other")
      }
      flag
    }

    val appleStream: DataStream[StartUpLog] = splitStream.select("apple","china")
    appleStream.map(_.ch).addSink(MyKafkaUtil.getKafkaSink("sensor"))


    environment.execute()


  }

}
