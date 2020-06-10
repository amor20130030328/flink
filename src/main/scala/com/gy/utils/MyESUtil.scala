package com.gy.utils

import java.util

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object MyESUtil {

val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("hadoop102",9200,"http"))
  httpHosts.add(new HttpHost("hadoop103",9200,"http"))
  httpHosts.add(new HttpHost("hadoop104",9200,"http"))

  def getElasticSearchSink(indexName :String):ElasticsearchSink[String] ={
   val esFunc = new ElasticsearchSinkFunction[String] {
     override def process(ele: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

       println("试图保存:" + ele)
       val jSONObject = JSON.parseObject(ele)
       val indexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jSONObject)
       requestIndexer.add(indexRequest)
       println("保存一条")
     }
   }
    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,esFunc)
    //刷新前缓冲的最大动作量
    sinkBuilder.build()
  }
}
