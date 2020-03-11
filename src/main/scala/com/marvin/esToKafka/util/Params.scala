package com.marvin.esToKafka.util

import org.apache.kafka.common.serialization.StringDeserializer


/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: Params.scala
* @time: 2020-03-11 17:28
*/

object Params {

  def getKafkaParams(servers:String,groupid:String): Map[String, Object] ={
    Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"-> groupid,
      "enable.auto.commit" -> (false:java.lang.Boolean)//Kafka源不提交任何偏移量
    )
  }

}
