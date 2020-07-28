package com.marvin.esToKafka

import java.util.Properties
import com.marvin.esToKafka.util.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
哈哈哈马志彬新增
/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: KafkaProducer.scala
* @time: 2020-03-11 17:21
*/

object KafkaProducer {


  def main(args: Array[String]): Unit = {

    Config.load("config.properties")

    val props = new Properties()
    props.setProperty("bootstrap.servers",Config.getString("kafka.server"))
    props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    producer.send(new ProducerRecord(Config.getString("kafka.topic"),"1","mazhibin"))

    producer.close()
  }


}
