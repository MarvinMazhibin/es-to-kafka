package com.marvin.esToKafka.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable
/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: Init.scala
* @time: 2020-03-11 17:28
*/

trait Init {
  //通用配置
  def setupConf(): SparkConf = new SparkConf()
    //指定RDD的序列化的方式，worker之间的数据传输序列化方式
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .set("spark.streaming.stopGracefullyOnShutdown","true")
    .set("spark.streaming.kafka.maxRatePerPartition","500")
    .set("spark.task.maxFailures","1")
    .set("spark.speculationfalse","false")
    .set("es.index.auto.create","true")//es
    .set("es.nodes","192.168.1.200")

  def setupSsc(conf:SparkConf,second:Int) = new StreamingContext(conf,Seconds(second))

  def setupStream(ssc:StreamingContext,
                  topics:Array[String],
                  kafkaParams:Map[String, Object],
                  fromOffset:mutable.HashMap[TopicPartition,Long]):InputDStream[ConsumerRecord[String,String]] ={
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics,kafkaParams,fromOffset)
    )
  }

}
