package com.marvin.esToKafka.util

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: Offset.scala
* @time: 2020-03-11 17:27
*/

object Offset {



  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val zkServer: String = Config.getString("zookeeper.quorum")
  private val zkClient:ZkClient = ZkUtils.createZkClient(zkServer,30000,30000)
  private val zkUtils:ZkUtils = ZkUtils.apply(zkClient,isZkSecurityEnabled=false)

  def getBeginOffset(topics:Seq[String], groupId:String): mutable.HashMap[TopicPartition, Long]  ={
    val fromOffsets = scala.collection.mutable.HashMap.empty[TopicPartition, Long]
    zkUtils.getPartitionsForTopics(topics).foreach{x=>
      val topic = x._1
      val partitions = x._2
      val topicDirs = new ZKGroupTopicDirs(groupId,topic)
      partitions.map{ partition=>
        val tp = new TopicPartition(topic,partition)
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        zkUtils.makeSurePersistentPathExists(zkPath)
        val kafkaOffset = getOffsetForKafka(tp)
        Try(zkUtils.readData(zkPath)._1.toLong) match {
          //判断zookerper偏移量是否过时
          case Success(zkOffset) => if (zkOffset < kafkaOffset)fromOffsets += tp->kafkaOffset else fromOffsets += tp->zkOffset
          //判断第一次启动
          case Failure(ex:NumberFormatException) => fromOffsets += tp-> kafkaOffset
        }
      }
    }
    fromOffsets
  }

  def getOffsetForKafka(topicPartition: TopicPartition, time: Long = OffsetRequest.EarliestTime): Long ={
    val brokerId = zkUtils.getLeaderForPartition(topicPartition.topic,topicPartition.partition).get
    val broker = zkUtils.getBrokerInfo(brokerId).get
    val endpoint = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)
    val consumer = new SimpleConsumer(endpoint.host,endpoint.port,10000,100000,"getMinOffset")
    val tp = TopicAndPartition(topicPartition.topic,topicPartition.partition)
    val request= OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(time, 1)))
    consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tp).offsets.head
  }

  def saveEndOffset(offsetRange:OffsetRange,groupId:String): Unit ={
    val topicDirs = new ZKGroupTopicDirs(groupId,offsetRange.topic)
    val zkPath = s"${topicDirs.consumerOffsetDir}/${offsetRange.partition}"
    zkUtils.updatePersistentPath(zkPath,offsetRange.untilOffset.toString)
    logger.info(s"保存偏移量 topic:${offsetRange.topic} partition:${offsetRange.partition} offset:${offsetRange.untilOffset}")
  }


}
