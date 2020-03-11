package com.marvin.esToKafka.entrance

import com.marvin.esToKafka.util.{Config, Init, Offset, Params}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: LoadEs.scala
* @time: 2020-03-11 17:26
*/

object LoadEs extends Init {

  def main(args: Array[String]): Unit = {

    Config.load("config.properties")
    val topics=Config.getString("kafka.topic").split(",")
    val groupId=Config.getString("kafka.groupid")
    val fromOffset=Offset.getBeginOffset(topics,groupId)
    println(fromOffset)

    val conf=setupConf().setAppName("toes").setMaster("local[2]")//打包需要注释本地模式
    val ssc=setupSsc(conf,1)
    val stream=setupStream(ssc,topics,Params.getKafkaParams(Config.getString("kafka.server"),groupId),fromOffset)

    import org.elasticsearch.spark._
    stream.foreachRDD{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println(rdd.count())
      if (!rdd.isEmpty()){
        rdd.map(_.value())
          .map(x=>Map("name"->x))
          .saveToEs("test/doc")
        Offset.saveEndOffset(offsetRanges(TaskContext.get.partitionId),groupId)//保存偏移量
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
